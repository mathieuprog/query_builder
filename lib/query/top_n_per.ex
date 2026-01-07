defmodule QueryBuilder.Query.TopNPer do
  @moduledoc false

  require Ecto.Query
  alias Ecto.Query
  import QueryBuilder.Utils

  @window_name :qb__top_n_per
  @rank_binding :qb__top_n_per_ranked
  @rn_field :qb__rn

  def top_n_per(%Ecto.Query{} = ecto_query, assoc_list, opts) do
    %{n: n, partition_by: partition_by, order_by: order_by, prefer_distinct_on?: prefer_distinct_on?} =
      validate_opts!(opts)

    root_schema = QueryBuilder.Utils.root_schema(ecto_query)
    primary_key_fields = root_schema.__schema__(:primary_key)

    if primary_key_fields == [] do
      raise ArgumentError,
            "top_n_per/2 requires the root schema to have a primary key so it can join ranked rows back to the root; " <>
              "got schema with no primary key: #{inspect(root_schema)}"
    end

    if ecto_query.limit != nil or ecto_query.offset != nil do
      raise ArgumentError,
            "top_n_per/2 must be applied before limit/offset; " <>
              "call top_n_per before QueryBuilder.limit/2 or QueryBuilder.offset/2"
    end

    if Ecto.Query.has_named_binding?(ecto_query, @rank_binding) do
      raise ArgumentError,
            "top_n_per/2 internal error: query already has named binding #{inspect(@rank_binding)}; " <>
              "this binding name is reserved by QueryBuilder"
    end

    validate_order_by_includes_primary_key!(order_by, primary_key_fields, root_schema)

    if has_to_many_joins?(assoc_list) and ecto_query.group_bys == [] and
         distinct_absent?(ecto_query.distinct) do
      raise ArgumentError,
            "top_n_per/2 cannot be applied to a query with to-many joins unless the query collapses join rows " <>
              "into unique root rows (e.g. via group_by on the root primary key or an explicit distinct). " <>
              "This query has a to-many join and no group_by/distinct; " <>
              "use where_exists_subquery/3 (when filtering), add group_by on #{inspect(primary_key_fields)}, " <>
              "or use distinct_roots/1 on Postgres."
    end

    partition_by_exprs = build_partition_by_exprs!(ecto_query, assoc_list, partition_by)
    order_by_exprs = build_order_by_exprs!(ecto_query, assoc_list, order_by)

    use_distinct_on? = prefer_distinct_on? and n == 1 and distinct_absent?(ecto_query.distinct)

    if not use_distinct_on? and Enum.any?(ecto_query.windows, fn {name, _} -> name == @window_name end) do
      raise ArgumentError,
            "top_n_per/2 cannot be applied to a query that already defines a window named #{inspect(@window_name)}; " <>
              "this window name is reserved by QueryBuilder"
    end

    if use_distinct_on? do
      distinct_query =
        ecto_query
        |> Query.exclude([:preload, :select, :order_by, :distinct])
        |> Ecto.Query.distinct(^partition_by_exprs)
        |> Ecto.Query.order_by(^order_by_exprs)
        |> Ecto.Query.select(^build_primary_key_select_map(root_schema, primary_key_fields))

      distinct_subquery = Ecto.Query.subquery(distinct_query)
      join_on = build_primary_key_join_dynamic!(root_schema, primary_key_fields, @rank_binding)

      Ecto.Query.join(
        ecto_query,
        :inner,
        [{^root_schema, x}],
        r in ^distinct_subquery,
        as: ^@rank_binding,
        on: ^join_on
      )
    else
      ranked_query_excludes =
        if distinct_absent?(ecto_query.distinct) do
          [:preload, :select, :order_by]
        else
          [:preload, :select]
        end

      ranked_query =
        ecto_query
        |> Query.exclude(ranked_query_excludes)
        |> Ecto.Query.windows([
          {@window_name,
           [
             partition_by: ^partition_by_exprs,
             order_by: ^order_by_exprs
           ]}
        ])
        |> Ecto.Query.select(^build_rank_select_map(root_schema, primary_key_fields, @rn_field))

      ranked_subquery = Ecto.Query.subquery(ranked_query)

      join_on = build_primary_key_join_dynamic!(root_schema, primary_key_fields, @rank_binding)

      ecto_query =
        Ecto.Query.join(
          ecto_query,
          :inner,
          [{^root_schema, x}],
          r in ^ranked_subquery,
          as: ^@rank_binding,
          on: ^join_on
        )

      Ecto.Query.where(ecto_query, [{^@rank_binding, r}], field(r, ^@rn_field) <= ^n)
    end
  end

  defp validate_opts!(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "top_n_per/2 expects `opts` to be a keyword list, got: #{inspect(opts)}"
    end

    n = Keyword.fetch!(opts, :n)
    partition_by = Keyword.fetch!(opts, :partition_by)
    order_by = Keyword.fetch!(opts, :order_by)
    prefer_distinct_on? = Keyword.get(opts, :prefer_distinct_on?, false)

    allowed_keys = [:n, :partition_by, :order_by, :prefer_distinct_on?]
    unknown_keys = opts |> Keyword.keys() |> Enum.uniq() |> Kernel.--(allowed_keys)

    if unknown_keys != [] do
      raise ArgumentError,
            "top_n_per/2 got unknown options: #{inspect(unknown_keys)}. " <>
              "Supported options: #{inspect(allowed_keys)}"
    end

    unless is_integer(n) and n >= 1 do
      raise ArgumentError, "top_n_per/2 expects `n` to be a positive integer, got: #{inspect(n)}"
    end

    unless is_boolean(prefer_distinct_on?) do
      raise ArgumentError,
            "top_n_per/2 expects `prefer_distinct_on?` to be a boolean, got: #{inspect(prefer_distinct_on?)}"
    end

    if partition_by in [nil, []] do
      raise ArgumentError,
            "top_n_per/2 requires a non-empty `partition_by` option"
    end

    if order_by in [nil, []] do
      raise ArgumentError,
            "top_n_per/2 requires a non-empty `order_by` option"
    end

    %{
      n: n,
      partition_by: partition_by,
      order_by: order_by,
      prefer_distinct_on?: prefer_distinct_on?
    }
  rescue
    KeyError ->
      raise ArgumentError,
            "top_n_per/2 requires :partition_by, :order_by, and :n options; got: #{inspect(opts)}"
  end

  defp validate_opts!(other) do
    raise ArgumentError,
          "top_n_per/2 expects `opts` to be a keyword list, got: #{inspect(other)}"
  end

  defp validate_order_by_includes_primary_key!(order_by, primary_key_fields, root_schema)
       when is_list(order_by) do
    unless Keyword.keyword?(order_by) do
      raise ArgumentError,
            "top_n_per/2 expects `order_by` to be a keyword list, got: #{inspect(order_by)}"
    end

    order_fields =
      order_by
      |> Enum.flat_map(fn
        {_direction, token} when is_atom(token) or is_binary(token) -> [to_string(token)]
        _ -> []
      end)
      |> MapSet.new()

    missing =
      primary_key_fields
      |> Enum.reject(fn pk_field ->
        MapSet.member?(order_fields, Atom.to_string(pk_field))
      end)

    if missing != [] do
      raise ArgumentError,
            "top_n_per/2 requires `order_by` to include the root primary key fields as a tie-breaker; " <>
              "missing: #{inspect(missing)} for root schema #{inspect(root_schema)}. " <>
              "Example: `order_by: [desc: :inserted_at, desc: :id]`."
    end
  end

  defp validate_order_by_includes_primary_key!(order_by, _primary_key_fields, _root_schema) do
    raise ArgumentError,
          "top_n_per/2 expects `order_by` to be a keyword list, got: #{inspect(order_by)}"
  end

  defp build_partition_by_exprs!(ecto_query, assoc_list, partition_by)
       when is_list(partition_by) do
    if Keyword.keyword?(partition_by) do
      raise ArgumentError,
            "top_n_per/2 expects `partition_by` to be a token or a list of tokens/expressions, got a keyword list: #{inspect(partition_by)}"
    end

    partition_by
    |> Enum.flat_map(&build_partition_by_exprs!(ecto_query, assoc_list, &1))
  end

  defp build_partition_by_exprs!(ecto_query, assoc_list, fun) when is_function(fun, 1) do
    fun.(&find_field_and_binding_from_token(ecto_query, assoc_list, &1))
    |> build_partition_by_exprs!(ecto_query, assoc_list)
  end

  defp build_partition_by_exprs!(_ecto_query, _assoc_list, %Ecto.Query.DynamicExpr{} = dynamic),
    do: [dynamic]

  defp build_partition_by_exprs!(_ecto_query, _assoc_list, %QueryBuilder.Aggregate{} = aggregate) do
    raise ArgumentError,
          "top_n_per/2 does not support aggregate expressions in `partition_by`: #{inspect(aggregate)}"
  end

  defp build_partition_by_exprs!(ecto_query, assoc_list, token)
       when is_atom(token) or is_binary(token) do
    {field, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, token)
    [Ecto.Query.dynamic([{^binding, x}], field(x, ^field))]
  end

  defp build_partition_by_exprs!(_ecto_query, _assoc_list, other) do
    raise ArgumentError,
          "top_n_per/2 expects `partition_by` to be a token, a list of tokens/expressions, a dynamic, or a 1-arity function; got: #{inspect(other)}"
  end

  defp build_order_by_exprs!(ecto_query, assoc_list, order_by) when is_list(order_by) do
    unless Keyword.keyword?(order_by) do
      raise ArgumentError,
            "top_n_per/2 expects `order_by` to be a keyword list, got: #{inspect(order_by)}"
    end

    Enum.map(order_by, fn
      {direction, %QueryBuilder.Aggregate{} = aggregate} when is_atom(direction) ->
        {direction, QueryBuilder.Aggregate.to_dynamic(ecto_query, assoc_list, aggregate)}

      {direction, %Ecto.Query.DynamicExpr{} = dynamic} when is_atom(direction) ->
        {direction, dynamic}

      {direction, fun} when is_atom(direction) and is_function(fun, 1) ->
        {direction, fun.(&find_field_and_binding_from_token(ecto_query, assoc_list, &1))}

      {direction, token} when is_atom(direction) and (is_atom(token) or is_binary(token)) ->
        {field, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, token)
        {direction, Ecto.Query.dynamic([{^binding, x}], field(x, ^field))}

      other ->
        raise ArgumentError,
              "top_n_per/2 received an invalid order_by expression: #{inspect(other)}. " <>
                "Expected `{direction, token}`, `{direction, aggregate}`, `{direction, dynamic}`, or `{direction, fun}`."
    end)
  end

  defp build_order_by_exprs!(_ecto_query, _assoc_list, other) do
    raise ArgumentError,
          "top_n_per/2 expects `order_by` to be a keyword list, got: #{inspect(other)}"
  end

  defp build_rank_select_map(root_schema, primary_key_fields, rn_field) do
    pk_map =
      Enum.reduce(primary_key_fields, %{}, fn pk_field, acc ->
        Map.put(acc, pk_field, Ecto.Query.dynamic([{^root_schema, x}], field(x, ^pk_field)))
      end)

    rn_dynamic = Ecto.Query.dynamic([], over(row_number(), :qb__top_n_per))
    Map.put(pk_map, rn_field, rn_dynamic)
  end

  defp build_primary_key_select_map(root_schema, primary_key_fields) do
    Enum.reduce(primary_key_fields, %{}, fn pk_field, acc ->
      Map.put(acc, pk_field, Ecto.Query.dynamic([{^root_schema, x}], field(x, ^pk_field)))
    end)
  end

  defp build_primary_key_join_dynamic!(root_schema, [pk_field], rank_binding) do
    Ecto.Query.dynamic(
      [{^root_schema, x}, {^rank_binding, r}],
      field(x, ^pk_field) == field(r, ^pk_field)
    )
  end

  defp build_primary_key_join_dynamic!(root_schema, pk_fields, rank_binding)
       when is_list(pk_fields) and length(pk_fields) > 1 do
    pk_fields
    |> Enum.map(fn pk_field ->
      Ecto.Query.dynamic(
        [{^root_schema, x}, {^rank_binding, r}],
        field(x, ^pk_field) == field(r, ^pk_field)
      )
    end)
    |> Enum.reduce(&Ecto.Query.dynamic(^&1 and ^&2))
  end

  defp has_to_many_joins?([]), do: false

  defp has_to_many_joins?([assoc_data | rest]) do
    joined_to_many? = assoc_data.join_spec.joined? and assoc_data.cardinality == :many
    joined_to_many? or has_to_many_joins?(assoc_data.nested_assocs) or has_to_many_joins?(rest)
  end

  defp distinct_absent?(nil), do: true
  defp distinct_absent?(%Query.ByExpr{expr: false}), do: true
  defp distinct_absent?(%Query.ByExpr{expr: []}), do: true
  defp distinct_absent?(_), do: false
end
