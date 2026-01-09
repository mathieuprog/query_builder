defmodule QueryBuilder.Query.LeftJoinTopN do
  @moduledoc false

  require Ecto.Query
  alias Ecto.Query

  alias QueryBuilder.AssocList
  alias QueryBuilder.Query.OrderBy
  alias QueryBuilder.Query.Where

  def left_join_top_n(%Query{} = ecto_query, %AssocList{} = assoc_list, assoc_field, opts) do
    %{
      n: n,
      order_by: order_by,
      where: where_filters,
      child_assoc_fields: child_assoc_fields
    } =
      validate_opts!(opts)

    root_schema = assoc_list.root_schema

    %Ecto.Association.Has{
      cardinality: :many,
      related: assoc_schema,
      owner_key: owner_key,
      related_key: related_key
    } =
      fetch_has_many_assoc!(root_schema, assoc_field)

    validate_order_by!(order_by, assoc_field)
    validate_order_by_includes_primary_key!(order_by, assoc_schema)

    top_n_binding = root_schema._top_n_binding(assoc_field)

    if Query.has_named_binding?(ecto_query, top_n_binding) do
      raise ArgumentError,
            "left_join_top_n/3 attempted to use named binding #{inspect(top_n_binding)} for " <>
              "#{inspect(root_schema)}.#{inspect(assoc_field)}, but that binding name is already present " <>
              "in the query. Fix: rename the conflicting binding (avoid `as: #{inspect(top_n_binding)}`) " <>
              "or do not call left_join_top_n for that association."
    end

    {child_query, child_assoc_list} = build_child_query!(assoc_schema, child_assoc_fields)

    child_query =
      child_query
      |> maybe_where(child_assoc_list, where_filters)
      |> OrderBy.order_by(child_assoc_list, order_by)
      |> Ecto.Query.limit(^n)

    correlation_dynamic =
      Ecto.Query.dynamic(
        [{^assoc_schema, c}],
        field(c, ^related_key) == field(parent_as(^root_schema), ^owner_key)
      )

    child_query = Query.where(child_query, ^correlation_dynamic)
    child_subquery = Query.subquery(child_query)

    ecto_query =
      Query.join(
        ecto_query,
        :left_lateral,
        [{^root_schema, _p}],
        c in ^child_subquery,
        as: ^top_n_binding,
        on: true
      )

    Query.select(ecto_query, [{^root_schema, p}, {^top_n_binding, c}], {p, c})
  end

  defp build_child_query!(assoc_schema, child_assoc_fields) do
    ecto_query = assoc_schema._query()

    assoc_list =
      case List.wrap(child_assoc_fields) do
        [] ->
          AssocList.new(assoc_schema)

        assoc_fields ->
          AssocList.build(assoc_schema, AssocList.new(assoc_schema), assoc_fields)
      end

    ecto_query = QueryBuilder.JoinMaker.make_joins(ecto_query, assoc_list)

    {ecto_query, assoc_list}
  end

  defp maybe_where(query, _assoc_list, []), do: query

  defp maybe_where(query, assoc_list, filters) do
    Where.where(query, assoc_list, filters, [])
  end

  defp validate_opts!(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "left_join_top_n/3 expects `opts` to be a keyword list, got: #{inspect(opts)}"
    end

    allowed_keys = [
      :n,
      :order_by,
      :where,
      :child_assoc_fields
    ]

    unknown_keys = opts |> Keyword.keys() |> Enum.uniq() |> Kernel.--(allowed_keys)

    if unknown_keys != [] do
      raise ArgumentError,
            "left_join_top_n/3 got unknown options: #{inspect(unknown_keys)}. " <>
              "Supported options: #{inspect(allowed_keys)}"
    end

    n = Keyword.fetch!(opts, :n)
    order_by = Keyword.fetch!(opts, :order_by)
    where = Keyword.get(opts, :where, [])
    child_assoc_fields = Keyword.get(opts, :child_assoc_fields, [])

    unless is_integer(n) and n >= 1 do
      raise ArgumentError,
            "left_join_top_n/3 expects `n` to be a positive integer, got: #{inspect(n)}"
    end

    if order_by in [nil, []] do
      raise ArgumentError, "left_join_top_n/3 requires a non-empty `order_by:` option"
    end

    if is_nil(where) do
      raise ArgumentError,
            "left_join_top_n/3 does not accept `where: nil`; omit the option or pass []"
    end

    unless is_list(where) do
      raise ArgumentError,
            "left_join_top_n/3 expects `where:` to be a keyword list (or a list of filters), got: #{inspect(where)}"
    end

    if is_nil(child_assoc_fields) do
      raise ArgumentError,
            "left_join_top_n/3 does not accept `child_assoc_fields: nil`; " <>
              "omit the option or pass []"
    end

    %{
      n: n,
      order_by: order_by,
      where: where,
      child_assoc_fields: child_assoc_fields
    }
  rescue
    KeyError ->
      raise ArgumentError,
            "left_join_top_n/3 requires `n:` and `order_by:` options; got: #{inspect(opts)}"
  end

  defp validate_opts!(nil) do
    raise ArgumentError,
          "left_join_top_n/3 expects `opts` to be a keyword list, got: nil"
  end

  defp validate_opts!(other) do
    raise ArgumentError,
          "left_join_top_n/3 expects `opts` to be a keyword list, got: #{inspect(other)}"
  end

  defp fetch_has_many_assoc!(root_schema, assoc_field) when is_atom(assoc_field) do
    assoc = root_schema.__schema__(:association, assoc_field)

    case assoc do
      %Ecto.Association.Has{cardinality: :many} ->
        assoc

      %Ecto.Association.Has{cardinality: :one} ->
        raise ArgumentError,
              "left_join_top_n/3 only supports has_many (to-many) associations; " <>
                "#{inspect(root_schema)}.#{inspect(assoc_field)} is a has_one association"

      %Ecto.Association.BelongsTo{} ->
        raise ArgumentError,
              "left_join_top_n/3 only supports has_many (to-many) associations; " <>
                "#{inspect(root_schema)}.#{inspect(assoc_field)} is a belongs_to association"

      %Ecto.Association.ManyToMany{} ->
        raise ArgumentError,
              "left_join_top_n/3 does not support many_to_many associations; " <>
                "#{inspect(root_schema)}.#{inspect(assoc_field)} is many_to_many"

      %Ecto.Association.HasThrough{} ->
        raise ArgumentError,
              "left_join_top_n/3 does not support has_many/has_one through associations; " <>
                "#{inspect(root_schema)}.#{inspect(assoc_field)} is a through association"

      nil ->
        raise ArgumentError,
              "unknown association #{inspect(assoc_field)} for #{inspect(root_schema)}; " <>
                "available associations: #{inspect(root_schema.__schema__(:associations))}"

      other ->
        raise ArgumentError,
              "left_join_top_n/3 does not support association #{inspect(root_schema)}.#{inspect(assoc_field)}; " <>
                "got association struct: #{inspect(other)}"
    end
  end

  defp fetch_has_many_assoc!(root_schema, assoc_field) do
    raise ArgumentError,
          "left_join_top_n/3 expects `assoc_field` to be an association atom, got: #{inspect(assoc_field)} " <>
            "for #{inspect(root_schema)}"
  end

  defp validate_order_by!(order_by, assoc_field) do
    if is_nil(order_by) do
      raise ArgumentError,
            "left_join_top_n/3 expects `order_by:` to be a keyword list for " <>
              "#{inspect(assoc_field)}, got nil"
    end

    unless Keyword.keyword?(order_by) do
      raise ArgumentError,
            "left_join_top_n/3 expects `order_by:` to be a keyword list for " <>
              "#{inspect(assoc_field)}, got: #{inspect(order_by)}"
    end
  end

  defp validate_order_by_includes_primary_key!(order_by, assoc_schema)
       when is_atom(assoc_schema) do
    primary_key_fields = assoc_schema.__schema__(:primary_key)

    if primary_key_fields == [] do
      raise ArgumentError,
            "left_join_top_n/3 requires the association schema to have a primary key so it can be used as " <>
              "a deterministic tie-breaker in `order_by:`; got schema with no primary key: #{inspect(assoc_schema)}"
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
            "left_join_top_n/3 requires `order_by:` to include the association primary key fields as a tie-breaker; " <>
              "missing: #{inspect(missing)} for association schema #{inspect(assoc_schema)}. " <>
              "Example: `order_by: [desc: :inserted_at, desc: :id]`."
    end
  end
end
