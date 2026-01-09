defmodule QueryBuilder.Query.Planner do
  @moduledoc false

  alias QueryBuilder.AssocList

  @barrier_ops [:top_n_per]
  @preload_op :preload

  @join_only_ops [:inner_join, :left_join]

  alias QueryBuilder.Query.Distinct
  alias QueryBuilder.Query.DistinctRoots
  alias QueryBuilder.Query.GroupBy
  alias QueryBuilder.Query.Having
  alias QueryBuilder.Query.LeftJoinLatest
  alias QueryBuilder.Query.LeftJoinTopN
  alias QueryBuilder.Query.Limit
  alias QueryBuilder.Query.Offset
  alias QueryBuilder.Query.OrderBy
  alias QueryBuilder.Query.Select
  alias QueryBuilder.Query.SelectMerge
  alias QueryBuilder.Query.TopNPer
  alias QueryBuilder.Query.Where
  alias QueryBuilder.Query.WhereExistsSubquery
  alias QueryBuilder.Query.WhereNotExistsSubquery

  def compile(%{ecto_query: ecto_query, operations: operations}) when is_list(operations) do
    QueryBuilder.Utils.with_token_cache(fn ->
      root_schema = QueryBuilder.Utils.root_schema(ecto_query)
      operations = Enum.reverse(operations)

      validate_select_operations!(operations)

      {preload_ops, stage_ops} =
        Enum.split_with(operations, &match?({@preload_op, _assocs, _args}, &1))

      {ecto_query, output_assoc_list} = compile_stages(ecto_query, root_schema, stage_ops)

      output_assoc_list = apply_preload_ops(root_schema, output_assoc_list, preload_ops)
      validate_distinct_roots_preload_conflicts!(operations, output_assoc_list)

      ecto_query =
        if preload_ops == [] do
          ecto_query
        else
          QueryBuilder.Query.Preload.preload(ecto_query, output_assoc_list)
        end

      {ecto_query, output_assoc_list}
    end)
  end

  def compile(other) do
    raise ArgumentError,
          "QueryBuilder.Query.Planner.compile/1 expects a %QueryBuilder.Query{}, got: #{inspect(other)}"
  end

  defp validate_distinct_roots_preload_conflicts!(operations, assoc_list) do
    distinct_roots? = Enum.any?(operations, &match?({:distinct_roots, _assocs, _args}, &1))

    if distinct_roots? do
      paths = to_many_through_join_preload_paths(assoc_list)

      if paths != [] do
        raise ArgumentError,
              "distinct_roots/1 cannot be combined with `preload_through_join` on to-many associations, " <>
                "because it collapses join rows and would drop association rows. " <>
                "Use `preload_separate/*` instead. Conflicting preload paths: #{inspect(paths)}"
      end
    end

    :ok
  end

  defp to_many_through_join_preload_paths(%AssocList{} = assoc_list) do
    do_to_many_through_join_preload_paths(assoc_list.roots, [])
  end

  defp do_to_many_through_join_preload_paths(nodes_map, prefix) when is_map(nodes_map) do
    Enum.flat_map(nodes_map, fn {_assoc_field, assoc_data} ->
      current = prefix ++ [assoc_data.assoc_field]

      matches =
        case assoc_data.preload_spec do
          %AssocList.PreloadSpec{strategy: :through_join} when assoc_data.cardinality == :many ->
            [current]

          _ ->
            []
        end

      matches ++ do_to_many_through_join_preload_paths(assoc_data.nested_assocs, current)
    end)
  end

  defp apply_preload_ops(_root_schema, %AssocList{} = assoc_list, []), do: assoc_list

  defp apply_preload_ops(root_schema, %AssocList{} = assoc_list, preload_ops)
       when is_list(preload_ops) do
    Enum.reduce(preload_ops, assoc_list, fn
      {@preload_op, assocs, [%AssocList.PreloadSpec{} = preload_spec]}, assoc_list ->
        AssocList.build(root_schema, assoc_list, assocs,
          join: :none,
          preload_spec: preload_spec
        )

      {@preload_op, _assocs, [other]}, _assoc_list ->
        raise ArgumentError, "invalid preload spec: #{inspect(other)}"

      {@preload_op, _assocs, args}, _assoc_list ->
        raise ArgumentError, "internal error: invalid preload operation args: #{inspect(args)}"

      other, _assoc_list ->
        raise ArgumentError, "internal error: invalid preload operation: #{inspect(other)}"
    end)
  end

  defp compile_stages(ecto_query, root_schema, operations) do
    stages = split_into_stages(operations)

    Enum.reduce(stages, {ecto_query, AssocList.new(root_schema)}, fn stage_ops,
                                                                     {ecto_query,
                                                                      _output_assoc_list} ->
      stage_ends_with_barrier? =
        case List.last(stage_ops) do
          {type, _assocs, _args} when type in @barrier_ops -> true
          _ -> false
        end

      {ecto_query, stage_assoc_list} = compile_stage(ecto_query, root_schema, stage_ops)

      output_assoc_list =
        if stage_ends_with_barrier? do
          AssocList.new(root_schema)
        else
          stage_assoc_list
        end

      {ecto_query, output_assoc_list}
    end)
  end

  defp split_into_stages([]), do: []

  defp split_into_stages(operations) do
    {stages, current_stage_rev} =
      Enum.reduce(operations, {[], []}, fn {type, _assocs, _args} = op,
                                           {stages, current_stage_rev} ->
        current_stage_rev = [op | current_stage_rev]

        if type in @barrier_ops do
          {[Enum.reverse(current_stage_rev) | stages], []}
        else
          {stages, current_stage_rev}
        end
      end)

    stages =
      case current_stage_rev do
        [] -> stages
        current_stage_rev -> [Enum.reverse(current_stage_rev) | stages]
      end

    Enum.reverse(stages)
  end

  defp compile_stage(ecto_query, root_schema, stage_ops) do
    assoc_list =
      Enum.reduce(stage_ops, AssocList.new(root_schema), fn {_type, assocs, _args} = op,
                                                            assoc_list ->
        case List.wrap(assocs) do
          [] ->
            assoc_list

          assoc_fields ->
            AssocList.build(root_schema, assoc_list, assoc_fields, assoc_build_opts(op))
        end
      end)

    ecto_query = QueryBuilder.JoinMaker.make_joins(ecto_query, assoc_list)

    ecto_query =
      Enum.reduce(stage_ops, ecto_query, fn {type, _assocs, _args} = op, ecto_query ->
        if type in @join_only_ops do
          ecto_query
        else
          apply_operation(ecto_query, op, assoc_list)
        end
      end)

    {ecto_query, assoc_list}
  end

  defp assoc_build_opts({:inner_join, _assocs, _args}), do: [join: :inner]

  defp assoc_build_opts({:left_join, _assocs, [left_join_mode, join_filters]})
       when left_join_mode in [:leaf, :path] and is_list(join_filters),
       do: [join: :left, join_filters: join_filters, left_join_mode: left_join_mode]

  defp assoc_build_opts({:left_join, _assocs, args}) do
    raise ArgumentError, "internal error: invalid left_join operation args: #{inspect(args)}"
  end

  defp assoc_build_opts(_operation), do: []

  defp apply_operation(ecto_query, {:where, _assocs, [filters, or_filters]}, assoc_list) do
    Where.where(ecto_query, assoc_list, filters, or_filters)
  end

  defp apply_operation(ecto_query, {:select, _assocs, [selection]}, assoc_list) do
    Select.select(ecto_query, assoc_list, selection)
  end

  defp apply_operation(ecto_query, {:select_merge, _assocs, [selection]}, assoc_list) do
    SelectMerge.select_merge(ecto_query, assoc_list, selection)
  end

  defp apply_operation(ecto_query, {:distinct, _assocs, [value]}, assoc_list) do
    Distinct.distinct(ecto_query, assoc_list, value)
  end

  defp apply_operation(ecto_query, {:distinct_roots, _assocs, []}, assoc_list) do
    DistinctRoots.distinct_roots(ecto_query, assoc_list)
  end

  defp apply_operation(ecto_query, {:group_by, _assocs, [expr]}, assoc_list) do
    GroupBy.group_by(ecto_query, assoc_list, expr)
  end

  defp apply_operation(ecto_query, {:having, _assocs, [filters, or_filters]}, assoc_list) do
    Having.having(ecto_query, assoc_list, filters, or_filters)
  end

  defp apply_operation(ecto_query, {:order_by, _assocs, [value]}, assoc_list) do
    OrderBy.order_by(ecto_query, assoc_list, value)
  end

  defp apply_operation(ecto_query, {:limit, _assocs, [value]}, assoc_list) do
    Limit.limit(ecto_query, assoc_list, value)
  end

  defp apply_operation(ecto_query, {:offset, _assocs, [value]}, assoc_list) do
    Offset.offset(ecto_query, assoc_list, value)
  end

  defp apply_operation(
         ecto_query,
         {:where_exists_subquery, _assocs, [assoc_fields, scope, filters, or_filters]},
         assoc_list
       ) do
    WhereExistsSubquery.where_exists_subquery(
      ecto_query,
      assoc_list,
      assoc_fields,
      scope,
      filters,
      or_filters
    )
  end

  defp apply_operation(
         ecto_query,
         {:where_not_exists_subquery, _assocs, [assoc_fields, scope, filters, or_filters]},
         assoc_list
       ) do
    WhereNotExistsSubquery.where_not_exists_subquery(
      ecto_query,
      assoc_list,
      assoc_fields,
      scope,
      filters,
      or_filters
    )
  end

  defp apply_operation(ecto_query, {:top_n_per, _assocs, [opts]}, assoc_list) do
    TopNPer.top_n_per(ecto_query, assoc_list, opts)
  end

  defp apply_operation(ecto_query, {:left_join_latest, _assocs, [assoc_field, opts]}, assoc_list) do
    LeftJoinLatest.left_join_latest(ecto_query, assoc_list, assoc_field, opts)
  end

  defp apply_operation(ecto_query, {:left_join_top_n, _assocs, [assoc_field, opts]}, assoc_list) do
    LeftJoinTopN.left_join_top_n(ecto_query, assoc_list, assoc_field, opts)
  end

  defp apply_operation(_ecto_query, {type, _assocs, args}, _assoc_list) do
    raise ArgumentError,
          "internal error: unknown query operation #{inspect(type)} with args #{inspect(args)}"
  end

  defp validate_select_operations!(operations) do
    select_indexes =
      operations
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {{type, _assocs, _args}, index}
        when type in [:select, :left_join_latest, :left_join_top_n] ->
          [index]

        {_op, _index} ->
          []
      end)

    case select_indexes do
      [] ->
        :ok

      [select_index] ->
        if Enum.any?(Enum.take(operations, select_index), fn
             {:select_merge, _assocs, _args} -> true
             _ -> false
           end) do
          raise ArgumentError,
                "only one select expression is allowed in query; " <>
                  "calling `select/*` (or `left_join_latest/3` / `left_join_top_n/3`) after `select_merge/*` is not supported (Ecto semantics)"
        end

        :ok

      _many ->
        raise ArgumentError,
              "only one select expression is allowed in query; " <>
                "call `select/*` (or `left_join_latest/3` / `left_join_top_n/3`) at most once and use `select_merge/*` to add fields"
    end
  end
end
