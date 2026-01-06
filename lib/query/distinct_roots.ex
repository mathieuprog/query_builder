defmodule QueryBuilder.Query.DistinctRoots do
  @moduledoc false

  alias QueryBuilder.AssocList.PreloadSpec

  def distinct_roots(ecto_query, assoc_list) do
    root_schema = QueryBuilder.Utils.root_schema(ecto_query)
    primary_key_fields = root_schema.__schema__(:primary_key)

    if primary_key_fields == [] do
      raise ArgumentError,
            "distinct_roots/1 requires the root schema to have a primary key so it can dedupe roots; " <>
              "got schema with no primary key: #{inspect(root_schema)}"
    end

    paths = to_many_through_join_preload_paths(assoc_list)

    if paths != [] do
      raise ArgumentError,
            "distinct_roots/1 cannot be combined with `preload_through_join` on to-many associations, " <>
              "because it collapses join rows and would drop association rows. " <>
              "Use `preload_separate/*` instead. Conflicting preload paths: #{inspect(paths)}"
    end

    QueryBuilder.Query.Distinct.distinct(ecto_query, assoc_list, primary_key_fields)
  end

  defp to_many_through_join_preload_paths(assoc_list) do
    do_to_many_through_join_preload_paths(assoc_list, [])
  end

  defp do_to_many_through_join_preload_paths([], _prefix), do: []

  defp do_to_many_through_join_preload_paths([assoc_data | rest], prefix) do
    current = prefix ++ [assoc_data.assoc_field]

    matches =
      case assoc_data.preload_spec do
        %PreloadSpec{strategy: :through_join} when assoc_data.cardinality == :many ->
          [current]

        _ ->
          []
      end

    matches ++
      do_to_many_through_join_preload_paths(assoc_data.nested_assocs, current) ++
      do_to_many_through_join_preload_paths(rest, prefix)
  end
end
