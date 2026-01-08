defmodule QueryBuilder.Query.DistinctRoots do
  @moduledoc false

  def distinct_roots(ecto_query, assoc_list) do
    root_schema = assoc_list.root_schema
    primary_key_fields = root_schema.__schema__(:primary_key)

    if primary_key_fields == [] do
      raise ArgumentError,
            "distinct_roots/1 requires the root schema to have a primary key so it can dedupe roots; " <>
              "got schema with no primary key: #{inspect(root_schema)}"
    end

    QueryBuilder.Query.Distinct.distinct(
      ecto_query,
      assoc_list,
      primary_key_fields
    )
  end
end
