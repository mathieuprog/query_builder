defmodule QueryBuilder.Query.WhereExistsSubquery do
  @moduledoc false

  def where_exists_subquery(ecto_query, assoc_list, assoc_fields, scope, filters, or_filters) do
    QueryBuilder.Query.Exists.apply_exists(
      ecto_query,
      assoc_list.root_schema,
      assoc_fields,
      scope,
      filters,
      or_filters,
      false
    )
  end
end
