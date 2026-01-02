defmodule QueryBuilder.Query.WhereNotExistsSubquery do
  @moduledoc false

  def where_not_exists_subquery(ecto_query, _assoc_list, assoc_fields, scope, filters, or_filters) do
    QueryBuilder.Query.Exists.apply_exists(
      ecto_query,
      assoc_fields,
      scope,
      filters,
      or_filters,
      true
    )
  end
end
