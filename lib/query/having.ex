defmodule QueryBuilder.Query.Having do
  @moduledoc false

  require Ecto.Query

  def having(ecto_query, assoc_list, filters, or_filters) do
    filters = QueryBuilder.Aggregate.normalize_having_filters(List.wrap(filters))
    or_filters = QueryBuilder.Aggregate.normalize_having_or_filters(List.wrap(or_filters))

    dynamic_query =
      QueryBuilder.Query.Where.build_dynamic_query(ecto_query, assoc_list, filters, or_filters)

    Ecto.Query.having(ecto_query, ^dynamic_query)
  end
end
