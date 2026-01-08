defmodule QueryBuilder.Query.Exists do
  @moduledoc false

  require Ecto.Query

  def apply_exists(
        ecto_query,
        source_schema,
        assoc_fields,
        scope_filters,
        filters,
        or_filters,
        negate?
      )
      when is_atom(source_schema) do
    subquery =
      source_schema._query()
      |> correlate_to_parent!(source_schema)
      |> apply_joins_and_filters(source_schema, assoc_fields, scope_filters, filters, or_filters)
      |> Ecto.Query.select([], 1)

    exists_dynamic =
      if negate? do
        Ecto.Query.dynamic([], not exists(subquery))
      else
        Ecto.Query.dynamic([], exists(subquery))
      end

    Ecto.Query.where(ecto_query, ^exists_dynamic)
  end

  defp apply_joins_and_filters(
         ecto_query,
         source_schema,
         assoc_fields,
         scope_filters,
         filters,
         or_filters
       ) do
    assoc_list =
      QueryBuilder.AssocList.build(
        source_schema,
        QueryBuilder.AssocList.new(source_schema),
        assoc_fields,
        join: :inner
      )

    ecto_query = QueryBuilder.JoinMaker.make_joins(ecto_query, assoc_list)

    ecto_query =
      ecto_query
      |> maybe_where(assoc_list, scope_filters, [])
      |> maybe_where(assoc_list, filters, or_filters)

    ecto_query
  end

  defp maybe_where(ecto_query, _assoc_list, [], []), do: ecto_query

  defp maybe_where(ecto_query, assoc_list, filters, or_filters) do
    QueryBuilder.Query.Where.where(ecto_query, assoc_list, filters, or_filters)
  end

  defp correlate_to_parent!(ecto_query, source_schema) do
    primary_keys = source_schema.__schema__(:primary_key)

    if primary_keys == [] do
      raise ArgumentError,
            "where_exists_subquery/where_not_exists_subquery require the root schema to have a primary key " <>
              "so the EXISTS subquery can be correlated via parent_as/1; " <>
              "got schema with no primary key: #{inspect(source_schema)}"
    end

    correlation_dynamic =
      primary_keys
      |> Enum.map(fn primary_key ->
        Ecto.Query.dynamic(
          [{^source_schema, x}],
          field(x, ^primary_key) == field(parent_as(^source_schema), ^primary_key)
        )
      end)
      |> Enum.reduce(fn left, right -> Ecto.Query.dynamic(^left and ^right) end)

    Ecto.Query.where(ecto_query, ^correlation_dynamic)
  end
end
