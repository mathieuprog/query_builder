defmodule QueryBuilder do
  require Ecto.Query
  alias Ecto.Query

  defmacro __using__(opts) do
    quote do
      require QueryBuilder.Schema
      QueryBuilder.Schema.__using__(unquote(opts))
    end
  end

  def preload(query, assoc_fields) do
    ensure_query_has_binding(query)
    |> QueryBuilder.Query.Preload.preload(assoc_fields)
  end

  def where(query, filters) do
    where(query, [], filters)
  end

  def where(query, assoc_fields, filters) do
    ensure_query_has_binding(query)
    |> QueryBuilder.Query.Where.where(assoc_fields, filters)
  end

  def order_by(query, value) do
    order_by(query, [], value)
  end

  def order_by(query, assoc_fields, value) do
    ensure_query_has_binding(query)
    |> QueryBuilder.Query.OrderBy.order_by(assoc_fields, value)
  end

  def join(query, assoc_fields, type) do
    ensure_query_has_binding(query)
    |> QueryBuilder.Query.Join.join(assoc_fields, type)
  end

  def from_list(query, []), do: query

  def from_list(query, [{operation, arguments} | tail]) do
    apply(__MODULE__, operation, [query, arguments])
    |> from_list(tail)
  end

  defp ensure_query_has_binding(query) do
    schema = QueryBuilder.Utils.root_schema(query)

    unless Query.has_named_binding?(query, schema.binding()) do
      schema.query()
    else
      query
    end
  end
end
