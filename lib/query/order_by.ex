defmodule QueryBuilder.Query.OrderBy do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Query.Helper

  def order_by(query, assoc_fields, value) do
    token = QueryBuilder.Token.token(query, assoc_fields)

    {query, token} = QueryBuilder.JoinMaker.make_joins(query, token)

    apply_order_values(query, token, List.wrap(value))
  end

  defp apply_order_values(query, _token, []), do: query

  defp apply_order_values(query, token, [order | tail]) do
    query = apply_order(query, token, order)
    apply_order_values(query, token, tail)
  end

  defp apply_order(query, token, {field, direction}) do
    {field, binding} = field_and_binding(query, token, field)

    Ecto.Query.order_by(query, [{^binding, x}], [{^direction, field(x, ^field)}])
  end
end
