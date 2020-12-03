defmodule QueryBuilder.Query.OrderBy do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def order_by(%QueryBuilder.Query{ecto_query: ecto_query, token: token}, assoc_fields, value) do
    token = QueryBuilder.Token.token(ecto_query, token, assoc_fields)

    %QueryBuilder.Query{ecto_query: ecto_query, token: token} =
      QueryBuilder.JoinMaker.make_joins(ecto_query, token)

    %QueryBuilder.Query{
      ecto_query: apply_order_values(ecto_query, token, List.wrap(value)),
      token: token
    }
  end

  def order_by(query, assoc_fields, value) do
    order_by(%QueryBuilder.Query{ecto_query: query, token: nil}, assoc_fields, value)
  end

  defp apply_order_values(query, _token, []), do: query

  defp apply_order_values(query, token, [order | tail]) do
    query = apply_order(query, token, order)
    apply_order_values(query, token, tail)
  end

  defp apply_order(query, token, {field, direction}) do
    {field, binding} = find_field_and_binding_from_token(query, token, field)

    Ecto.Query.order_by(query, [{^binding, x}], [{^direction, field(x, ^field)}])
  end
end
