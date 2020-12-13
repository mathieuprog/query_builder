defmodule QueryBuilder.Query.OrderBy do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def order_by(ecto_query, assoc_list, value) do
    apply_order_values(ecto_query, assoc_list, List.wrap(value))
  end

  defp apply_order_values(query, _assoc_list, []), do: query

  defp apply_order_values(query, assoc_list, [order | tail]) do
    query = apply_order(query, assoc_list, order)
    apply_order_values(query, assoc_list, tail)
  end

  defp apply_order(query, assoc_list, {field, direction}) do
    {field, binding} = find_field_and_binding_from_token(query, assoc_list, field)

    Ecto.Query.order_by(query, [{^binding, x}], [{^direction, field(x, ^field)}])
  end
end
