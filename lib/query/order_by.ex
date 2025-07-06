defmodule QueryBuilder.Query.OrderBy do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def order_by(ecto_query, assoc_list, values) do
    dynamic = build_dynamic(ecto_query, assoc_list, values)

    Ecto.Query.order_by(ecto_query, ^dynamic)
  end

  def build_dynamic(ecto_query, assoc_list, values) do
    values
    |> Enum.filter(&(&1 != []))
    |> Enum.map(fn
      {direction, custom_fun} when is_function(custom_fun) ->
        {direction, custom_fun.(&find_field_and_binding_from_token(ecto_query, assoc_list, &1))}

      {direction, field} ->
        {field, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, field)
        {direction, Ecto.Query.dynamic([{^binding, x}], field(x, ^field))}
    end)
  end
end
