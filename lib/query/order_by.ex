defmodule QueryBuilder.Query.OrderBy do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def order_by(ecto_query, assoc_list, values) do
    dynamic = build_dynamic(assoc_list, values)

    Ecto.Query.order_by(ecto_query, ^dynamic)
  end

  defp build_dynamic(assoc_list, values) do
    values
    |> Enum.filter(&(&1 != []))
    |> Enum.map(fn
      {direction, %QueryBuilder.Aggregate{} = aggregate} when is_atom(direction) ->
        {direction, QueryBuilder.Aggregate.to_dynamic(assoc_list, aggregate)}

      {direction, %Ecto.Query.DynamicExpr{} = dynamic} when is_atom(direction) ->
        {direction, dynamic}

      {direction, custom_fun} when is_function(custom_fun) ->
        {direction, custom_fun.(&find_field_and_binding_from_token(assoc_list, &1))}

      {direction, field} ->
        {field, binding} = find_field_and_binding_from_token(assoc_list, field)
        {direction, Ecto.Query.dynamic([{^binding, x}], field(x, ^field))}
    end)
  end
end
