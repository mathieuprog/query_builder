defmodule CustomQueryBuilder do
  use QueryBuilder.Extension
  require Ecto.Query

  defmacro __using__(opts) do
    quote do
      require QueryBuilder
      QueryBuilder.__using__(unquote(opts))
    end
  end

  def where_initcap(query, field, value) do
    text_equals_condition = fn field, value, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      Ecto.Query.dynamic([{^binding, x}], fragment("initcap(?)", ^value) == field(x, ^field))
    end

    query
    |> where(&text_equals_condition.(field, value, &1))
  end
end
