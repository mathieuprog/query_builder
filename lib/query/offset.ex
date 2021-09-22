defmodule QueryBuilder.Query.Offset do
  @moduledoc false

  require Ecto.Query

  def offset(ecto_query, _assoc_fields, value) do
    apply_offset(ecto_query, value)
  end

  defp apply_offset(query, value) when is_integer(value) do
    Ecto.Query.offset(query, ^value)
  end

  defp apply_offset(query, value) do
    case Integer.parse(value) do
      {int_value, _rest} ->
        apply_offset(query, int_value)

      _ ->
        raise Ecto.QueryError,
          message: "Offset value must be integer. Got #{inspect(value)}",
          query: query
    end
  end
end
