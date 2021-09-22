defmodule QueryBuilder.Query.Limit do
  @moduledoc false

  require Ecto.Query

  def limit(ecto_query, _assoc_fields, value) do
    apply_limit(ecto_query, value)
  end

  defp apply_limit(query, value) when is_integer(value) do
    Ecto.Query.limit(query, ^value)
  end

  defp apply_limit(query, value) do
    case Integer.parse(value) do
      {int_value, _rest} ->
        apply_limit(query, int_value)

      _ ->
        raise Ecto.QueryError,
          message: "Limit value must be integer. Got #{inspect(value)}",
          query: query
    end
  end
end
