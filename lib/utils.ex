defmodule QueryBuilder.Utils do
  @moduledoc false

  def root_schema(query) do
    %{from: %{source: {_, context}}} = Ecto.Queryable.to_query(query)
    context
  end

  def to_string(query), do: Inspect.Ecto.Query.to_string(query)
end
