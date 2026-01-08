defmodule QueryBuilder.Query do
  @moduledoc false
  defstruct(
    ecto_query: nil,
    operations: []
  )

  @doc false
  def to_query_and_assoc_list(%__MODULE__{} = query) do
    QueryBuilder.Query.Planner.compile(query)
  end
end

defimpl Ecto.Queryable, for: QueryBuilder.Query do
  def to_query(%QueryBuilder.Query{} = query) do
    {ecto_query, _assoc_list} = QueryBuilder.Query.to_query_and_assoc_list(query)
    ecto_query
  end
end
