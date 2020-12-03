defmodule QueryBuilder.Query do
  @moduledoc false
  defstruct ecto_query: nil, token: %{}
end

defimpl Ecto.Queryable, for: QueryBuilder.Query do
  def to_query(%{ecto_query: ecto_query, token: token}) do
    QueryBuilder.Query.Preload.do_preload(ecto_query, token, Map.fetch!(token, :preload))
    |> Ecto.Queryable.to_query()
  end
end
