defmodule QueryBuilder.Query.Join do
  @moduledoc false

  def join(%QueryBuilder.Query{ecto_query: ecto_query, token: token}, assoc_fields, type) do
    token = QueryBuilder.Token.token(ecto_query, token, assoc_fields)

    QueryBuilder.JoinMaker.make_joins(ecto_query, token, type: type)
  end

  def join(query, assoc_fields, type) do
    join(%QueryBuilder.Query{ecto_query: query, token: nil}, assoc_fields, type)
  end
end
