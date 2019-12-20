defmodule QueryBuilder.Query.Join do
  @moduledoc false

  def join(query, assoc_fields, type) do
    token = QueryBuilder.Token.token(query, assoc_fields)

    {query, _token} = QueryBuilder.JoinMaker.make_joins(query, token, type: type)

    query
  end
end
