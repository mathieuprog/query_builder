defmodule QueryBuilder.Pagination do
  @moduledoc false

  def paginate_cursor(%QueryBuilder.Query{} = query, repo, opts \\ []) do
    QueryBuilder.Pagination.Cursor.paginate_cursor(query, repo, opts)
  end

  def paginate_offset(%QueryBuilder.Query{} = query, repo, opts \\ []) do
    QueryBuilder.Pagination.Offset.paginate_offset(query, repo, opts)
  end
end
