defmodule QueryBuilder.Pagination.Cursor do
  @moduledoc false

  alias QueryBuilder.Pagination.Cursor.CursorProjection
  alias QueryBuilder.Pagination.Cursor.KeysFirst
  alias QueryBuilder.Pagination.Cursor.SingleQuery
  alias QueryBuilder.Pagination.Cursor.StrategySelection
  alias QueryBuilder.Pagination.CursorCodec

  def paginate_cursor(%QueryBuilder.Query{} = query, repo, opts \\ []) do
    if Keyword.has_key?(opts, :unsafe_sql_row_pagination?) do
      raise ArgumentError,
            "paginate_cursor/3 does not support `unsafe_sql_row_pagination?`; " <>
              "use `paginate_offset/3` for offset/row pagination."
    end

    QueryBuilder.Utils.with_token_cache(fn ->
      plan = StrategySelection.plan(query, repo, opts)

      {entries, first_row_cursor_map, last_row_cursor_map, has_more?} =
        case plan.strategy do
          :single_query -> SingleQuery.run(plan)
          :cursor_projection -> CursorProjection.run(plan)
          :keys_first -> KeysFirst.run(plan)
        end

      build_cursor = fn
        nil -> nil
        cursor_map when is_map(cursor_map) -> CursorCodec.encode_cursor(cursor_map)
      end

      %{
        pagination: %{
          cursor_direction: plan.cursor_direction,
          cursor_for_entries_before: build_cursor.(first_row_cursor_map),
          cursor_for_entries_after: build_cursor.(last_row_cursor_map),
          has_more_entries: has_more?,
          max_page_size: plan.page_size
        },
        paginated_entries: entries
      }
    end)
  end
end
