defmodule QueryBuilder.Pagination.Cursor.CursorProjection do
  @moduledoc false

  require Ecto.Query

  alias QueryBuilder.Pagination.Cursor.StrategySelection
  alias QueryBuilder.Pagination.Cursor.Plan
  alias QueryBuilder.Pagination.Utils, as: PaginationUtils

  @cursor_projection_root_key :__qb__cursor_projection_root__

  def run(%Plan{} = plan) do
    {page_ecto_query, deferred_preloads} =
      QueryBuilder.Query.Preload.split_for_pagination(plan.ecto_query, plan.assoc_list)

    if page_ecto_query.preloads != [] or page_ecto_query.assocs != [] do
      raise ArgumentError,
            "paginate_cursor/3 internal error: cursor projection pagination cannot be combined with through-join preloads"
    end

    root_value_expr = Ecto.Query.dynamic([{^plan.root_schema, x}], x)

    projection_select_map =
      plan.cursor_select_map
      |> Map.put(@cursor_projection_root_key, root_value_expr)

    rows_all =
      page_ecto_query
      |> Ecto.Query.select([{^plan.root_schema, _x}], ^projection_select_map)
      |> plan.repo.all()
      |> Enum.map(fn row ->
        {
          Map.fetch!(row, @cursor_projection_root_key),
          Map.delete(row, @cursor_projection_root_key)
        }
      end)

    keys_all =
      Enum.map(rows_all, fn {entry, _cursor_map} ->
        PaginationUtils.primary_key_value_from_entry(entry, plan.primary_key_fields)
      end)

    StrategySelection.ensure_root_keys_unique!(keys_all, plan.order_by_list)

    {rows, has_more?} =
      PaginationUtils.normalize_paginated_rows(rows_all, plan.page_size, plan.cursor_direction)

    entries = Enum.map(rows, &elem(&1, 0))

    entries =
      PaginationUtils.maybe_apply_deferred_preloads(plan.repo, entries, deferred_preloads)

    first_row = List.first(rows)
    last_row = List.last(rows)

    first_row_cursor_map = if is_nil(first_row), do: nil, else: elem(first_row, 1)
    last_row_cursor_map = if is_nil(last_row), do: nil, else: elem(last_row, 1)

    {entries, first_row_cursor_map, last_row_cursor_map, has_more?}
  end
end
