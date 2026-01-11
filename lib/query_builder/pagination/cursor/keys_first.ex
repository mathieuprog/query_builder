defmodule QueryBuilder.Pagination.Cursor.KeysFirst do
  @moduledoc false

  require Ecto.Query
  alias Ecto.Query

  alias QueryBuilder.Pagination.Cursor.StrategySelection
  alias QueryBuilder.Pagination.Cursor.Plan
  alias QueryBuilder.Pagination.KeysFirst, as: KeysFirstLoader
  alias QueryBuilder.Pagination.Utils, as: PaginationUtils

  def run(%Plan{} = plan) do
    page_keys_query =
      plan.ecto_query
      |> Query.exclude([:preload, :select])
      |> Ecto.Query.select([{^plan.root_schema, x}], ^plan.cursor_select_map)
      |> Ecto.Query.distinct(true)

    page_key_rows_all = plan.repo.all(page_keys_query)

    keys_all =
      Enum.map(
        page_key_rows_all,
        &PaginationUtils.primary_key_value_from_row(&1, plan.primary_key_fields)
      )

    StrategySelection.ensure_root_keys_unique!(keys_all, plan.order_by_list)

    {page_key_rows, has_more?} =
      PaginationUtils.normalize_paginated_rows(
        page_key_rows_all,
        plan.page_size,
        plan.cursor_direction
      )

    keys =
      Enum.map(
        page_key_rows,
        &PaginationUtils.primary_key_value_from_row(&1, plan.primary_key_fields)
      )

    entries =
      KeysFirstLoader.load_entries_for_page(
        plan.repo,
        plan.ecto_query,
        plan.root_schema,
        plan.primary_key_fields,
        keys
      )

    first_row = List.first(page_key_rows)
    last_row = List.last(page_key_rows)

    {entries, first_row, last_row, has_more?}
  end
end
