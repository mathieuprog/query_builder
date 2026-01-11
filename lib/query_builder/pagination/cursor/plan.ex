defmodule QueryBuilder.Pagination.Cursor.Plan do
  @moduledoc false

  @enforce_keys [
    :repo,
    :page_size,
    :cursor_direction,
    :order_by_list,
    :root_schema,
    :primary_key_fields,
    :base_query_has_preloads?,
    :ecto_query,
    :assoc_list,
    :cursor_select_map,
    :strategy
  ]

  defstruct repo: nil,
            page_size: nil,
            cursor_direction: nil,
            order_by_list: [],
            root_schema: nil,
            primary_key_fields: [],
            base_query_has_preloads?: false,
            ecto_query: nil,
            assoc_list: nil,
            cursor_select_map: %{},
            strategy: nil
end
