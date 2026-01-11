defmodule QueryBuilder.Pagination.Cursor.SingleQuery do
  @moduledoc false

  alias QueryBuilder.Pagination.Cursor.Plan
  alias QueryBuilder.Pagination.Utils, as: PaginationUtils

  def run(%Plan{} = plan) do
    {page_ecto_query, deferred_preloads} =
      if plan.base_query_has_preloads? do
        {plan.ecto_query, []}
      else
        QueryBuilder.Query.Preload.split_for_pagination(plan.ecto_query, plan.assoc_list)
      end

    {entries, has_more?} =
      page_ecto_query
      |> plan.repo.all()
      |> PaginationUtils.normalize_paginated_rows(plan.page_size, plan.cursor_direction)

    entries =
      PaginationUtils.maybe_apply_deferred_preloads(plan.repo, entries, deferred_preloads)

    first_entry = List.first(entries)
    last_entry = List.last(entries)

    {entries, cursor_map_from_entry(first_entry, plan.order_by_list),
     cursor_map_from_entry(last_entry, plan.order_by_list), has_more?}
  end

  defp cursor_map_from_entry(nil, _order_by_list), do: nil

  defp cursor_map_from_entry(entry, order_by_list) do
    Enum.reduce(order_by_list, %{}, fn {_direction, token}, acc ->
      {token_str, value} = cursor_value_from_entry(entry, token)
      Map.put(acc, token_str, value)
    end)
  end

  defp cursor_value_from_entry(entry, token) do
    token_str = to_string(token)

    case String.split(token_str, "@", parts: 3) do
      [field] ->
        field = String.to_existing_atom(field)
        {token_str, Map.fetch!(entry, field)}

      [field, assoc_field] ->
        field = String.to_existing_atom(field)
        assoc_field = String.to_existing_atom(assoc_field)
        assoc = Map.fetch!(entry, assoc_field)

        value =
          cond do
            match?(%Ecto.Association.NotLoaded{}, assoc) ->
              raise ArgumentError,
                    "paginate_cursor/3 internal error: expected association #{inspect(assoc_field)} to be preloaded " <>
                      "in order to build cursor field #{inspect(token_str)} from the returned structs"

            is_nil(assoc) ->
              nil

            is_map(assoc) ->
              Map.fetch!(assoc, field)

            true ->
              raise ArgumentError,
                    "paginate_cursor/3 internal error: expected association #{inspect(assoc_field)} to be a struct or nil, got: #{inspect(assoc)}"
          end

        {token_str, value}

      _ ->
        raise ArgumentError,
              "paginate_cursor/3 internal error: unexpected cursor token #{inspect(token_str)}"
    end
  end
end
