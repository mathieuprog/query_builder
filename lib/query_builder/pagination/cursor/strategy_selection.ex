defmodule QueryBuilder.Pagination.Cursor.StrategySelection do
  @moduledoc false

  require Ecto.Query

  alias QueryBuilder.Pagination.Cursor.Plan
  alias QueryBuilder.Pagination.CursorCodec
  alias QueryBuilder.Pagination.Utils, as: PaginationUtils

  def plan(%QueryBuilder.Query{} = query, repo, opts) do
    page_size = PaginationUtils.normalize_page_size_opts!(opts, "paginate_cursor/3")
    cursor_direction = normalize_cursor_direction!(opts)

    base_ecto_query = PaginationUtils.base_ecto_query_from_query(query)
    base_query_has_preloads? = PaginationUtils.base_query_has_preloads?(base_ecto_query)

    PaginationUtils.raise_if_base_query_has_order_bys!(base_ecto_query, "paginate_cursor/3")

    root_schema = QueryBuilder.Utils.root_schema(base_ecto_query)
    primary_key_fields = root_schema.__schema__(:primary_key)
    ensure_root_has_primary_key!(root_schema, primary_key_fields)

    cursor = CursorCodec.decode_cursor!(Keyword.get(opts, :cursor))

    query =
      query
      |> QueryBuilder.limit(page_size + 1)
      |> PaginationUtils.ensure_primary_key_order_by(primary_key_fields)
      |> maybe_reverse_order_by_for_before(cursor_direction)

    order_by_list = order_by_list_from_operations(query.operations)
    validate_cursor_pagination_supported!(order_by_list)

    if cursor != %{} do
      CursorCodec.validate_cursor_matches_order_by!(cursor, order_by_list)
    end

    query = maybe_apply_cursor_filters(query, repo, order_by_list, cursor)

    {ecto_query, assoc_list} = QueryBuilder.Query.to_query_and_assoc_list(query)
    PaginationUtils.ensure_paginate_select_is_root!(ecto_query)

    cursor_fields_extractable? =
      cursor_fields_extractable_from_entries?(root_schema, assoc_list, order_by_list)

    unique_roots_safe? = PaginationUtils.only_to_one_assoc_joins?(ecto_query, root_schema)
    has_to_many_preloads? = PaginationUtils.has_to_many_preloads?(assoc_list)
    has_through_join_preloads? = PaginationUtils.has_through_join_preloads?(assoc_list)

    strategy =
      choose_cursor_pagination_strategy(
        base_query_has_preloads?: base_query_has_preloads?,
        cursor_fields_extractable?: cursor_fields_extractable?,
        unique_roots_safe?: unique_roots_safe?,
        has_to_many_preloads?: has_to_many_preloads?,
        has_through_join_preloads?: has_through_join_preloads?
      )

    cursor_select_map =
      if strategy == :single_query do
        %{}
      else
        build_cursor_select_map(assoc_list, order_by_list)
      end

    %Plan{
      repo: repo,
      page_size: page_size,
      cursor_direction: cursor_direction,
      order_by_list: order_by_list,
      root_schema: root_schema,
      primary_key_fields: primary_key_fields,
      base_query_has_preloads?: base_query_has_preloads?,
      ecto_query: ecto_query,
      assoc_list: assoc_list,
      cursor_select_map: cursor_select_map,
      strategy: strategy
    }
  end

  defp normalize_cursor_direction!(opts) do
    cursor_direction = Keyword.get(opts, :direction, :after)

    unless cursor_direction in [:after, :before] do
      raise ArgumentError,
            "paginate_cursor/3 cursor direction #{inspect(cursor_direction)} is invalid"
    end

    cursor_direction
  end

  defp ensure_root_has_primary_key!(root_schema, primary_key_fields)
       when is_atom(root_schema) and is_list(primary_key_fields) do
    if primary_key_fields == [] do
      raise ArgumentError,
            "paginate_cursor/3 requires the root schema to have a primary key so it can append a stable tie-breaker " <>
              "and reload unique root rows; got schema with no primary key: #{inspect(root_schema)}. " <>
              "If you truly need raw SQL-row pagination, use `limit/2` + `offset/2` directly on an Ecto query."
    end

    :ok
  end

  defp maybe_reverse_order_by_for_before(%QueryBuilder.Query{} = query, :before) do
    operations =
      Enum.map(query.operations, fn
        {:order_by, assocs, [keyword_list]} ->
          updated_keyword_list =
            Enum.map(keyword_list, fn {direction, field} ->
              {CursorCodec.reverse_order_direction(direction, field), field}
            end)

          {:order_by, assocs, [updated_keyword_list]}

        operation ->
          operation
      end)

    Map.put(query, :operations, operations)
  end

  defp maybe_reverse_order_by_for_before(%QueryBuilder.Query{} = query, :after), do: query

  defp validate_cursor_pagination_supported!(order_by_list) do
    cursor_pagination_supported? =
      Enum.all?(order_by_list, fn {direction, field} ->
        CursorCodec.cursorable_order_by_field?(field) and
          CursorCodec.supported_cursor_order_direction?(direction)
      end)

    if not cursor_pagination_supported? do
      raise ArgumentError,
            "paginate_cursor/3 requires cursorable order_by fields to support cursor pagination; " <>
              "got: #{inspect(order_by_list)}. " <>
              "Fix: use cursorable order_by fields (atoms/strings, including tokens like :name@role), or use `paginate_offset/3`."
    end

    :ok
  end

  defp maybe_apply_cursor_filters(%QueryBuilder.Query{} = query, _repo, _order_by_list, cursor)
       when is_map(cursor) and map_size(cursor) == 0,
       do: query

  defp maybe_apply_cursor_filters(%QueryBuilder.Query{} = query, repo, order_by_list, cursor)
       when is_map(cursor) do
    filters = build_keyset_or_filters(repo, order_by_list, cursor)

    case filters do
      [] ->
        query

      [first_filter | rest_filters] ->
        or_filters = Enum.map(rest_filters, &{:or, &1})
        QueryBuilder.where(query, [], first_filter, or_filters)
    end
  end

  defp choose_cursor_pagination_strategy(opts) when is_list(opts) do
    base_query_has_preloads? = Keyword.fetch!(opts, :base_query_has_preloads?)
    cursor_fields_extractable? = Keyword.fetch!(opts, :cursor_fields_extractable?)
    unique_roots_safe? = Keyword.fetch!(opts, :unique_roots_safe?)
    has_to_many_preloads? = Keyword.fetch!(opts, :has_to_many_preloads?)
    has_through_join_preloads? = Keyword.fetch!(opts, :has_through_join_preloads?)

    cond do
      cursor_fields_extractable? and
        unique_roots_safe? and
          (not base_query_has_preloads? or not has_to_many_preloads?) ->
        :single_query

      not base_query_has_preloads? and
        unique_roots_safe? and
        not cursor_fields_extractable? and
          not has_through_join_preloads? ->
        :cursor_projection

      true ->
        :keys_first
    end
  end

  def ensure_root_keys_unique!(keys_all, order_by_list) when is_list(keys_all) do
    if length(keys_all) != length(Enum.uniq(keys_all)) do
      raise ArgumentError,
            "paginate_cursor/3 could not produce a page of unique root rows; " <>
              "this usually means your order_by depends on a to-many join (e.g. ordering by a has_many field). " <>
              "Use an aggregation (e.g. max/min) or order by root/to-one fields only. " <>
              "order_by: #{inspect(order_by_list)}"
    end

    :ok
  end

  defp order_by_list_from_operations(operations) when is_list(operations) do
    operations
    |> Enum.filter(&match?({:order_by, _, _}, &1))
    |> Enum.reverse()
    |> Enum.flat_map(fn {:order_by, _assocs, [keyword_list]} -> keyword_list end)
    |> Enum.map(fn {direction, field} ->
      if is_atom(field) or is_binary(field) do
        {direction, to_string(field)}
      else
        {direction, field}
      end
    end)
    |> Enum.uniq_by(fn {_direction, field} -> field end)
  end

  defp build_keyset_or_filters(repo, order_by_list, cursor) do
    adapter = repo.__adapter__()

    {_, filters} =
      Enum.reduce(order_by_list, {[], []}, fn {direction, field},
                                              {prev_fields_rev, filters_rev} ->
        {dir, nulls} = CursorCodec.normalize_cursor_order_direction(adapter, direction, field)
        value = Map.fetch!(cursor, to_string(field))

        filters_rev =
          case keyset_groups_for_field(prev_fields_rev, field, dir, nulls, value) do
            [] -> filters_rev
            [group] -> [group | filters_rev]
            [group1, group2] -> [group2, group1 | filters_rev]
          end

        prev_fields_rev = [{field, value} | prev_fields_rev]
        {prev_fields_rev, filters_rev}
      end)

    Enum.reverse(filters)
  end

  defp cursor_fields_extractable_from_entries?(root_schema, assoc_list, order_by_list)
       when is_atom(root_schema) do
    Enum.all?(order_by_list, fn {_direction, token} ->
      token_str = to_string(token)

      case String.split(token_str, "@", parts: 3) do
        [_field] ->
          true

        [_field, assoc_field] ->
          assoc_field = String.to_existing_atom(assoc_field)

          not is_nil(root_schema.__schema__(:association, assoc_field)) and
            preloaded_to_one_root_assoc?(assoc_list, assoc_field)

        _ ->
          false
      end
    end)
  end

  defp preloaded_to_one_root_assoc?(%QueryBuilder.AssocList{} = assoc_list, assoc_field)
       when is_atom(assoc_field) do
    case QueryBuilder.AssocList.root_assoc(assoc_list, assoc_field) do
      %QueryBuilder.AssocList.Node{preload_spec: preload_spec, cardinality: :one}
      when not is_nil(preload_spec) ->
        true

      _ ->
        false
    end
  end

  defp build_cursor_select_map(assoc_list, order_by_list) do
    cursor_field_tokens = Enum.map(order_by_list, &elem(&1, 1))

    Enum.reduce(cursor_field_tokens, %{}, fn token, acc ->
      {field, binding} =
        QueryBuilder.Utils.find_field_and_binding_from_token(assoc_list, token)

      value_expr = Ecto.Query.dynamic([{^binding, x}], field(x, ^field))

      Map.put(acc, to_string(token), value_expr)
    end)
  end

  defp keyset_groups_for_field(prev_fields_rev, field, _dir, nulls, nil) do
    # If the cursor value is NULL, we can’t emit `field < NULL` / `field > NULL`.
    # Instead, we:
    #   - optionally include a branch for the non-NULL group (when NULLs sort first)
    #   - then rely on subsequent order_by fields for tie-breaking inside the NULL group
    case nulls do
      :first ->
        [[{field, :ne, nil} | prev_fields_rev]]

      :last ->
        []
    end
  end

  defp keyset_groups_for_field(prev_fields_rev, field, dir, nulls, value) do
    operator =
      case dir do
        :asc -> :gt
        :desc -> :lt
      end

    group = [{field, operator, value} | prev_fields_rev]

    # When NULLs sort last, NULL is after any non-NULL cursor value, so include it.
    case nulls do
      :last -> [group, [{field, nil} | prev_fields_rev]]
      :first -> [group]
    end
  end
end
