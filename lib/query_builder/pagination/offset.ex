defmodule QueryBuilder.Pagination.Offset do
  @moduledoc false

  require Ecto.Query
  alias Ecto.Query

  alias QueryBuilder.Pagination.KeysFirst
  alias QueryBuilder.Pagination.Utils, as: PaginationUtils

  def paginate_offset(%QueryBuilder.Query{} = query, repo, opts \\ []) do
    if Keyword.has_key?(opts, :unsafe_sql_row_pagination?) do
      raise ArgumentError,
            "paginate_offset/3 does not accept `unsafe_sql_row_pagination?`; " <>
              "offset/row pagination is already explicit in this function."
    end

    if not is_nil(Keyword.get(opts, :cursor)) do
      raise ArgumentError,
            "paginate_offset/3 does not support `cursor:`; use `paginate_cursor/3` for cursor pagination."
    end

    if Keyword.has_key?(opts, :direction) do
      raise ArgumentError,
            "paginate_offset/3 does not support `direction:`; " <>
              "use `offset/2` to move pages (or use `paginate_cursor/3` for cursor pagination)."
    end

    QueryBuilder.Utils.with_token_cache(fn ->
      page_size = PaginationUtils.normalize_page_size_opts!(opts, "paginate_offset/3")

      base_ecto_query = PaginationUtils.base_ecto_query_from_query(query)
      base_query_has_preloads? = PaginationUtils.base_query_has_preloads?(base_ecto_query)

      PaginationUtils.raise_if_base_query_has_order_bys!(base_ecto_query, "paginate_offset/3")

      root_schema = QueryBuilder.Utils.root_schema(base_ecto_query)
      primary_key_fields = root_schema.__schema__(:primary_key)

      if primary_key_fields == [] do
        raise ArgumentError,
              "paginate_offset/3 requires the root schema to have a primary key so it can return unique root rows; " <>
                "got schema with no primary key: #{inspect(root_schema)}. " <>
                "If you truly need raw SQL-row pagination, use `limit/2` + `offset/2` directly on an Ecto query."
      end

      query =
        query
        |> QueryBuilder.limit(page_size + 1)
        |> PaginationUtils.ensure_primary_key_order_by(primary_key_fields)

      {ecto_query, assoc_list} = QueryBuilder.Query.to_query_and_assoc_list(query)

      PaginationUtils.ensure_paginate_select_is_root!(ecto_query)

      {entries, has_more?} =
        if PaginationUtils.only_to_one_assoc_joins?(ecto_query, root_schema) do
          {page_ecto_query, deferred_preloads} =
            if base_query_has_preloads? do
              {ecto_query, []}
            else
              QueryBuilder.Query.Preload.split_for_pagination(ecto_query, assoc_list)
            end

          {entries, has_more?} =
            page_ecto_query
            |> repo.all()
            |> PaginationUtils.normalize_offset_paginated_rows(page_size)

          entries =
            PaginationUtils.maybe_apply_deferred_preloads(repo, entries, deferred_preloads)

          {entries, has_more?}
        else
          order_by_entries = offset_order_by_entries(query.operations)

          page_keys_select_map =
            build_offset_page_keys_select_map(assoc_list, primary_key_fields, order_by_entries)

          page_keys_query =
            ecto_query
            |> Query.exclude([:preload, :select])
            |> Ecto.Query.select([{^root_schema, _x}], ^page_keys_select_map)
            |> Ecto.Query.distinct(true)

          keys_all =
            page_keys_query
            |> repo.all()
            |> Enum.map(&PaginationUtils.primary_key_value_from_row(&1, primary_key_fields))

          if length(keys_all) != length(Enum.uniq(keys_all)) do
            raise ArgumentError,
                  "paginate_offset/3 could not produce a page of unique root rows; " <>
                    "this usually means your order_by depends on a to-many join (e.g. ordering by a has_many field). " <>
                    "Fix: order by root/to-one fields only, or use an aggregate (e.g. max/min) to order groups."
          end

          {keys, has_more?} = PaginationUtils.normalize_offset_paginated_rows(keys_all, page_size)

          entries =
            KeysFirst.load_entries_for_page(
              repo,
              ecto_query,
              root_schema,
              primary_key_fields,
              keys
            )

          {entries, has_more?}
        end

      %{
        pagination: %{
          has_more_entries: has_more?,
          max_page_size: page_size
        },
        paginated_entries: entries
      }
    end)
  end

  defp offset_order_by_entries(operations) when is_list(operations) do
    operations
    |> Enum.filter(&match?({:order_by, _assocs, _args}, &1))
    |> Enum.reverse()
    |> Enum.flat_map(fn
      {:order_by, _assocs, [keyword_list]} when is_list(keyword_list) ->
        Enum.reject(keyword_list, &(&1 == []))

      _ ->
        []
    end)
  end

  defp build_offset_page_keys_select_map(assoc_list, pk_fields, order_by_entries)
       when is_list(pk_fields) and is_list(order_by_entries) do
    pk_select_map =
      Enum.reduce(pk_fields, %{}, fn pk_field, acc ->
        {field, binding} =
          QueryBuilder.Utils.find_field_and_binding_from_token(assoc_list, pk_field)

        Map.put(
          acc,
          Atom.to_string(pk_field),
          Ecto.Query.dynamic([{^binding, x}], field(x, ^field))
        )
      end)

    Enum.reduce(Enum.with_index(order_by_entries, 1), pk_select_map, fn
      {{_direction, %QueryBuilder.Aggregate{} = aggregate}, index}, acc ->
        Map.put(
          acc,
          "__qb__order_by__#{index}",
          QueryBuilder.Aggregate.to_dynamic(assoc_list, aggregate)
        )

      {{_direction, %Ecto.Query.DynamicExpr{} = dynamic}, index}, acc ->
        Map.put(acc, "__qb__order_by__#{index}", dynamic)

      {{_direction, custom_fun}, index}, acc when is_function(custom_fun, 1) ->
        Map.put(
          acc,
          "__qb__order_by__#{index}",
          custom_fun.(&QueryBuilder.Utils.find_field_and_binding_from_token(assoc_list, &1))
        )

      {{_direction, token}, index}, acc when is_atom(token) or is_binary(token) ->
        {field, binding} = QueryBuilder.Utils.find_field_and_binding_from_token(assoc_list, token)

        Map.put(
          acc,
          "__qb__order_by__#{index}",
          Ecto.Query.dynamic([{^binding, x}], field(x, ^field))
        )

      {{_direction, other}, _index}, _acc ->
        raise ArgumentError,
              "paginate_offset/3 internal error: unexpected order_by field #{inspect(other)}"
    end)
  end
end
