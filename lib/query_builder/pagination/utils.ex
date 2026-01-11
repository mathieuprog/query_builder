defmodule QueryBuilder.Pagination.Utils do
  @moduledoc false

  def normalize_page_size_opts!(opts, context) when is_list(opts) do
    page_size = Keyword.get(opts, :page_size, QueryBuilder.default_page_size())

    unless is_integer(page_size) and page_size >= 1 do
      raise ArgumentError,
            "#{context} page_size must be a positive integer, got: #{inspect(page_size)}"
    end

    max_page_size = Keyword.get(opts, :max_page_size)

    if not is_nil(max_page_size) and not (is_integer(max_page_size) and max_page_size >= 1) do
      raise ArgumentError,
            "#{context} max_page_size must be a positive integer, got: #{inspect(max_page_size)}"
    end

    if is_nil(max_page_size) do
      page_size
    else
      min(max_page_size, page_size)
    end
  end

  def normalize_page_size_opts!(opts, context) do
    raise ArgumentError,
          "#{context} expects opts to be a keyword list, got: #{inspect(opts)}"
  end

  def base_ecto_query_from_query(%QueryBuilder.Query{} = query) do
    case query.ecto_query do
      %Ecto.Query{} = ecto_query -> ecto_query
      other -> Ecto.Queryable.to_query(other)
    end
  end

  def base_query_has_preloads?(%Ecto.Query{} = base_ecto_query) do
    base_ecto_query.preloads != [] or base_ecto_query.assocs != []
  end

  def raise_if_base_query_has_order_bys!(%Ecto.Query{} = base_ecto_query, context) do
    if base_ecto_query.order_bys != [] do
      raise ArgumentError,
            "#{context} does not support paginating a query whose base ecto_query already has `order_by` clauses; " <>
              "express ordering via `QueryBuilder.order_by/*` (or remove base ordering via `Ecto.Query.exclude(base_query, :order_by)`) " <>
              "before calling #{context}. base order_bys: #{inspect(base_ecto_query.order_bys)}"
    end

    :ok
  end

  def ensure_primary_key_order_by(%QueryBuilder.Query{} = query, primary_key_fields)
      when is_list(primary_key_fields) do
    existing_order_fields =
      query.operations
      |> Enum.flat_map(fn
        {:order_by, _assocs, [keyword_list]} ->
          Enum.flat_map(keyword_list, fn
            {_direction, field} when is_atom(field) or is_binary(field) ->
              [to_string(field)]

            _ ->
              []
          end)

        _ ->
          []
      end)
      |> MapSet.new()

    missing_pk_fields =
      Enum.reject(primary_key_fields, fn pk_field ->
        MapSet.member?(existing_order_fields, Atom.to_string(pk_field))
      end)

    case missing_pk_fields do
      [] ->
        query

      pk_fields ->
        QueryBuilder.order_by(query, Enum.map(pk_fields, &{:asc, &1}))
    end
  end

  def ensure_paginate_select_is_root!(ecto_query) do
    case ecto_query.select do
      nil ->
        :ok

      %Ecto.Query.SelectExpr{expr: {:&, _, [0]}} ->
        :ok

      %Ecto.Query.SelectExpr{} = select ->
        raise ArgumentError,
              "paginate_cursor/3 and paginate_offset/3 do not support custom select expressions; " <>
                "expected selecting the root schema struct (e.g. `select: u` or no select), got: #{inspect(select.expr)}"
    end
  end

  def only_to_one_assoc_joins?(%Ecto.Query{} = ecto_query, root_schema)
      when is_atom(root_schema) do
    schemas_by_index = %{0 => root_schema}

    Enum.reduce_while(
      Enum.with_index(ecto_query.joins, 1),
      schemas_by_index,
      fn {join, join_index}, schemas_by_index ->
        case join do
          %Ecto.Query.JoinExpr{assoc: {parent_index, assoc_field}}
          when is_integer(parent_index) and is_atom(assoc_field) ->
            with {:ok, parent_schema} <- Map.fetch(schemas_by_index, parent_index),
                 %{cardinality: :one, queryable: assoc_schema} <-
                   parent_schema.__schema__(:association, assoc_field),
                 true <- is_atom(assoc_schema) do
              {:cont, Map.put(schemas_by_index, join_index, assoc_schema)}
            else
              _ -> {:halt, :unsafe}
            end

          _ ->
            {:halt, :unsafe}
        end
      end
    ) != :unsafe
  end

  def has_to_many_preloads?(%QueryBuilder.AssocList{} = assoc_list) do
    QueryBuilder.AssocList.any?(assoc_list, fn assoc_data ->
      assoc_data.preload_spec != nil and assoc_data.cardinality == :many
    end)
  end

  def has_through_join_preloads?(%QueryBuilder.AssocList{} = assoc_list) do
    QueryBuilder.AssocList.any?(assoc_list, fn
      %QueryBuilder.AssocList.Node{
        preload_spec: %QueryBuilder.AssocList.PreloadSpec{strategy: :through_join}
      } ->
        true

      _ ->
        false
    end)
  end

  def primary_key_value_from_row(row, [pk_field]) do
    Map.fetch!(row, Atom.to_string(pk_field))
  end

  def primary_key_value_from_row(row, pk_fields) when is_list(pk_fields) do
    pk_fields
    |> Enum.map(&Map.fetch!(row, Atom.to_string(&1)))
    |> List.to_tuple()
  end

  def primary_key_value_from_entry(entry, [pk_field]) do
    Map.fetch!(entry, pk_field)
  end

  def primary_key_value_from_entry(entry, pk_fields) when is_list(pk_fields) do
    pk_fields
    |> Enum.map(&Map.fetch!(entry, &1))
    |> List.to_tuple()
  end

  def maybe_apply_deferred_preloads(_repo, [], _preloads), do: []

  def maybe_apply_deferred_preloads(repo, entries, preloads) when is_list(entries) do
    case preloads do
      [] -> entries
      _ -> repo.preload(entries, preloads)
    end
  end

  def normalize_paginated_rows(rows, page_size, cursor_direction) do
    rows = reverse_if_before(rows, cursor_direction)
    has_more? = length(rows) == page_size + 1

    rows =
      if has_more? do
        case cursor_direction do
          :before -> tl(rows)
          :after -> List.delete_at(rows, -1)
        end
      else
        rows
      end

    {rows, has_more?}
  end

  def normalize_offset_paginated_rows(rows, page_size) do
    has_more? = length(rows) == page_size + 1

    rows =
      if has_more? do
        List.delete_at(rows, -1)
      else
        rows
      end

    {rows, has_more?}
  end

  defp reverse_if_before(rows, :before), do: Enum.reverse(rows)
  defp reverse_if_before(rows, :after), do: rows
end
