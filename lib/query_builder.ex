defmodule QueryBuilder do
  require Ecto.Query
  alias Ecto.Query

  defmacro __using__(opts) do
    quote do
      require QueryBuilder.Schema
      QueryBuilder.Schema.__using__(unquote(opts))
    end
  end

  def new(ecto_query) do
    %QueryBuilder.Query{ecto_query: ensure_query_has_binding(ecto_query)}
  end

  @doc ~S"""
  Builds an `Ecto.SubQuery` using QueryBuilder operations.

  This is a convenience wrapper around `from_opts/3` (`mode: :full`) + `Ecto.Query.subquery/1`.

  Example:
  ```elixir
  user_ids =
    QueryBuilder.subquery(User,
      where: [deleted: false],
      select: :id
    )

  Article
  |> QueryBuilder.where({:author_id, :in, user_ids})
  |> Repo.all()
  ```
  """
  def subquery(queryable, opts \\ []) do
    queryable
    |> from_opts(opts, mode: :full)
    |> Ecto.Queryable.to_query()
    |> Ecto.Query.subquery()
  end

  def paginate(%QueryBuilder.Query{} = query, repo, opts \\ []) do
    page_size = Keyword.get(opts, :page_size, default_page_size())
    cursor_direction = Keyword.get(opts, :direction, :after)
    unsafe_sql_row_pagination? = Keyword.get(opts, :unsafe_sql_row_pagination?, false)

    unless is_integer(page_size) and page_size >= 1 do
      raise ArgumentError,
            "paginate/3 page_size must be a positive integer, got: #{inspect(page_size)}"
    end

    max_page_size = Keyword.get(opts, :max_page_size)

    if not is_nil(max_page_size) and not (is_integer(max_page_size) and max_page_size >= 1) do
      raise ArgumentError,
            "paginate/3 max_page_size must be a positive integer, got: #{inspect(max_page_size)}"
    end

    unless cursor_direction in [:after, :before] do
      raise ArgumentError, "cursor direction #{inspect(cursor_direction)} is invalid"
    end

    base_ecto_query = Ecto.Queryable.to_query(query.ecto_query)
    root_schema = QueryBuilder.Utils.root_schema(base_ecto_query)
    primary_key_fields = root_schema.__schema__(:primary_key)

    if primary_key_fields == [] and not unsafe_sql_row_pagination? do
      raise ArgumentError,
            "paginate/3 requires the root schema to have a primary key so it can append a stable tie-breaker " <>
              "and reload unique root rows; got schema with no primary key: #{inspect(root_schema)}. " <>
              "If you want SQL-row pagination (no cursor), pass `unsafe_sql_row_pagination?: true`."
    end

    if base_ecto_query.order_bys != [] do
      raise ArgumentError,
            "paginate/3 does not support paginating a query whose base ecto_query already has `order_by` clauses; " <>
              "express ordering via `QueryBuilder.order_by/*` (or remove base ordering via `Ecto.Query.exclude(base_query, :order_by)`) " <>
              "before calling paginate/3. base order_bys: #{inspect(base_ecto_query.order_bys)}"
    end

    page_size =
      if is_nil(max_page_size) do
        page_size
      else
        min(max_page_size, page_size)
      end

    cursor = decode_cursor!(Keyword.get(opts, :cursor))

    query = limit(query, page_size + 1)

    query =
      if primary_key_fields == [] do
        query
      else
        existing_order_fields =
          query.operations
          |> Enum.flat_map(fn
            %{type: :order_by, args: [keyword_list]} ->
              Enum.flat_map(keyword_list, fn
                {_direction, field} when is_atom(field) or is_binary(field) -> [to_string(field)]
                _ -> []
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
            order_by(query, Enum.map(pk_fields, &{:asc, &1}))
        end
      end

    # Reverse sorting order if direction is :before
    operations =
      if cursor_direction == :before do
        query.operations
        |> Enum.map(fn
          %{type: :order_by, args: [keyword_list]} = operation ->
            updated_keyword_list =
              Enum.map(keyword_list, fn {direction, field} ->
                {reverse_order_direction(direction, field), field}
              end)

            Map.put(operation, :args, [updated_keyword_list])

          operation ->
            operation
        end)
      else
        query.operations
      end

    query = Map.put(query, :operations, operations)

    order_by_list =
      query.operations
      |> Enum.filter(&match?(%{type: :order_by}, &1))
      |> Enum.reverse()
      |> Enum.flat_map(fn %{args: [keyword_list]} -> keyword_list end)
      |> Enum.map(fn {direction, field} ->
        if is_atom(field) or is_binary(field) do
          {direction, to_string(field)}
        else
          {direction, field}
        end
      end)
      |> Enum.uniq_by(fn {_direction, field} -> field end)

    cursor_pagination_supported? =
      primary_key_fields != [] and
        Enum.all?(order_by_list, fn {direction, field} ->
          cursorable_order_by_field?(field) and supported_cursor_order_direction?(direction)
        end)

    if cursor != %{} and not cursor_pagination_supported? do
      raise ArgumentError,
            "paginate/3 cursor pagination requires order_by fields to be simple fields (atoms/strings, including tokens like :name@role) " <>
              "with supported directions (:asc, :desc, :asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last); " <>
              "got: #{inspect(order_by_list)}. " <>
              "If you want to opt into SQL-row pagination (no cursor), pass `unsafe_sql_row_pagination?: true` and omit `cursor:`."
    end

    if not cursor_pagination_supported? and not unsafe_sql_row_pagination? do
      raise ArgumentError,
            "paginate/3 requires cursorable order_by fields to support cursor pagination; " <>
              "got: #{inspect(order_by_list)}. " <>
              "Fix: use cursorable order_by fields (atoms/strings, including tokens like :name@role), or pass " <>
              "`unsafe_sql_row_pagination?: true` to opt into SQL-row pagination (no cursor)."
    end

    if cursor != %{} do
      validate_cursor_matches_order_by!(cursor, order_by_list)
    end

    valid_cursor? = cursor_pagination_supported? and cursor != %{}

    query =
      if valid_cursor? do
        filters = build_keyset_or_filters(repo, order_by_list, cursor)

        case filters do
          [] ->
            query

          [first_filter | rest_filters] ->
            or_filters = Enum.map(rest_filters, &{:or, &1})
            where(query, [], first_filter, or_filters)
        end
      else
        query
      end

    {ecto_query, assoc_list} = QueryBuilder.Query.to_query_and_assoc_list(query)

    ensure_paginate_select_is_root!(ecto_query)

    {entries, first_row_cursor_map, last_row_cursor_map, has_more?} =
      if cursor_pagination_supported? do
        if single_query_cursor_pagination_possible?(ecto_query, assoc_list, order_by_list) do
          {entries, has_more?} =
            ecto_query
            |> repo.all()
            |> normalize_paginated_rows(page_size, cursor_direction)

          first_entry = List.first(entries)
          last_entry = List.last(entries)

          {entries, cursor_map_from_entry(first_entry, order_by_list),
           cursor_map_from_entry(last_entry, order_by_list), has_more?}
        else
          cursor_select_map = build_cursor_select_map(ecto_query, assoc_list, order_by_list)

          page_keys_query =
            ecto_query
            |> Query.exclude([:preload, :select])
            |> Ecto.Query.select([{^root_schema, x}], ^cursor_select_map)
            |> Ecto.Query.distinct(true)

          page_key_rows = repo.all(page_keys_query)

          {page_key_rows, has_more?} =
            normalize_paginated_rows(page_key_rows, page_size, cursor_direction)

          keys = Enum.map(page_key_rows, &primary_key_value_from_row(&1, primary_key_fields))

          if length(keys) != length(Enum.uniq(keys)) do
            raise ArgumentError,
                  "paginate/3 could not produce a page of unique root rows; " <>
                    "this usually means your order_by depends on a to-many join (e.g. ordering by a has_many field). " <>
                    "Use an aggregation (e.g. max/min) or order by root/to-one fields only. " <>
                    "order_by: #{inspect(order_by_list)}"
          end

          entries = load_entries_for_page(repo, ecto_query, root_schema, primary_key_fields, keys)

          first_row = List.first(page_key_rows)
          last_row = List.last(page_key_rows)

          {entries, first_row, last_row, has_more?}
        end
      else
        if ecto_query.preloads != [] do
          has_more_query =
            ecto_query
            |> Query.exclude([:preload, :select])
            |> Ecto.Query.select([{^root_schema, _x}], 1)

          has_more? = length(repo.all(has_more_query)) == page_size + 1

          entries_query =
            ecto_query
            |> Query.exclude([:limit])
            |> Ecto.Query.limit(^page_size)

          entries = repo.all(entries_query)

          entries = reverse_if_before(entries, cursor_direction)

          {entries, nil, nil, has_more?}
        else
          {entries, has_more?} =
            ecto_query
            |> repo.all()
            |> normalize_paginated_rows(page_size, cursor_direction)

          {entries, nil, nil, has_more?}
        end
      end

    build_cursor = fn
      nil -> nil
      cursor_map when is_map(cursor_map) -> encode_cursor(cursor_map)
    end

    %{
      pagination: %{
        cursor_direction: cursor_direction,
        cursor_for_entries_before: build_cursor.(first_row_cursor_map),
        cursor_for_entries_after: build_cursor.(last_row_cursor_map),
        has_more_entries: has_more?,
        max_page_size: page_size
      },
      paginated_entries: entries
    }
  end

  defp build_keyset_or_filters(repo, order_by_list, cursor) do
    adapter = repo.__adapter__()

    order_specs =
      Enum.map(order_by_list, fn {direction, field} ->
        {dir, nulls} = normalize_cursor_order_direction(adapter, direction, field)

        %{
          field: field,
          dir: dir,
          nulls: nulls,
          cursor_value: Map.fetch!(cursor, to_string(field))
        }
      end)

    {_, filters} =
      Enum.reduce(order_specs, {[], []}, fn %{
                                              field: field,
                                              dir: dir,
                                              nulls: nulls,
                                              cursor_value: value
                                            },
                                            {prev_fields, filters} ->
        filters = filters ++ keyset_groups_for_field(prev_fields, field, dir, nulls, value)
        {prev_fields ++ [{field, value}], filters}
      end)

    filters
  end

  defp ensure_paginate_select_is_root!(ecto_query) do
    case ecto_query.select do
      nil ->
        :ok

      %Ecto.Query.SelectExpr{expr: {:&, _, [0]}} ->
        :ok

      %Ecto.Query.SelectExpr{} = select ->
        raise ArgumentError,
              "paginate/3 does not support custom select expressions; " <>
                "expected selecting the root schema struct (e.g. `select: u` or no select), got: #{inspect(select.expr)}"
    end
  end

  defp single_query_cursor_pagination_possible?(ecto_query, assoc_list, order_by_list) do
    cursor_fields_extractable_from_entries?(ecto_query, assoc_list, order_by_list) and
      only_to_one_assoc_joins?(ecto_query) and not has_to_many_preloads?(assoc_list)
  end

  defp cursor_fields_extractable_from_entries?(ecto_query, assoc_list, order_by_list) do
    source_schema = QueryBuilder.Utils.root_schema(ecto_query)

    Enum.all?(order_by_list, fn {_direction, token} ->
      token_str = to_string(token)

      case String.split(token_str, "@", parts: 3) do
        [_field] ->
          true

        [_field, assoc_field] ->
          assoc_field =
            try do
              String.to_existing_atom(assoc_field)
            rescue
              ArgumentError -> nil
            end

          not is_nil(assoc_field) and
            not is_nil(source_schema.__schema__(:association, assoc_field)) and
            preloaded_to_one_root_assoc?(assoc_list, assoc_field)

        _ ->
          false
      end
    end)
  end

  defp preloaded_to_one_root_assoc?([], _assoc_field), do: false

  defp preloaded_to_one_root_assoc?([assoc_data | rest], assoc_field) do
    if assoc_data.assoc_field == assoc_field do
      assoc_data.preload_spec != nil and assoc_data.cardinality == :one
    else
      preloaded_to_one_root_assoc?(rest, assoc_field)
    end
  end

  defp only_to_one_assoc_joins?(%Ecto.Query{} = ecto_query) do
    root_schema = QueryBuilder.Utils.root_schema(ecto_query)

    Enum.reduce_while(ecto_query.joins, [root_schema], fn join, schemas ->
      case join do
        %Ecto.Query.JoinExpr{assoc: {parent_index, assoc_field}}
        when is_integer(parent_index) and is_atom(assoc_field) ->
          with {:ok, parent_schema} <- Enum.fetch(schemas, parent_index),
               %{cardinality: :one, queryable: assoc_schema} <-
                 parent_schema.__schema__(:association, assoc_field),
               true <- is_atom(assoc_schema) do
            {:cont, schemas ++ [assoc_schema]}
          else
            _ -> {:halt, :unsafe}
          end

        _ ->
          {:halt, :unsafe}
      end
    end) != :unsafe
  end

  defp has_to_many_preloads?([]), do: false

  defp has_to_many_preloads?([assoc_data | rest]) do
    (assoc_data.preload_spec != nil and assoc_data.cardinality == :many) ||
      has_to_many_preloads?(assoc_data.nested_assocs) ||
      has_to_many_preloads?(rest)
  end

  defp cursor_map_from_entry(nil, _order_by_list), do: nil

  defp cursor_map_from_entry(entry, order_by_list) do
    Enum.reduce(order_by_list, %{}, fn {_direction, token}, acc ->
      token_str = to_string(token)

      value =
        case String.split(token_str, "@", parts: 3) do
          [field] ->
            field = String.to_existing_atom(field)
            Map.fetch!(entry, field)

          [field, assoc_field] ->
            field = String.to_existing_atom(field)
            assoc_field = String.to_existing_atom(assoc_field)
            assoc = Map.fetch!(entry, assoc_field)

            cond do
              match?(%Ecto.Association.NotLoaded{}, assoc) ->
                raise ArgumentError,
                      "paginate/3 internal error: expected association #{inspect(assoc_field)} to be preloaded " <>
                        "in order to build cursor field #{inspect(token_str)} from the returned structs"

              is_nil(assoc) ->
                nil

              is_map(assoc) ->
                Map.fetch!(assoc, field)

              true ->
                raise ArgumentError,
                      "paginate/3 internal error: expected association #{inspect(assoc_field)} to be a struct or nil, got: #{inspect(assoc)}"
            end

          _ ->
            raise ArgumentError,
                  "paginate/3 internal error: unexpected cursor token #{inspect(token_str)}"
        end

      Map.put(acc, token_str, value)
    end)
  end

  defp primary_key_value_from_row(row, [pk_field]) do
    Map.fetch!(row, Atom.to_string(pk_field))
  end

  defp primary_key_value_from_row(row, pk_fields) when is_list(pk_fields) do
    pk_fields
    |> Enum.map(&Map.fetch!(row, Atom.to_string(&1)))
    |> List.to_tuple()
  end

  defp load_entries_for_page(_repo, _ecto_query, _source_schema, _pk_fields, []), do: []

  defp load_entries_for_page(repo, ecto_query, source_schema, [pk_field], keys)
       when is_list(keys) do
    entries_query =
      ecto_query
      |> Query.exclude([:limit, :offset, :order_by])
      |> Ecto.Query.where([{^source_schema, x}], field(x, ^pk_field) in ^keys)

    entries = repo.all(entries_query)

    entries_by_key =
      Enum.reduce(entries, %{}, fn entry, acc ->
        Map.put_new(acc, Map.fetch!(entry, pk_field), entry)
      end)

    Enum.map(keys, fn key ->
      case Map.fetch(entries_by_key, key) do
        {:ok, entry} ->
          entry

        :error ->
          raise ArgumentError,
                "paginate/3 internal error: expected to load an entry with primary key #{inspect(key)}, " <>
                  "but it was missing from the results"
      end
    end)
  end

  defp load_entries_for_page(repo, ecto_query, source_schema, pk_fields, keys)
       when is_list(pk_fields) and length(pk_fields) > 1 and is_list(keys) do
    dynamic_keys =
      Enum.map(keys, fn key ->
        key_parts =
          key
          |> Tuple.to_list()
          |> then(&Enum.zip(pk_fields, &1))

        key_parts
        |> Enum.map(fn {field, value} ->
          Ecto.Query.dynamic([{^source_schema, x}], field(x, ^field) == ^value)
        end)
        |> Enum.reduce(&Ecto.Query.dynamic(^&1 and ^&2))
      end)

    [first | rest] = dynamic_keys
    where_dynamic = Enum.reduce(rest, first, &Ecto.Query.dynamic(^&1 or ^&2))

    entries_query =
      ecto_query
      |> Query.exclude([:limit, :offset, :order_by])
      |> Ecto.Query.where(^where_dynamic)

    entries = repo.all(entries_query)

    entries_by_key =
      Enum.reduce(entries, %{}, fn entry, acc ->
        key =
          pk_fields
          |> Enum.map(&Map.fetch!(entry, &1))
          |> List.to_tuple()

        Map.put_new(acc, key, entry)
      end)

    Enum.map(keys, fn key ->
      case Map.fetch(entries_by_key, key) do
        {:ok, entry} ->
          entry

        :error ->
          raise ArgumentError,
                "paginate/3 internal error: expected to load an entry with primary key #{inspect(key)}, " <>
                  "but it was missing from the results"
      end
    end)
  end

  defp reverse_if_before(rows, :before), do: Enum.reverse(rows)
  defp reverse_if_before(rows, :after), do: rows

  defp normalize_paginated_rows(rows, page_size, cursor_direction) do
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

  defp keyset_groups_for_field(prev_fields, field, _dir, nulls, nil) do
    # If the cursor value is NULL, we can’t emit `field < NULL` / `field > NULL`.
    # Instead, we:
    #   - optionally include a branch for the non-NULL group (when NULLs sort first)
    #   - then rely on subsequent order_by fields for tie-breaking inside the NULL group
    case nulls do
      :first ->
        [prev_fields ++ [{field, :ne, nil}]]

      :last ->
        []
    end
  end

  defp keyset_groups_for_field(prev_fields, field, dir, nulls, value) do
    operator =
      case dir do
        :asc -> :gt
        :desc -> :lt
      end

    groups = [prev_fields ++ [{field, operator, value}]]

    # When NULLs sort last, NULL is after any non-NULL cursor value, so include it.
    case nulls do
      :last -> groups ++ [prev_fields ++ [{field, nil}]]
      :first -> groups
    end
  end

  defp build_cursor_select_map(ecto_query, assoc_list, order_by_list) do
    cursor_field_tokens = Enum.map(order_by_list, &elem(&1, 1))

    Enum.reduce(cursor_field_tokens, %{}, fn token, acc ->
      {field, binding} =
        QueryBuilder.Utils.find_field_and_binding_from_token(ecto_query, assoc_list, token)

      value_expr = Ecto.Query.dynamic([{^binding, x}], field(x, ^field))

      Map.put(acc, to_string(token), value_expr)
    end)
  end

  defp encode_cursor(cursor_map) when is_map(cursor_map) do
    cursor_map
    |> Jason.encode!()
    |> Base.url_encode64()
  end

  defp decode_cursor!(nil), do: %{}

  defp decode_cursor!(cursor) when is_binary(cursor) do
    if cursor == "" do
      raise ArgumentError,
            "paginate/3 cursor cannot be an empty string; omit `cursor:` (or pass `nil`) for the first page"
    end

    decoded_string =
      case Base.url_decode64(cursor) do
        {:ok, decoded} ->
          decoded

        :error ->
          case Base.url_decode64(cursor, padding: false) do
            {:ok, decoded} ->
              decoded

            :error ->
              raise ArgumentError,
                    "paginate/3 invalid cursor; expected base64url-encoded JSON, got: #{inspect(cursor)}"
          end
      end

    decoded_cursor =
      case Jason.decode(decoded_string) do
        {:ok, decoded} ->
          decoded

        {:error, error} ->
          raise ArgumentError,
                "paginate/3 invalid cursor; expected base64url-encoded JSON, got JSON decode error: #{Exception.message(error)}"
      end

    unless is_map(decoded_cursor) do
      raise ArgumentError,
            "paginate/3 invalid cursor; expected a JSON object (map), got: #{inspect(decoded_cursor)}"
    end

    decoded_cursor = normalize_cursor_map!(decoded_cursor)

    if decoded_cursor == %{} do
      raise ArgumentError,
            "paginate/3 invalid cursor; decoded cursor map was empty; omit `cursor:` (or pass `nil`) for the first page"
    end

    decoded_cursor
  end

  defp decode_cursor!(cursor) when is_map(cursor) do
    cursor = normalize_cursor_map!(cursor)

    if cursor == %{} do
      raise ArgumentError,
            "paginate/3 cursor map cannot be empty; omit `cursor:` (or pass `nil`) for the first page"
    end

    cursor
  end

  defp decode_cursor!(cursor) do
    raise ArgumentError,
          "paginate/3 cursor must be a map or a base64url-encoded JSON map (string), got: #{inspect(cursor)}"
  end

  defp normalize_cursor_map!(cursor) when is_map(cursor) do
    normalized_pairs =
      Enum.map(cursor, fn {key, value} ->
        {normalize_cursor_key!(key), value}
      end)

    normalized_keys = Enum.map(normalized_pairs, &elem(&1, 0))

    if length(normalized_keys) != length(Enum.uniq(normalized_keys)) do
      raise ArgumentError,
            "paginate/3 cursor map has duplicate keys after normalization: #{inspect(normalized_keys)}"
    end

    Map.new(normalized_pairs)
  end

  defp normalize_cursor_key!(key) when is_binary(key) do
    if key == "" do
      raise ArgumentError, "paginate/3 cursor map has an empty key"
    end

    key
  end

  defp normalize_cursor_key!(key) when is_atom(key) do
    Atom.to_string(key)
  end

  defp normalize_cursor_key!(key) do
    raise ArgumentError,
          "paginate/3 cursor map keys must be strings or atoms; got: #{inspect(key)}"
  end

  defp validate_cursor_matches_order_by!(cursor, order_by_list) when is_map(cursor) do
    expected_keys =
      order_by_list
      |> Enum.map(fn {_direction, field} -> to_string(field) end)
      |> Enum.uniq()

    cursor_keys = Map.keys(cursor)

    expected_set = MapSet.new(expected_keys)
    cursor_set = MapSet.new(cursor_keys)

    missing =
      expected_set
      |> MapSet.difference(cursor_set)
      |> MapSet.to_list()
      |> Enum.sort()

    extra =
      cursor_set
      |> MapSet.difference(expected_set)
      |> MapSet.to_list()
      |> Enum.sort()

    if missing != [] or extra != [] do
      raise ArgumentError,
            "paginate/3 cursor does not match the query's order_by fields; " <>
              "expected keys: #{inspect(expected_keys)}, " <>
              "missing: #{inspect(missing)}, extra: #{inspect(extra)}. " <>
              "This cursor was likely generated for a different query or the query's order_by changed."
    end
  end

  defp cursorable_order_by_field?(field) when is_atom(field) or is_binary(field), do: true
  defp cursorable_order_by_field?(_field), do: false

  defp supported_cursor_order_direction?(direction)
       when direction in [
              :asc,
              :desc,
              :asc_nulls_first,
              :asc_nulls_last,
              :desc_nulls_first,
              :desc_nulls_last
            ],
       do: true

  defp supported_cursor_order_direction?(_direction), do: false

  defp normalize_cursor_order_direction(adapter, direction, field) do
    case direction do
      :asc -> {:asc, adapter_default_nulls_position!(adapter, :asc, field)}
      :desc -> {:desc, adapter_default_nulls_position!(adapter, :desc, field)}
      :asc_nulls_first -> {:asc, :first}
      :asc_nulls_last -> {:asc, :last}
      :desc_nulls_first -> {:desc, :first}
      :desc_nulls_last -> {:desc, :last}
    end
  end

  defp adapter_default_nulls_position!(adapter, dir, field) when dir in [:asc, :desc] do
    case {adapter, dir} do
      {Ecto.Adapters.Postgres, :asc} ->
        :last

      {Ecto.Adapters.Postgres, :desc} ->
        :first

      {Ecto.Adapters.MyXQL, :asc} ->
        :first

      {Ecto.Adapters.MyXQL, :desc} ->
        :last

      {Ecto.Adapters.SQLite3, :asc} ->
        :first

      {Ecto.Adapters.SQLite3, :desc} ->
        :last

      {other, _} ->
        raise ArgumentError,
              "paginate/3 cannot infer the default NULL ordering for adapter #{inspect(other)} " <>
                "when using #{inspect(dir)} for #{inspect(field)}; " <>
                "supported adapters: Ecto.Adapters.Postgres, Ecto.Adapters.MyXQL, Ecto.Adapters.SQLite3. " <>
                "Use explicit *_nulls_* order directions if supported by your adapter."
    end
  end

  defp reverse_order_direction(direction, field) do
    case direction do
      :asc ->
        :desc

      :desc ->
        :asc

      :asc_nulls_first ->
        :desc_nulls_last

      :asc_nulls_last ->
        :desc_nulls_first

      :desc_nulls_first ->
        :asc_nulls_last

      :desc_nulls_last ->
        :asc_nulls_first

      other ->
        raise ArgumentError,
              "paginate/3 can't reverse order direction #{inspect(other)} for field #{inspect(field)} " <>
                "(supported: :asc, :desc, :asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last)"
    end
  end

  # NOTE: Cursor token parsing/resolution is intentionally delegated to the same
  # token system used by where/order_by (`QueryBuilder.Utils.find_field_and_binding_from_token/3`),
  # so we don't couple pagination correctness to binding naming conventions.

  def default_page_size() do
    Application.get_env(:query_builder, :default_page_size, 100)
  end

  @doc ~S"""
  Preloads associations using *separate* queries (Ecto's default preload behavior).

  This always performs query-preload, even if the association is joined in SQL.

  Example:
  ```
  QueryBuilder.preload_separate(query, [role: :permissions, articles: [:stars, comments: :user]])
  ```
  """
  def preload_separate(%QueryBuilder.Query{} = query, assoc_fields) do
    %{
      query
      | operations: [
          %{
            type: :preload,
            assocs: assoc_fields,
            preload_spec: QueryBuilder.AssocList.PreloadSpec.new(:separate),
            args: []
          }
          | query.operations
        ]
    }
  end

  def preload_separate(ecto_query, assoc_fields) do
    ecto_query = ensure_query_has_binding(ecto_query)
    preload_separate(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields)
  end

  @doc ~S"""
  Preloads a (direct) association using a separate query with an explicit scope.

  This is the QueryBuilder equivalent of Ecto’s query-based separate preload:

  ```elixir
  User
  |> preload([u],
    authored_articles:
      ^from(a in assoc(u, :authored_articles),
        where: a.published == true,
        order_by: [desc: a.inserted_at]
      )
  )
  ```

  Supported options:
  - `where:` filters (QueryBuilder `where/2` filter shape)
  - `order_by:` keyword list (QueryBuilder `order_by/2` shape)

  Restrictions (fail-fast):
  - Only supports a single, direct association (no nested paths).
  - Filters/order_by must reference fields on the association schema (no `@assoc` tokens).
  - Does not accept custom filter/order_by functions. Use an Ecto preload query for advanced cases.
  - Cannot be combined with nested preloads under the same association; use an explicit Ecto query-based preload query instead.
  """
  def preload_separate_scoped(query, assoc_field, opts \\ [])

  def preload_separate_scoped(_query, nil, _opts) do
    raise ArgumentError, "preload_separate_scoped/3 expects an association field, got nil"
  end

  def preload_separate_scoped(_query, _assoc_field, nil) do
    raise ArgumentError, "preload_separate_scoped/3 expects opts to be a keyword list, got nil"
  end

  def preload_separate_scoped(%QueryBuilder.Query{} = query, assoc_field, opts)
      when is_atom(assoc_field) do
    opts = normalize_preload_separate_scoped_opts!(opts, assoc_field)

    %{
      query
      | operations: [
          %{
            type: :preload,
            assocs: assoc_field,
            preload_spec: QueryBuilder.AssocList.PreloadSpec.new(:separate, opts),
            args: []
          }
          | query.operations
        ]
    }
  end

  def preload_separate_scoped(query, assoc_field, opts) when is_atom(assoc_field) do
    ecto_query = ensure_query_has_binding(query)
    preload_separate_scoped(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_field, opts)
  end

  def preload_separate_scoped(_query, assoc_field, _opts) do
    raise ArgumentError,
          "preload_separate_scoped/3 expects `assoc_field` to be an atom (direct association), got: #{inspect(assoc_field)}"
  end

  @doc ~S"""
  Preloads associations *through join bindings* (join-preload).

  This requires the association to already be joined (for example because you filtered
  through it, ordered by it, or explicitly joined it with `left_join/2`). If the
  association isn't joined, this raises `ArgumentError`.

  Example:
  ```
  User
  |> QueryBuilder.left_join(:role)
  |> QueryBuilder.preload_through_join(:role)
  ```
  """
  def preload_through_join(%QueryBuilder.Query{} = query, assoc_fields) do
    %{
      query
      | operations: [
          %{
            type: :preload,
            assocs: assoc_fields,
            preload_spec: QueryBuilder.AssocList.PreloadSpec.new(:through_join),
            args: []
          }
          | query.operations
        ]
    }
  end

  def preload_through_join(ecto_query, assoc_fields) do
    ecto_query = ensure_query_has_binding(ecto_query)
    preload_through_join(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields)
  end

  @doc ~S"""
  An AND where query expression.

  Example:
  ```
  QueryBuilder.where(query, firstname: "John")
  ```
  """
  def where(_query, nil) do
    raise ArgumentError,
          "where/2 expects `filters` to be a keyword list (or a list of filters); got nil"
  end

  def where(query, filters) do
    where(query, [], filters)
  end

  @doc ~S"""
  An AND where query expression.

  Associations are passed in second argument; fields from these associations can then
  be referenced by writing the field name, followed by the "@" character and the
  association name, as an atom. For example: `:name@users`.

  Example:
  ```
  QueryBuilder.where(query, [role: :permissions], name@permissions: :write)
  ```

  OR clauses may be passed through last argument `opts`. For example:

  ```elixir
  QueryBuilder.where(query, [], [firstname: "John"], or: [firstname: "Alice", lastname: "Doe"], or: [firstname: "Bob"])
  ```
  """
  def where(query, assoc_fields, filters, or_filters \\ [])

  def where(_query, _assoc_fields, nil, _or_filters) do
    raise ArgumentError,
          "where/4 expects `filters` to be a keyword list (or a list of filters); got nil"
  end

  def where(_query, _assoc_fields, _filters, nil) do
    raise ArgumentError,
          "where/4 expects `or_filters` to be a keyword list like `[or: [...], or: [...]]`; got nil"
  end

  def where(%QueryBuilder.Query{} = query, _assoc_fields, [], []) do
    query
  end

  def where(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    %{
      query
      | operations: [
          %{type: :where, assocs: assoc_fields, args: [filters, or_filters]} | query.operations
        ]
    }
  end

  def where(ecto_query, assoc_fields, filters, or_filters) do
    ecto_query = ensure_query_has_binding(ecto_query)
    where(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, filters, or_filters)
  end

  @doc ~S"""
  An OR where query expression (an OR of AND groups).

  Examples:

  ```elixir
  QueryBuilder.where_any(query, [[firstname: "John"], [firstname: "Alice", lastname: "Doe"]])
  ```

  ```elixir
  QueryBuilder.where_any(query, :role, [[name@role: "admin"], [name@role: "author"]])
  ```
  """
  def where_any(query, or_groups) do
    where_any(query, [], or_groups)
  end

  def where_any(query, assoc_fields, or_groups)

  def where_any(%QueryBuilder.Query{} = query, assoc_fields, or_groups) do
    or_groups = normalize_or_groups!(or_groups, :where_any, "where_any/2 and where_any/3")

    case Enum.reject(or_groups, &(&1 == [])) do
      [] ->
        query

      [first | rest] ->
        or_filters = Enum.map(rest, &{:or, &1})
        where(query, assoc_fields, first, or_filters)
    end
  end

  def where_any(ecto_query, assoc_fields, or_groups) do
    ecto_query = ensure_query_has_binding(ecto_query)
    where_any(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, or_groups)
  end

  @doc ~S"""
  A select query expression.

  Selection supports:
  - a single field token (`:name` or `:name@role`) → selects a single value
  - a tuple of tokens/values → selects a tuple
  - a list of field tokens → selects a map keyed by the tokens
  - a map of output keys to field tokens → selects a map with your keys
  - a custom 1-arity function escape hatch (receives a token resolver)

  Examples:

  ```elixir
  User |> QueryBuilder.select(:name) |> Repo.all()
  ```

  ```elixir
  User |> QueryBuilder.select([:id, :name]) |> Repo.all()
  # => [%{id: 100, name: "Alice"}, ...]
  ```

  ```elixir
  User |> QueryBuilder.select(:role, %{role_name: :name@role}) |> Repo.all()
  ```

  ```elixir
  User |> QueryBuilder.select({:id, :name}) |> Repo.all()
  # => [{100, "Alice"}, ...]
  ```

  Like Ecto, only one `select` expression is allowed. Calling `select/*` more
  than once (or calling `select/*` after `select_merge/*`) raises. Use
  `select_merge/*` to accumulate fields into the selection.

  Note: `paginate/3` requires selecting the root struct; using `select/*` will make
  pagination raise (fail-fast).
  """
  def select(query, selection) do
    select(query, [], selection)
  end

  def select(query, assoc_fields, selection)

  def select(%QueryBuilder.Query{} = query, assoc_fields, selection) do
    if Enum.any?(query.operations, fn %{type: type} -> type in [:select, :select_merge] end) do
      raise ArgumentError,
            "only one select expression is allowed in query; " <>
              "call `select/*` at most once and use `select_merge/*` to add fields"
    end

    %{
      query
      | operations: [%{type: :select, assocs: assoc_fields, args: [selection]} | query.operations]
    }
  end

  def select(ecto_query, assoc_fields, selection) do
    ecto_query = ensure_query_has_binding(ecto_query)
    select(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, selection)
  end

  @doc ~S"""
  A `select_merge` query expression.

  This merges a map into the existing selection (Ecto `select_merge` semantics).

  Notes:
  - If there is no prior `select`, Ecto merges into the root struct by default.
  - `select_merge` requires explicit keys for `field@assoc` values (use a map).
  - `paginate/3` requires selecting the root struct; any custom select expression
    (including `select_merge`) will make pagination raise (fail-fast).

  Examples:

  ```elixir
  User
  |> QueryBuilder.select_merge(%{name: :name})
  |> Repo.all()
  ```

  ```elixir
  User
  |> QueryBuilder.select_merge(:role, %{role_name: :name@role})
  |> Repo.all()
  ```
  """
  def select_merge(query, selection) do
    select_merge(query, [], selection)
  end

  def select_merge(query, assoc_fields, selection)

  def select_merge(%QueryBuilder.Query{} = query, assoc_fields, selection) do
    %{
      query
      | operations: [
          %{type: :select_merge, assocs: assoc_fields, args: [selection]} | query.operations
        ]
    }
  end

  def select_merge(ecto_query, assoc_fields, selection) do
    ecto_query = ensure_query_has_binding(ecto_query)
    select_merge(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, selection)
  end

  @doc ~S"""
  Aggregate helpers for grouped queries.

  These return aggregate expressions that can be used in `select/*`, `order_by/*`, and `having/*`.

  Examples:
  ```
  QueryBuilder.count(:id)
  QueryBuilder.count(:id, :distinct)
  QueryBuilder.sum(:amount)
  ```
  """
  def count(), do: %QueryBuilder.Aggregate{op: :count, arg: nil, modifier: nil}

  def count(token) when is_atom(token) or is_binary(token),
    do: %QueryBuilder.Aggregate{op: :count, arg: token, modifier: nil}

  def count(token, :distinct) when is_atom(token) or is_binary(token),
    do: %QueryBuilder.Aggregate{op: :count, arg: token, modifier: :distinct}

  def count_distinct(token), do: count(token, :distinct)

  def avg(token) when is_atom(token) or is_binary(token),
    do: %QueryBuilder.Aggregate{op: :avg, arg: token, modifier: nil}

  def sum(token) when is_atom(token) or is_binary(token),
    do: %QueryBuilder.Aggregate{op: :sum, arg: token, modifier: nil}

  def min(token) when is_atom(token) or is_binary(token),
    do: %QueryBuilder.Aggregate{op: :min, arg: token, modifier: nil}

  def max(token) when is_atom(token) or is_binary(token),
    do: %QueryBuilder.Aggregate{op: :max, arg: token, modifier: nil}

  @doc ~S"""
  A correlated `EXISTS(...)` subquery filter.

  This is the explicit alternative to `where/4` when filtering through to-many
  associations would otherwise duplicate root rows (SQL join multiplication).

  Example:
  ```
  User
  |> QueryBuilder.where_exists_subquery(
    [authored_articles: :comments],
    where: [title@comments: "It's great!"],
    scope: []
  )
  |> Repo.all()
  ```

  `scope:` is **required** to make the “new query block” boundary explicit. It is
  applied inside the `EXISTS(...)` subquery (and is not inferred from outer joins).
  Pass `scope: []` to explicitly declare “no extra scoping”.

  `where:` adds AND filters inside the subquery. To express OR groups, use
  `where_any: [[...], ...]`.
  """
  def where_exists_subquery(query, assoc_fields, opts \\ [])

  def where_exists_subquery(%QueryBuilder.Query{} = query, assoc_fields, opts) do
    if assoc_fields in [nil, []] do
      raise ArgumentError,
            "where_exists_subquery/3 requires a non-empty assoc_fields argument " <>
              "(e.g. `where_exists_subquery(:comments, where: [..], scope: ..)` or `where_exists_subquery([articles: :comments], where: [..], scope: ..)`)"
    end

    {where_any, opts} = Keyword.pop(opts, :where_any, :__missing__)
    {where_filters, opts} = Keyword.pop(opts, :where, [])

    {scope, opts} =
      case Keyword.pop(opts, :scope, :__missing__) do
        {:__missing__, _} ->
          raise ArgumentError,
                "where_exists_subquery/3 requires an explicit `scope:` option; " <>
                  "pass `scope: []` to explicitly declare no extra scoping"

        {scope, opts} when is_list(scope) ->
          {scope, opts}

        {other, _} ->
          raise ArgumentError,
                "where_exists_subquery/3 expects `scope:` to be a list of filters, got: #{inspect(other)}"
      end

    if is_nil(where_filters) or not is_list(where_filters) do
      raise ArgumentError,
            "where_exists_subquery/3 expects `where:` to be a list of filters; got: #{inspect(where_filters)}"
    end

    if Keyword.has_key?(opts, :or) do
      raise ArgumentError,
            "where_exists_subquery/3 does not support `or:`; use `where_any: [[...], ...]`"
    end

    where_any_groups =
      case where_any do
        :__missing__ -> []
        other -> normalize_or_groups!(other, :where_any, "where_exists_subquery/3")
      end

    where_any_groups = Enum.reject(where_any_groups, &(&1 == []))

    {predicate_filters, predicate_or_filters} =
      case where_any_groups do
        [] -> {[], []}
        [first | rest] -> {first, Enum.map(rest, &{:or, &1})}
      end

    effective_scope_filters = scope ++ where_filters

    unknown_opt_keys =
      opts
      |> Keyword.keys()
      |> Enum.uniq()

    if unknown_opt_keys != [] do
      raise ArgumentError,
            "unknown options for where_exists_subquery/3: #{inspect(unknown_opt_keys)} " <>
              "(supported: :where, :scope, :where_any)"
    end

    %{
      query
      | operations: [
          %{
            type: :where_exists_subquery,
            assocs: [],
            args: [
              assoc_fields,
              effective_scope_filters,
              predicate_filters,
              predicate_or_filters
            ]
          }
          | query.operations
        ]
    }
  end

  def where_exists_subquery(ecto_query, assoc_fields, opts) do
    ecto_query = ensure_query_has_binding(ecto_query)

    where_exists_subquery(
      %QueryBuilder.Query{ecto_query: ecto_query},
      assoc_fields,
      opts
    )
  end

  @doc ~S"""
  A correlated `NOT EXISTS(...)` subquery filter.

  Example:
  ```
  User
  |> QueryBuilder.where_not_exists_subquery(:authored_articles, where: [], scope: [])
  |> Repo.all()
  ```

  `where:` adds AND filters inside the subquery. To express OR groups, use
  `where_any: [[...], ...]`.
  """
  def where_not_exists_subquery(query, assoc_fields, opts \\ [])

  def where_not_exists_subquery(%QueryBuilder.Query{} = query, assoc_fields, opts) do
    if assoc_fields in [nil, []] do
      raise ArgumentError,
            "where_not_exists_subquery/3 requires a non-empty assoc_fields argument " <>
              "(e.g. `where_not_exists_subquery(:comments, where: [..], scope: ..)` or `where_not_exists_subquery([articles: :comments], where: [..], scope: ..)`)"
    end

    {where_any, opts} = Keyword.pop(opts, :where_any, :__missing__)
    {where_filters, opts} = Keyword.pop(opts, :where, [])

    {scope, opts} =
      case Keyword.pop(opts, :scope, :__missing__) do
        {:__missing__, _} ->
          raise ArgumentError,
                "where_not_exists_subquery/3 requires an explicit `scope:` option; " <>
                  "pass `scope: []` to explicitly declare no extra scoping"

        {scope, opts} when is_list(scope) ->
          {scope, opts}

        {other, _} ->
          raise ArgumentError,
                "where_not_exists_subquery/3 expects `scope:` to be a list of filters, got: #{inspect(other)}"
      end

    if is_nil(where_filters) or not is_list(where_filters) do
      raise ArgumentError,
            "where_not_exists_subquery/3 expects `where:` to be a list of filters; got: #{inspect(where_filters)}"
    end

    if Keyword.has_key?(opts, :or) do
      raise ArgumentError,
            "where_not_exists_subquery/3 does not support `or:`; use `where_any: [[...], ...]`"
    end

    where_any_groups =
      case where_any do
        :__missing__ -> []
        other -> normalize_or_groups!(other, :where_any, "where_not_exists_subquery/3")
      end

    where_any_groups = Enum.reject(where_any_groups, &(&1 == []))

    {predicate_filters, predicate_or_filters} =
      case where_any_groups do
        [] -> {[], []}
        [first | rest] -> {first, Enum.map(rest, &{:or, &1})}
      end

    effective_scope_filters = scope ++ where_filters

    unknown_opt_keys =
      opts
      |> Keyword.keys()
      |> Enum.uniq()

    if unknown_opt_keys != [] do
      raise ArgumentError,
            "unknown options for where_not_exists_subquery/3: #{inspect(unknown_opt_keys)} " <>
              "(supported: :where, :scope, :where_any)"
    end

    %{
      query
      | operations: [
          %{
            type: :where_not_exists_subquery,
            assocs: [],
            args: [
              assoc_fields,
              effective_scope_filters,
              predicate_filters,
              predicate_or_filters
            ]
          }
          | query.operations
        ]
    }
  end

  def where_not_exists_subquery(ecto_query, assoc_fields, opts) do
    ecto_query = ensure_query_has_binding(ecto_query)

    where_not_exists_subquery(
      %QueryBuilder.Query{ecto_query: ecto_query},
      assoc_fields,
      opts
    )
  end

  # Migration shim: v1 accepted where_exists_subquery/4; v2 uses where_exists_subquery/3 opts.
  def where_exists_subquery(_query, _assoc_fields, _filters, _opts) do
    raise ArgumentError,
          "where_exists_subquery/4 was replaced by where_exists_subquery/3; " <>
            "use `where_exists_subquery(assoc_fields, where: [...], where_any: [[...], ...], scope: [...])`"
  end

  # Migration shim: v1 accepted where_not_exists_subquery/4; v2 uses where_not_exists_subquery/3 opts.
  def where_not_exists_subquery(_query, _assoc_fields, _filters, _opts) do
    raise ArgumentError,
          "where_not_exists_subquery/4 was replaced by where_not_exists_subquery/3; " <>
            "use `where_not_exists_subquery(assoc_fields, where: [...], where_any: [[...], ...], scope: [...])`"
  end

  # Migration shim: v1 used where_exists/4; v2 renamed it to where_exists_subquery/3.
  def where_exists(_query, _assoc_fields, _filters, _or_filters \\ []) do
    raise ArgumentError,
          "where_exists/4 was renamed to where_exists_subquery/3; " <>
            "use `where_exists_subquery(assoc_fields, where: [...], scope: [...])`"
  end

  # Migration shim: v1 used where_not_exists/4; v2 renamed it to where_not_exists_subquery/3.
  def where_not_exists(_query, _assoc_fields, _filters, _or_filters \\ []) do
    raise ArgumentError,
          "where_not_exists/4 was renamed to where_not_exists_subquery/3; " <>
            "use `where_not_exists_subquery(assoc_fields, where: [...], scope: [...])`"
  end

  @doc ~S"""
  Run `QueryBuilder.where/2` only if given condition is met.
  """
  def maybe_where(query, true, filters) do
    where(query, [], filters)
  end

  def maybe_where(query, false, _), do: query

  def maybe_where(query, condition, assoc_fields, filters, or_filters \\ [])

  @doc ~S"""
  Run `QueryBuilder.where/4` only if given condition is met.
  """
  def maybe_where(query, true, assoc_fields, filters, or_filters) do
    where(query, assoc_fields, filters, or_filters)
  end

  def maybe_where(query, false, _, _, _), do: query

  @doc ~S"""
  A distinct query expression.

  When passed `true`/`false`, this sets `DISTINCT` for the current select expression.

  You can also pass order_by-like expressions (tokens/directions) to generate
  `DISTINCT ON (...)` on databases that support it (e.g. Postgres).
  """
  def distinct(_query, nil) do
    raise ArgumentError,
          "distinct/2 expects a boolean or an order_by-like expression (tokens, lists/keyword lists); got nil"
  end

  def distinct(query, value) do
    distinct(query, [], value)
  end

  def distinct(_query, _assoc_fields, nil) do
    raise ArgumentError,
          "distinct/3 expects a boolean or an order_by-like expression (tokens, lists/keyword lists); got nil"
  end

  def distinct(%QueryBuilder.Query{} = query, _assoc_fields, []) do
    query
  end

  def distinct(%QueryBuilder.Query{} = query, assoc_fields, value) do
    %{
      query
      | operations: [%{type: :distinct, assocs: assoc_fields, args: [value]} | query.operations]
    }
  end

  def distinct(ecto_query, assoc_fields, value) do
    ecto_query = ensure_query_has_binding(ecto_query)
    distinct(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, value)
  end

  @doc ~S"""
  A group by query expression.

  Example:
  ```
  QueryBuilder.group_by(query, :category)
  ```
  """
  def group_by(_query, nil) do
    raise ArgumentError,
          "group_by/2 expects a token, a list of tokens/expressions, a dynamic, or a 1-arity function; got nil"
  end

  def group_by(query, expr) do
    group_by(query, [], expr)
  end

  def group_by(_query, _assoc_fields, nil) do
    raise ArgumentError,
          "group_by/3 expects a token, a list of tokens/expressions, a dynamic, or a 1-arity function; got nil"
  end

  def group_by(%QueryBuilder.Query{} = query, _assoc_fields, []) do
    query
  end

  def group_by(%QueryBuilder.Query{} = query, assoc_fields, expr) do
    %{
      query
      | operations: [%{type: :group_by, assocs: assoc_fields, args: [expr]} | query.operations]
    }
  end

  def group_by(ecto_query, assoc_fields, expr) do
    ecto_query = ensure_query_has_binding(ecto_query)
    group_by(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, expr)
  end

  @doc ~S"""
  An AND having query expression.

  Like `where`, but applied after grouping.
  """
  def having(_query, nil) do
    raise ArgumentError,
          "having/2 expects `filters` to be a keyword list (or a list of filters); got nil"
  end

  def having(query, filters) do
    having(query, [], filters)
  end

  def having(query, assoc_fields, filters, or_filters \\ [])

  def having(_query, _assoc_fields, nil, _or_filters) do
    raise ArgumentError,
          "having/4 expects `filters` to be a keyword list (or a list of filters); got nil"
  end

  def having(_query, _assoc_fields, _filters, nil) do
    raise ArgumentError,
          "having/4 expects `or_filters` to be a keyword list like `[or: [...], or: [...]]`; got nil"
  end

  def having(%QueryBuilder.Query{} = query, _assoc_fields, [], []) do
    query
  end

  def having(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    %{
      query
      | operations: [
          %{type: :having, assocs: assoc_fields, args: [filters, or_filters]} | query.operations
        ]
    }
  end

  def having(ecto_query, assoc_fields, filters, or_filters) do
    ecto_query = ensure_query_has_binding(ecto_query)
    having(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, filters, or_filters)
  end

  @doc ~S"""
  An OR having query expression (an OR of AND groups).
  """
  def having_any(query, or_groups) do
    having_any(query, [], or_groups)
  end

  def having_any(query, assoc_fields, or_groups)

  def having_any(%QueryBuilder.Query{} = query, assoc_fields, or_groups) do
    or_groups = normalize_or_groups!(or_groups, :having_any, "having_any/2 and having_any/3")

    case Enum.reject(or_groups, &(&1 == [])) do
      [] ->
        query

      [first | rest] ->
        or_filters = Enum.map(rest, &{:or, &1})
        having(query, assoc_fields, first, or_filters)
    end
  end

  def having_any(ecto_query, assoc_fields, or_groups) do
    ecto_query = ensure_query_has_binding(ecto_query)
    having_any(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, or_groups)
  end

  @doc ~S"""
  An order by query expression.

  Example:
  ```
  QueryBuilder.order_by(query, asc: :lastname, asc: :firstname)
  ```
  """
  def order_by(_query, nil) do
    raise ArgumentError, "order_by/2 expects a keyword list; got nil"
  end

  def order_by(query, value) do
    order_by(query, [], value)
  end

  @doc ~S"""
  An order by query expression.

  For more about the second argument, see `where/3`.

  Example:
  ```
  QueryBuilder.order_by(query, :articles, asc: :title@articles)
  ```
  """
  def order_by(_query, _assoc_fields, nil) do
    raise ArgumentError, "order_by/3 expects a keyword list; got nil"
  end

  def order_by(%QueryBuilder.Query{} = query, _assoc_fields, []) do
    query
  end

  def order_by(%QueryBuilder.Query{} = query, assoc_fields, value) do
    %{
      query
      | operations: [%{type: :order_by, assocs: assoc_fields, args: [value]} | query.operations]
    }
  end

  def order_by(ecto_query, assoc_fields, value) do
    ecto_query = ensure_query_has_binding(ecto_query)
    order_by(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, value)
  end

  @doc ~S"""
  Run `QueryBuilder.order_by/2` only if given condition is met.
  """
  def maybe_order_by(query, true, value) do
    order_by(query, [], value)
  end

  def maybe_order_by(query, false, _), do: query

  @doc ~S"""
  Run `QueryBuilder.order_by/3` only if given condition is met.
  """
  def maybe_order_by(query, true, assoc_fields, value) do
    order_by(query, assoc_fields, value)
  end

  def maybe_order_by(query, false, _, _), do: query

  @doc ~S"""
  Wrap multiple arguments for use with `from_opts(..., mode: :full)`.

  `from_opts` passes each `{operation, value}` as a single argument to
  the operation (i.e. it calls `operation(query, value)`). Use `args/*` when you
  need to call an operation with multiple arguments (like `order_by/3`,
  `select/3`, `where/3`, or custom extension functions).

  Examples:
  ```elixir
  QueryBuilder.from_opts(User, [order_by: QueryBuilder.args(:role, asc: :name@role)], mode: :full)
  QueryBuilder.from_opts(User, [where: QueryBuilder.args(:role, [name@role: "admin"])], mode: :full)
  ```
  """
  def args(arg1, arg2), do: build_args!([arg1, arg2])
  def args(arg1, arg2, arg3), do: build_args!([arg1, arg2, arg3])
  def args(arg1, arg2, arg3, arg4), do: build_args!([arg1, arg2, arg3, arg4])

  def args(args) when is_list(args), do: build_args!(args)

  defp build_args!(args) do
    cond do
      args == [] ->
        raise ArgumentError, "args/1 expects at least 2 arguments; got an empty list"

      length(args) < 2 ->
        raise ArgumentError,
              "args/1 expects at least 2 arguments; " <>
                "for a single argument, pass it directly to from_opts/2 instead"

      Enum.any?(args, &is_nil/1) ->
        raise ArgumentError,
              "args/* does not accept nil arguments; omit the operation or pass [] instead"

      true ->
        %QueryBuilder.Args{args: args}
    end
  end

  @doc ~S"""
  A limit query expression.
  If multiple limit expressions are provided, the last expression is evaluated

  Example:
  ```
  QueryBuilder.limit(query, 10)
  ```
  """
  def limit(%QueryBuilder.Query{} = query, value) do
    # Limit order must be maintained, similar to Ecto:
    # - https://hexdocs.pm/ecto/Ecto.Query-macro-limit.html
    %{query | operations: [%{type: :limit, assocs: [], args: [value]} | query.operations]}
  end

  def limit(ecto_query, value) do
    ecto_query = ensure_query_has_binding(ecto_query)
    limit(%QueryBuilder.Query{ecto_query: ecto_query}, value)
  end

  @doc ~S"""
  A offset query expression.
  If multiple offset expressions are provided, the last expression is evaluated

  Example:
  ```
  QueryBuilder.offset(query, 10)
  ```
  """
  def offset(%QueryBuilder.Query{} = query, value) do
    # Offset order must be maintained, similar to Ecto:
    # - https://hexdocs.pm/ecto/Ecto.Query.html#offset/3
    %{query | operations: [%{type: :offset, assocs: [], args: [value]} | query.operations]}
  end

  def offset(ecto_query, value) do
    ecto_query = ensure_query_has_binding(ecto_query)
    offset(%QueryBuilder.Query{ecto_query: ecto_query}, value)
  end

  @doc ~S"""
  An inner join query expression.

  This emits `INNER JOIN`s for the given association path. It is “just join”: it does
  not apply filters.

  Example:
  ```
  QueryBuilder.inner_join(query, [authored_articles: :comments])
  ```
  """
  def inner_join(query, assoc_fields)

  def inner_join(_query, nil) do
    raise ArgumentError, "inner_join/2 expects assoc_fields, got nil"
  end

  def inner_join(%QueryBuilder.Query{} = query, assoc_fields) do
    %{
      query
      | operations: [%{type: :inner_join, assocs: assoc_fields, args: []} | query.operations]
    }
  end

  def inner_join(ecto_query, assoc_fields) do
    ecto_query = ensure_query_has_binding(ecto_query)
    inner_join(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields)
  end

  @doc ~S"""
  A join query expression.

  Example:
  ```
  QueryBuilder.left_join(query, :articles, title@articles: "Foo", or: [title@articles: "Bar"])
  ```

  Notes:
  - `left_join/4` only supports leaf associations (no nested assoc paths). For nested
    paths, use `left_join_leaf/4` or `left_join_path/4`.
  """
  def left_join(query, assoc_fields, filters \\ [], or_filters \\ [])

  def left_join(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    if assoc_fields_nested?(assoc_fields) do
      raise ArgumentError,
            "left_join/4 does not support nested association paths (it would be ambiguous whether intermediate hops " <>
              "should be inner-joined or left-joined). " <>
              "Use `left_join_leaf/4` for “INNER path + LEFT leaf”, or `left_join_path/4` for “LEFT every hop”. " <>
              "Got: #{inspect(assoc_fields)}"
    end

    left_join_leaf(query, assoc_fields, filters, or_filters)
  end

  def left_join(ecto_query, assoc_fields, filters, or_filters) do
    ecto_query = ensure_query_has_binding(ecto_query)
    left_join(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, filters, or_filters)
  end

  @doc ~S"""
  Left-joins the *leaf association* and uses inner joins to traverse intermediate
  associations in a nested path.

  This is the explicit version of the historical nested `left_join/4` behavior.

  Example (INNER authored_articles, LEFT comments):
  ```elixir
  User
  |> QueryBuilder.left_join_leaf([authored_articles: :comments])
  |> Repo.all()
  ```
  """
  def left_join_leaf(query, assoc_fields, filters \\ [], or_filters \\ [])

  def left_join_leaf(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    if is_nil(filters) do
      raise ArgumentError, "left_join_leaf/4 expects `filters` to be a list/keyword list, got nil"
    end

    if is_nil(or_filters) do
      raise ArgumentError, "left_join_leaf/4 expects `or_filters` to be a keyword list, got nil"
    end

    filters = List.wrap(filters)
    or_filters = List.wrap(or_filters)

    join_filters =
      if filters == [] and or_filters == [] do
        []
      else
        [filters, or_filters]
      end

    %{
      query
      | operations: [
          %{
            type: :left_join,
            assocs: assoc_fields,
            left_join_mode: :leaf,
            join_filters: join_filters
          }
          | query.operations
        ]
    }
  end

  def left_join_leaf(ecto_query, assoc_fields, filters, or_filters) do
    ecto_query = ensure_query_has_binding(ecto_query)
    left_join_leaf(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, filters, or_filters)
  end

  @doc ~S"""
  Left-joins *every hop* in a nested association path (a full left-joined chain).

  Example (LEFT authored_articles, LEFT comments):
  ```elixir
  User
  |> QueryBuilder.left_join_path([authored_articles: :comments])
  |> Repo.all()
  ```
  """
  def left_join_path(query, assoc_fields, filters \\ [], or_filters \\ [])

  def left_join_path(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    if is_nil(filters) do
      raise ArgumentError, "left_join_path/4 expects `filters` to be a list/keyword list, got nil"
    end

    if is_nil(or_filters) do
      raise ArgumentError, "left_join_path/4 expects `or_filters` to be a keyword list, got nil"
    end

    filters = List.wrap(filters)
    or_filters = List.wrap(or_filters)

    join_filters =
      if filters == [] and or_filters == [] do
        []
      else
        [filters, or_filters]
      end

    %{
      query
      | operations: [
          %{
            type: :left_join,
            assocs: assoc_fields,
            left_join_mode: :path,
            join_filters: join_filters
          }
          | query.operations
        ]
    }
  end

  def left_join_path(ecto_query, assoc_fields, filters, or_filters) do
    ecto_query = ensure_query_has_binding(ecto_query)
    left_join_path(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, filters, or_filters)
  end

  @doc ~S"""
  Applies a keyword list of operations to a query.

  By default, `from_opts/2` runs in **boundary mode**: it only allows a small
  join-independent subset of operations so callers don’t need to know whether the
  base query happens to join/preload anything.

  To opt into the full power of `from_opts`, use `mode: :full`.

  Examples:

  ```elixir
  # boundary (default)
  QueryBuilder.from_opts(query, [
    where: [name: "John"],
    order_by: [desc: :inserted_at],
    limit: 50
  ])

  # full (trusted internal usage)
  QueryBuilder.from_opts(query, [
    where: QueryBuilder.args(:role, [name@role: "admin"]),
    preload_separate: :role
  ], mode: :full)
  ```
  """
  @from_opts_supported_operations_boundary [
    :where,
    :where_any,
    :order_by,
    :limit,
    :offset
  ]

  @from_opts_supported_operations_full [
    :distinct,
    :group_by,
    :having,
    :having_any,
    :inner_join,
    :left_join,
    :left_join_leaf,
    :left_join_path,
    :limit,
    :maybe_order_by,
    :maybe_where,
    :offset,
    :order_by,
    :preload_separate,
    :preload_separate_scoped,
    :preload_through_join,
    :select,
    :select_merge,
    :where,
    :where_any,
    :where_exists,
    :where_exists_subquery,
    :where_not_exists,
    :where_not_exists_subquery
  ]

  @from_opts_supported_operations_boundary_string Enum.map_join(
                                                    @from_opts_supported_operations_boundary,
                                                    ", ",
                                                    &inspect/1
                                                  )

  @from_opts_supported_operations_full_string Enum.map_join(
                                                @from_opts_supported_operations_full,
                                                ", ",
                                                &inspect/1
                                              )

  def from_opts(query, opts) do
    from_opts(query, opts, mode: :boundary)
  end

  def from_opts(query, opts, from_opts_opts) do
    __from_opts__(query, opts, __MODULE__, from_opts_opts)
  end

  @doc false
  def __from_opts__(query, opts, apply_module) do
    __from_opts__(query, opts, apply_module, mode: :boundary)
  end

  @doc false
  def __from_opts__(query, opts, apply_module, from_opts_opts) do
    from_opts_opts = validate_from_opts_options!(from_opts_opts)
    mode = Keyword.fetch!(from_opts_opts, :mode)
    do_from_opts(query, opts, apply_module, mode)
  end

  defp do_from_opts(query, nil, _apply_module, _mode), do: query
  defp do_from_opts(query, [], _apply_module, _mode), do: query

  defp do_from_opts(_query, opts, _apply_module, _mode) when not is_list(opts) do
    raise ArgumentError,
          "from_opts/2 expects opts to be a keyword list like `[where: ...]`, got: #{inspect(opts)}"
  end

  defp do_from_opts(_query, [invalid | _] = opts, _apply_module, _mode)
       when not is_tuple(invalid) or tuple_size(invalid) != 2 do
    raise ArgumentError,
          "from_opts/2 expects opts to be a keyword list (list of `{operation, value}` pairs); " <>
            "got invalid entry: #{inspect(invalid)} in #{inspect(opts)}"
  end

  defp do_from_opts(query, [{operation, raw_arguments} | tail], apply_module, mode) do
    unless is_atom(operation) do
      raise ArgumentError,
            "from_opts/2 expects operation keys to be atoms, got: #{inspect(operation)}"
    end

    if is_nil(raw_arguments) do
      raise ArgumentError,
            "from_opts/2 does not accept nil for #{inspect(operation)}; omit the operation or pass []"
    end

    if is_tuple(raw_arguments) do
      validate_from_opts_tuple_arguments!(query, apply_module, operation, raw_arguments, mode)
    end

    arguments = normalize_from_opts_arguments!(raw_arguments, mode)
    arity = 1 + length(arguments)

    if apply_module == __MODULE__ do
      validate_query_builder_from_opts_operation!(operation, arity, mode)
    else
      validate_extension_from_opts_operation!(apply_module, operation, arity, mode)
    end

    if mode == :boundary do
      validate_from_opts_boundary_arguments!(operation, arguments)
    end

    result = apply(apply_module, operation, [query | arguments])
    do_from_opts(result, tail, apply_module, mode)
  end

  defp normalize_from_opts_arguments!(raw_arguments, mode) do
    case raw_arguments do
      %QueryBuilder.Args{} when mode == :boundary ->
        raise ArgumentError,
              "from_opts/2 does not accept QueryBuilder.args/* wrappers in boundary mode; " <>
                "pass a single argument (where/order_by/limit/offset) and avoid assoc traversal. " <>
                "If you intended to use the full from_opts surface, pass `mode: :full`."

      %QueryBuilder.Args{args: args} when is_list(args) and length(args) >= 2 ->
        args

      %QueryBuilder.Args{args: args} when is_list(args) ->
        raise ArgumentError,
              "from_opts/2 expects QueryBuilder.args/* to wrap at least 2 arguments, got: #{inspect(args)}"

      %QueryBuilder.Args{} = args ->
        raise ArgumentError,
              "from_opts/2 expects QueryBuilder.args/* to wrap a list of arguments; got: #{inspect(args)}"

      other ->
        [other]
    end
  end

  defp validate_query_builder_from_opts_operation!(operation, arity, mode) do
    supported_operations =
      case mode do
        :boundary -> @from_opts_supported_operations_boundary
        :full -> @from_opts_supported_operations_full
      end

    supported_operations_string =
      case mode do
        :boundary -> @from_opts_supported_operations_boundary_string
        :full -> @from_opts_supported_operations_full_string
      end

    unless function_exported?(__MODULE__, operation, arity) do
      raise ArgumentError,
            "unknown operation #{inspect(operation)}/#{arity} in from_opts/2; " <>
              "supported operations: #{supported_operations_string}"
    end

    unless operation in supported_operations do
      extra =
        if mode == :boundary do
          " If you intended to use joins/preloads/assoc traversal, pass `mode: :full`."
        else
          ""
        end

      raise ArgumentError,
            "operation #{inspect(operation)}/#{arity} is not supported in from_opts/2 (mode: #{inspect(mode)}); " <>
              "supported operations: #{supported_operations_string}." <> extra
    end
  end

  defp validate_extension_from_opts_operation!(apply_module, operation, arity, mode) do
    supported_operations =
      case mode do
        :boundary -> @from_opts_supported_operations_boundary
        :full -> @from_opts_supported_operations_full
      end

    supported_operations_string =
      case mode do
        :boundary -> @from_opts_supported_operations_boundary_string
        :full -> @from_opts_supported_operations_full_string
      end

    if mode == :boundary and operation not in supported_operations do
      raise ArgumentError,
            "operation #{inspect(operation)}/#{arity} is not supported in from_opts/2 (mode: #{inspect(mode)}); " <>
              "supported operations: #{supported_operations_string}. If you intended to use full mode, pass `mode: :full`."
    end

    if mode == :full and function_exported?(__MODULE__, operation, arity) and
         operation not in supported_operations do
      raise ArgumentError,
            "operation #{inspect(operation)}/#{arity} is not supported in from_opts/2 (mode: #{inspect(mode)}); " <>
              "supported operations: #{supported_operations_string}"
    end

    unless function_exported?(apply_module, operation, arity) do
      available =
        apply_module.__info__(:functions)
        |> Enum.map(&elem(&1, 0))
        |> Enum.uniq()
        |> Enum.sort()
        |> Enum.join(", ")

      raise ArgumentError,
            "unknown operation #{inspect(operation)}/#{arity} in from_opts/2; " <>
              "expected a public function on #{inspect(apply_module)}. Available operations: #{available}"
    end
  end

  defp validate_from_opts_tuple_arguments!(query, apply_module, operation, raw_arguments, mode) do
    cond do
      operation == :where and tuple_size(raw_arguments) < 2 ->
        raise ArgumentError,
              "from_opts/2 expects `where:` tuple filters to have at least 2 elements " <>
                "(e.g. `{field, value}` or `{field, operator, value}`); got: #{inspect(raw_arguments)}"

      # Migration guard: v1's from_list/from_opts expanded `{assoc_fields, filters, ...}` tuples
      # into multi-arg calls. v2 treats tuple values as data, so we fail fast and point callers
      # at the explicit wrapper (`QueryBuilder.args/*`).
      operation == :where and from_opts_where_tuple_looks_like_assoc_pack?(query, raw_arguments) ->
        raise ArgumentError,
              "from_opts/2 does not treat `where: {assoc_fields, filters, ...}` as a multi-arg call. " <>
                "Use `where: QueryBuilder.args(assoc_fields, filters, ...)` with `mode: :full` instead; " <>
                "got: #{inspect(raw_arguments)}"

      operation in [:where, :select] ->
        :ok

      operation in @from_opts_supported_operations_full ->
        case mode do
          :boundary ->
            raise ArgumentError,
                  "from_opts/2 boundary mode does not accept tuple values for #{inspect(operation)}. " <>
                    "Pass a single argument value. If you intended a multi-arg call, use `mode: :full` " <>
                    "and wrap arguments with `QueryBuilder.args/*`. Got: #{inspect(raw_arguments)}"

          :full ->
            raise ArgumentError,
                  "from_opts/2 does not accept tuple values for #{inspect(operation)}. " <>
                    "If you intended to call #{inspect(operation)} with multiple arguments, " <>
                    "wrap them with `QueryBuilder.args/*`. Got: #{inspect(raw_arguments)}"
        end

      apply_module != __MODULE__ and
        function_exported?(apply_module, operation, tuple_size(raw_arguments) + 1) and
          not function_exported?(apply_module, operation, 2) ->
        case mode do
          :boundary ->
            raise ArgumentError,
                  "from_opts/2 boundary mode does not expand tuple values into multiple arguments for #{inspect(operation)}. " <>
                    "Pass a single argument value. If you intended a multi-arg call, use `mode: :full` " <>
                    "and wrap arguments with `#{inspect(apply_module)}.args/*` (or `QueryBuilder.args/*`). " <>
                    "Got: #{inspect(raw_arguments)}"

          :full ->
            raise ArgumentError,
                  "from_opts/2 does not expand tuple values into multiple arguments for #{inspect(operation)}. " <>
                    "Use `#{inspect(apply_module)}.args/*` (or `QueryBuilder.args/*`) to wrap multiple arguments; " <>
                    "got: #{inspect(raw_arguments)}"
        end

      true ->
        :ok
    end
  end

  # Migration shim: v1 exposed from_list/2. Keep it to raise with a clear upgrade hint.
  def from_list(_query, _opts) do
    raise ArgumentError,
          "from_list/2 was renamed to from_opts/2; please update your call sites"
  end

  @doc false
  def from_opts_supported_operations(), do: @from_opts_supported_operations_boundary

  @doc false
  def from_opts_supported_operations(:boundary), do: @from_opts_supported_operations_boundary

  @doc false
  def from_opts_supported_operations(:full), do: @from_opts_supported_operations_full

  # Migration helper: distinguish where filter tuples (data) from the old v1 "assoc_fields pack"
  # tuple that used to mean "call where/3 or where/4". This lets `from_opts/2` raise a targeted
  # error instead of silently changing meaning.
  defp from_opts_where_tuple_looks_like_assoc_pack?(query, tuple) when is_tuple(tuple) do
    if tuple_size(tuple) < 2 do
      false
    else
      assoc_fields = elem(tuple, 0)
      second = elem(tuple, 1)

      cond do
        is_list(assoc_fields) ->
          true

        # Likely a filter tuple: {field, operator, value} / {field, operator, value, opts}
        is_atom(assoc_fields) and tuple_size(tuple) >= 3 and is_atom(second) ->
          false

        # Likely a filter tuple: {field, value} (scalar value)
        is_atom(assoc_fields) and tuple_size(tuple) == 2 and not is_list(second) ->
          false

        is_atom(assoc_fields) ->
          source_schema = QueryBuilder.Utils.root_schema(query)
          assoc_fields in source_schema.__schema__(:associations)

        true ->
          false
      end
    end
  end

  defp normalize_or_groups!(or_groups, opt_key, context) do
    cond do
      is_nil(or_groups) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` to be a list of filter groups; got nil"

      Keyword.keyword?(or_groups) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` to be a list of filter groups like `[[...], [...]]`; " <>
                "got a keyword list. Wrap it in a list if you intended a single group."

      not is_list(or_groups) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` to be a list of filter groups like `[[...], [...]]`; got: #{inspect(or_groups)}"

      Enum.any?(or_groups, &(not is_list(&1))) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` groups to be lists (e.g. `[[title: \"A\"], [title: \"B\"]]`); got: #{inspect(or_groups)}"

      true ->
        or_groups
    end
  end

  defp validate_from_opts_options!(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "from_opts/3 expects options to be a keyword list like `[mode: :boundary]`, got: #{inspect(opts)}"
    end

    mode = Keyword.get(opts, :mode, :boundary)

    unless mode in [:boundary, :full] do
      raise ArgumentError,
            "from_opts/3 expects `mode:` to be :boundary or :full, got: #{inspect(mode)}"
    end

    case Keyword.keys(opts) -- [:mode] do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "from_opts/3 got unknown options #{inspect(unknown)}; supported options: [:mode]"
    end

    [mode: mode]
  end

  defp validate_from_opts_options!(opts) do
    raise ArgumentError,
          "from_opts/3 expects options to be a keyword list like `[mode: :boundary]`, got: #{inspect(opts)}"
  end

  defp validate_from_opts_boundary_arguments!(:where, [filters]) do
    validate_from_opts_boundary_filters!(filters, "where")
  end

  defp validate_from_opts_boundary_arguments!(:where_any, [or_groups]) do
    validate_from_opts_boundary_or_groups!(or_groups, "where_any")
  end

  defp validate_from_opts_boundary_arguments!(:order_by, [value]) do
    validate_from_opts_boundary_order_by!(value)
  end

  defp validate_from_opts_boundary_arguments!(:limit, [value]) do
    validate_from_opts_boundary_non_negative_limit_offset!(value, :limit)
  end

  defp validate_from_opts_boundary_arguments!(:offset, [value]) do
    validate_from_opts_boundary_non_negative_limit_offset!(value, :offset)
  end

  defp validate_from_opts_boundary_arguments!(operation, _arguments) do
    raise ArgumentError,
          "operation #{inspect(operation)} is not supported in from_opts/2 (mode: :boundary); " <>
            "supported operations: #{@from_opts_supported_operations_boundary_string}. " <>
            "If you intended to use full mode, pass `mode: :full`."
  end

  defp validate_from_opts_boundary_non_negative_limit_offset!(value, operation)
       when operation in [:limit, :offset] and is_integer(value) do
    if value < 0 do
      raise ArgumentError,
            "from_opts/2 boundary mode expects #{operation} to be non-negative, got: #{inspect(value)}"
    end

    :ok
  end

  defp validate_from_opts_boundary_non_negative_limit_offset!(value, operation)
       when operation in [:limit, :offset] and is_binary(value) do
    trimmed = String.trim(value)

    case Integer.parse(trimmed) do
      {int_value, ""} when int_value < 0 ->
        raise ArgumentError,
              "from_opts/2 boundary mode expects #{operation} to be non-negative, got: #{inspect(value)}"

      _ ->
        :ok
    end
  end

  defp validate_from_opts_boundary_non_negative_limit_offset!(_value, _operation), do: :ok

  defp validate_from_opts_boundary_or_groups!(or_groups, context) do
    or_groups = normalize_or_groups!(or_groups, :where_any, "#{context} boundary validation")
    Enum.each(or_groups, &validate_from_opts_boundary_filters!(&1, context))
    :ok
  end

  defp validate_from_opts_boundary_filters!(filters, context) do
    cond do
      filters == [] ->
        :ok

      is_list(filters) ->
        Enum.each(filters, &validate_from_opts_boundary_filter!(&1, context))

      is_tuple(filters) ->
        validate_from_opts_boundary_filter!(filters, context)

      is_function(filters) ->
        raise ArgumentError,
              "from_opts/2 boundary mode does not allow function filters in #{context}; " <>
                "use explicit QueryBuilder calls instead"

      true ->
        raise ArgumentError,
              "from_opts/2 boundary mode expects #{context} filters to be a keyword list, a list of filters, or a filter tuple; " <>
                "got: #{inspect(filters)}"
    end
  end

  defp validate_from_opts_boundary_filter!(%QueryBuilder.Aggregate{} = aggregate, context) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregate expressions in #{context}: #{inspect(aggregate)}"
  end

  defp validate_from_opts_boundary_filter!(
         {%QueryBuilder.Aggregate{} = aggregate, _value},
         context
       ) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregate expressions in #{context}: #{inspect(aggregate)}"
  end

  defp validate_from_opts_boundary_filter!(
         {%QueryBuilder.Aggregate{} = aggregate, _operator, _value},
         context
       ) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregate expressions in #{context}: #{inspect(aggregate)}"
  end

  defp validate_from_opts_boundary_filter!(
         {%QueryBuilder.Aggregate{} = aggregate, _operator, _value, _opts},
         context
       ) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregate expressions in #{context}: #{inspect(aggregate)}"
  end

  defp validate_from_opts_boundary_filter!(fun, context) when is_function(fun) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow function filters in #{context}; " <>
            "use explicit QueryBuilder calls instead"
  end

  defp validate_from_opts_boundary_filter!({field, value}, context) do
    validate_from_opts_boundary_token!(field, context)
    validate_from_opts_boundary_filter_value!(value, context)
    :ok
  end

  defp validate_from_opts_boundary_filter!({field, operator, value}, context) do
    validate_from_opts_boundary_filter!({field, operator, value, []}, context)
  end

  defp validate_from_opts_boundary_filter!({field, operator, value, _operator_opts}, context)
       when is_atom(operator) do
    validate_from_opts_boundary_token!(field, context)
    validate_from_opts_boundary_filter_value!(value, context)
    :ok
  end

  defp validate_from_opts_boundary_filter!({field, operator, _value, _operator_opts}, context) do
    raise ArgumentError,
          "from_opts/2 boundary mode expects #{context} filter operators to be atoms, got: #{inspect(operator)} for field #{inspect(field)}"
  end

  defp validate_from_opts_boundary_filter!(other, context) do
    raise ArgumentError,
          "from_opts/2 boundary mode received an invalid #{context} filter: #{inspect(other)}"
  end

  defp validate_from_opts_boundary_filter_value!(value, context)
       when is_struct(value, Ecto.Query) or is_struct(value, Ecto.SubQuery) or
              is_struct(value, QueryBuilder.Query) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow subqueries in #{context} filters; got: #{inspect(value)}"
  end

  defp validate_from_opts_boundary_filter_value!(value, context)
       when is_struct(value, Ecto.Query.DynamicExpr) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow dynamic expressions in #{context} filters; got: #{inspect(value)}"
  end

  defp validate_from_opts_boundary_filter_value!(value, context) when is_atom(value) do
    str = Atom.to_string(value)

    if String.ends_with?(str, "@self") do
      referenced = binary_part(str, 0, byte_size(str) - byte_size("@self"))

      if String.contains?(referenced, "@") do
        raise ArgumentError,
              "from_opts/2 boundary mode does not allow assoc tokens in field-to-field filters in #{context}; got: #{inspect(value)}"
      end
    end

    :ok
  end

  defp validate_from_opts_boundary_filter_value!(_value, _context), do: :ok

  defp validate_from_opts_boundary_order_by!(value) do
    cond do
      value == [] ->
        :ok

      is_list(value) ->
        Enum.each(value, &validate_from_opts_boundary_order_expr!/1)

      true ->
        raise ArgumentError,
              "from_opts/2 boundary mode expects order_by to be a keyword list (or list of order expressions), got: #{inspect(value)}"
    end
  end

  defp validate_from_opts_boundary_order_expr!({direction, expr}) when is_atom(direction) do
    validate_from_opts_boundary_order_expr_value!(expr)
  end

  defp validate_from_opts_boundary_order_expr!(other) do
    raise ArgumentError,
          "from_opts/2 boundary mode received an invalid order_by expression: #{inspect(other)}"
  end

  defp validate_from_opts_boundary_order_expr_value!(%QueryBuilder.Aggregate{} = aggregate) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregates in order_by: #{inspect(aggregate)}"
  end

  defp validate_from_opts_boundary_order_expr_value!(%Ecto.Query.DynamicExpr{} = dynamic) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow dynamic expressions in order_by: #{inspect(dynamic)}"
  end

  defp validate_from_opts_boundary_order_expr_value!(fun) when is_function(fun) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow function order_by expressions; " <>
            "use explicit QueryBuilder calls instead"
  end

  defp validate_from_opts_boundary_order_expr_value!(token)
       when is_atom(token) or is_binary(token) do
    validate_from_opts_boundary_token!(token, "order_by")
    :ok
  end

  defp validate_from_opts_boundary_order_expr_value!(other) do
    raise ArgumentError,
          "from_opts/2 boundary mode expects order_by expressions to be tokens (atoms/strings), got: #{inspect(other)}"
  end

  defp validate_from_opts_boundary_token!(token, context)
       when is_atom(token) or is_binary(token) do
    if token |> to_string() |> String.contains?("@") do
      raise ArgumentError,
            "from_opts/2 boundary mode does not allow assoc tokens (field@assoc) in #{context}: #{inspect(token)}"
    end

    :ok
  end

  defp validate_from_opts_boundary_token!(token, context) do
    raise ArgumentError,
          "from_opts/2 boundary mode expects #{context} field tokens to be atoms or strings, got: #{inspect(token)}"
  end

  defp normalize_preload_separate_scoped_opts!(opts, assoc_field) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "preload_separate_scoped/3 expects opts to be a keyword list, got: #{inspect(opts)}"
    end

    allowed_keys = [:where, :order_by]

    unknown =
      opts
      |> Keyword.keys()
      |> Enum.uniq()
      |> Enum.reject(&(&1 in allowed_keys))

    if unknown != [] do
      raise ArgumentError,
            "preload_separate_scoped/3 got unknown options #{inspect(unknown)} for " <>
              "#{inspect(assoc_field)} (supported: :where, :order_by)"
    end

    where_filters = Keyword.get(opts, :where, [])
    order_by = Keyword.get(opts, :order_by, [])

    validate_scoped_preload_where_filters!(assoc_field, where_filters)
    validate_scoped_preload_order_by!(assoc_field, order_by)

    if where_filters == [] and order_by == [] do
      nil
    else
      [where: where_filters, order_by: order_by]
    end
  end

  defp validate_scoped_preload_where_filters!(assoc_field, nil) do
    raise ArgumentError,
          "preload_separate_scoped/3 expects `where:` to be a keyword list (or list of filters) for " <>
            "#{inspect(assoc_field)}, got nil"
  end

  defp validate_scoped_preload_where_filters!(assoc_field, filters) do
    filters = List.wrap(filters)

    Enum.each(filters, fn
      fun when is_function(fun) ->
        raise ArgumentError,
              "preload_separate_scoped/3 does not accept custom filter functions in `where:` for " <>
                "#{inspect(assoc_field)}; use an explicit Ecto preload query instead"

      %QueryBuilder.Aggregate{} = aggregate ->
        raise ArgumentError,
              "preload_separate_scoped/3 does not accept aggregate filters in `where:` for " <>
                "#{inspect(assoc_field)}; got: #{inspect(aggregate)}"

      {%QueryBuilder.Aggregate{} = aggregate, _value} ->
        raise ArgumentError,
              "preload_separate_scoped/3 does not accept aggregate filters in `where:` for " <>
                "#{inspect(assoc_field)}; got: #{inspect(aggregate)}"

      {field, value} ->
        validate_scoped_preload_field_token!(assoc_field, field)
        validate_scoped_preload_value_token!(assoc_field, value)

      {field, _operator, value} ->
        validate_scoped_preload_field_token!(assoc_field, field)
        validate_scoped_preload_value_token!(assoc_field, value)

      {field, _operator, value, _operator_opts} ->
        validate_scoped_preload_field_token!(assoc_field, field)
        validate_scoped_preload_value_token!(assoc_field, value)

      other ->
        raise ArgumentError,
              "preload_separate_scoped/3 got an invalid `where:` entry for #{inspect(assoc_field)}: " <>
                "#{inspect(other)}"
    end)
  end

  defp validate_scoped_preload_order_by!(assoc_field, nil) do
    raise ArgumentError,
          "preload_separate_scoped/3 expects `order_by:` to be a keyword list for " <>
            "#{inspect(assoc_field)}, got nil"
  end

  defp validate_scoped_preload_order_by!(assoc_field, order_by) do
    unless Keyword.keyword?(order_by) do
      raise ArgumentError,
            "preload_separate_scoped/3 expects `order_by:` to be a keyword list for " <>
              "#{inspect(assoc_field)}, got: #{inspect(order_by)}"
    end

    Enum.each(order_by, fn
      {direction, field} when is_atom(direction) and is_atom(field) ->
        validate_scoped_preload_field_token!(assoc_field, field)

      {direction, other} when is_atom(direction) ->
        raise ArgumentError,
              "preload_separate_scoped/3 expects `order_by:` fields to be tokens (atoms) for " <>
                "#{inspect(assoc_field)}, got: #{inspect(other)}"

      other ->
        raise ArgumentError,
              "preload_separate_scoped/3 expects `order_by:` entries to be `{direction, token}` for " <>
                "#{inspect(assoc_field)}, got: #{inspect(other)}"
    end)
  end

  defp validate_scoped_preload_field_token!(assoc_field, field) when is_atom(field) do
    token = Atom.to_string(field)

    if String.contains?(token, "@") do
      raise ArgumentError,
            "preload_separate_scoped/3 does not allow assoc tokens (containing `@`) for " <>
              "#{inspect(assoc_field)}; got: #{inspect(field)}"
    end
  end

  defp validate_scoped_preload_field_token!(assoc_field, other) do
    raise ArgumentError,
          "preload_separate_scoped/3 expects field tokens to be atoms for " <>
            "#{inspect(assoc_field)}, got: #{inspect(other)}"
  end

  defp validate_scoped_preload_value_token!(_assoc_field, value) when not is_atom(value), do: :ok

  defp validate_scoped_preload_value_token!(assoc_field, value) when is_atom(value) do
    marker = "@self"
    value_str = Atom.to_string(value)

    if String.ends_with?(value_str, marker) do
      referenced = binary_part(value_str, 0, byte_size(value_str) - byte_size(marker))

      if String.contains?(referenced, "@") do
        raise ArgumentError,
              "preload_separate_scoped/3 does not allow assoc tokens (containing `@`) in field-to-field " <>
                "filters for #{inspect(assoc_field)}; got: #{inspect(value)}"
      end
    end

    :ok
  end

  defp ensure_query_has_binding(query) do
    ecto_query =
      try do
        Ecto.Queryable.to_query(query)
      rescue
        Protocol.UndefinedError ->
          raise ArgumentError,
                "expected an Ecto.Queryable (schema module, Ecto.Query, or QueryBuilder.Query), got: #{inspect(query)}"
      end

    schema = QueryBuilder.Utils.root_schema(ecto_query)
    binding = schema._binding()
    root_as = ecto_query.from.as
    binding_used? = Query.has_named_binding?(ecto_query, binding)

    cond do
      root_as == binding ->
        ecto_query

      is_nil(root_as) and binding_used? ->
        raise ArgumentError,
              "expected root query to have named binding #{inspect(binding)} (#{inspect(schema)}), " <>
                "but that binding name is already used by another binding in the query (likely a join). " <>
                "QueryBuilder relies on #{inspect(binding)} referring to the root schema. " <>
                "Fix: rename the conflicting binding (avoid `as: #{inspect(binding)}` on joins), " <>
                "or start from the schema module (e.g. `#{inspect(schema)}`) instead of a pre-joined query."

      is_nil(root_as) ->
        Ecto.Query.from(ecto_query, as: ^binding)

      true ->
        collision_hint =
          if binding_used? do
            " The query also has a non-root named binding #{inspect(binding)}, so QueryBuilder cannot add it to the root."
          else
            ""
          end

        raise ArgumentError,
              "expected root query to have named binding #{inspect(binding)} (#{inspect(schema)}), " <>
                "but it already has named binding #{inspect(root_as)}." <>
                " Use `from(query, as: ^#{inspect(binding)})` before passing it to QueryBuilder." <>
                collision_hint
    end
  end

  defp assoc_fields_nested?(assoc_fields) do
    assoc_fields
    |> List.wrap()
    |> Enum.any?(fn
      {_field, nested_assoc_fields} -> nested_assoc_fields != []
      _ -> false
    end)
  end
end
