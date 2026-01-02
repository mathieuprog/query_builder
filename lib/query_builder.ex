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

  def paginate(%QueryBuilder.Query{} = query, repo, opts \\ []) do
    page_size = Keyword.get(opts, :page_size, default_page_size())
    cursor_direction = Keyword.get(opts, :direction, :after)
    unsafe_sql_row_pagination? = Keyword.get(opts, :unsafe_sql_row_pagination?, false)

    unless cursor_direction in [:after, :before] do
      raise ArgumentError, "cursor direction #{inspect(cursor_direction)} is invalid"
    end

    base_ecto_query = Ecto.Queryable.to_query(query.ecto_query)

    if base_ecto_query.order_bys != [] do
      raise ArgumentError,
            "paginate/3 does not support paginating a query whose base ecto_query already has `order_by` clauses; " <>
              "express ordering via `QueryBuilder.order_by/*` (or remove base ordering via `Ecto.Query.exclude(base_query, :order_by)`) " <>
              "before calling paginate/3. base order_bys: #{inspect(base_ecto_query.order_bys)}"
    end

    page_size =
      if max_page_size = Keyword.get(opts, :max_page_size) do
        min(max_page_size, page_size)
      else
        page_size
      end

    cursor = decode_cursor!(Keyword.get(opts, :cursor))

    query = limit(query, page_size + 1)

    already_sorting_on_id? =
      Enum.any?(query.operations, fn
        %{type: :order_by, args: [keyword_list]} ->
          Enum.member?(Keyword.values(keyword_list), :id)

        _ ->
          false
      end)

    query =
      if already_sorting_on_id? do
        query
      else
        order_by(query, asc: :id)
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
      |> Enum.uniq_by(fn {_direction, field} -> field end)

    cursor_pagination_supported? =
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

    {entries, first_row_cursor_map, last_row_cursor_map, has_more?} =
      if cursor_pagination_supported? do
        ensure_paginate_select_is_root!(ecto_query)

        if single_query_cursor_pagination_possible?(ecto_query, assoc_list, order_by_list) do
          entries = repo.all(ecto_query)

          entries =
            if cursor_direction == :before do
              Enum.reverse(entries)
            else
              entries
            end

          has_more? = length(entries) == page_size + 1

          entries =
            if has_more? do
              case cursor_direction do
                :before -> tl(entries)
                :after -> List.delete_at(entries, -1)
              end
            else
              entries
            end

          first_entry = List.first(entries)
          last_entry = List.last(entries)

          {entries, cursor_map_from_entry(first_entry, order_by_list),
           cursor_map_from_entry(last_entry, order_by_list), has_more?}
        else
          source_schema = QueryBuilder.Utils.root_schema(ecto_query)

          cursor_select_map = build_cursor_select_map(ecto_query, assoc_list, order_by_list)

          page_keys_query =
            ecto_query
            |> Query.exclude([:preload, :select])
            |> Ecto.Query.select([{^source_schema, x}], ^cursor_select_map)
            |> Ecto.Query.distinct(true)

          page_key_rows = repo.all(page_keys_query)

          page_key_rows =
            if cursor_direction == :before do
              Enum.reverse(page_key_rows)
            else
              page_key_rows
            end

          has_more? = length(page_key_rows) == page_size + 1

          page_key_rows =
            if has_more? do
              case cursor_direction do
                :before -> tl(page_key_rows)
                :after -> List.delete_at(page_key_rows, -1)
              end
            else
              page_key_rows
            end

          ids = Enum.map(page_key_rows, &Map.fetch!(&1, "id"))

          if length(ids) != length(Enum.uniq(ids)) do
            raise ArgumentError,
                  "paginate/3 could not produce a page of unique root rows; " <>
                    "this usually means your order_by depends on a to-many join (e.g. ordering by a has_many field). " <>
                    "Use an aggregation (e.g. max/min) or order by root/to-one fields only. " <>
                    "order_by: #{inspect(order_by_list)}"
          end

          entries = load_entries_for_page(repo, ecto_query, source_schema, ids)

          first_row = List.first(page_key_rows)
          last_row = List.last(page_key_rows)

          {entries, first_row, last_row, has_more?}
        end
      else
        entries = repo.all(ecto_query)

        entries =
          if cursor_direction == :before do
            Enum.reverse(entries)
          else
            entries
          end

        has_more? = length(entries) == page_size + 1

        entries =
          if has_more? do
            case cursor_direction do
              :before -> tl(entries)
              :after -> List.delete_at(entries, -1)
            end
          else
            entries
          end

        {entries, nil, nil, has_more?}
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
      assoc_data.preload and assoc_data.cardinality == :one
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
    (assoc_data.preload and assoc_data.cardinality == :many) ||
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

  defp load_entries_for_page(_repo, _ecto_query, _source_schema, []), do: []

  defp load_entries_for_page(repo, ecto_query, source_schema, ids) when is_list(ids) do
    entries_query =
      ecto_query
      |> Query.exclude([:limit, :offset, :order_by])
      |> Ecto.Query.where([{^source_schema, x}], field(x, :id) in ^ids)

    entries = repo.all(entries_query)

    entries_by_id =
      Enum.reduce(entries, %{}, fn entry, acc ->
        Map.put_new(acc, Map.fetch!(entry, :id), entry)
      end)

    Enum.map(ids, fn id ->
      case Map.fetch(entries_by_id, id) do
        {:ok, entry} ->
          entry

        :error ->
          raise ArgumentError,
                "paginate/3 internal error: expected to load an entry with id #{inspect(id)}, " <>
                  "but it was missing from the results"
      end
    end)
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
  Preloads the associations.

  Bindings are automatically set if joins have been made, or if it is preferable to
  join (i.e. one-to-one associations are preferable to include into the query result
  rather than emitting separate DB queries).

  Example:
  ```
  QueryBuilder.preload(query, [role: :permissions, articles: [:stars, comments: :user]])
  ```
  """
  def preload(%QueryBuilder.Query{} = query, assoc_fields) do
    %{query | operations: [%{type: :preload, assocs: assoc_fields, args: []} | query.operations]}
  end

  def preload(ecto_query, assoc_fields) do
    ecto_query = ensure_query_has_binding(ecto_query)
    preload(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields)
  end

  @doc ~S"""
  An AND where query expression.

  Example:
  ```
  QueryBuilder.where(query, firstname: "John")
  ```
  """
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

  def where_exists_subquery(_query, _assoc_fields, _filters, _opts) do
    raise ArgumentError,
          "where_exists_subquery/4 was replaced by where_exists_subquery/3; " <>
            "use `where_exists_subquery(assoc_fields, where: [...], where_any: [[...], ...], scope: [...])`"
  end

  def where_not_exists_subquery(_query, _assoc_fields, _filters, _opts) do
    raise ArgumentError,
          "where_not_exists_subquery/4 was replaced by where_not_exists_subquery/3; " <>
            "use `where_not_exists_subquery(assoc_fields, where: [...], where_any: [[...], ...], scope: [...])`"
  end

  def where_exists(_query, _assoc_fields, _filters, _or_filters \\ []) do
    raise ArgumentError,
          "where_exists/4 was renamed to where_exists_subquery/3; " <>
            "use `where_exists_subquery(assoc_fields, where: [...], scope: [...])`"
  end

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
  An order by query expression.

  Example:
  ```
  QueryBuilder.order_by(query, asc: :lastname, asc: :firstname)
  ```
  """
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
  A join query expression.

  Example:
  ```
  QueryBuilder.left_join(query, :articles, title@articles: "Foo", or: [title@articles: "Bar"])
  ```
  """
  def left_join(query, assoc_fields, filters \\ [], or_filters \\ [])

  def left_join(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    %{
      query
      | operations: [
          %{
            type: :left_join,
            assocs: assoc_fields,
            join_filters: [List.wrap(filters), List.wrap(or_filters)]
          }
          | query.operations
        ]
    }
  end

  def left_join(ecto_query, assoc_fields, filters, or_filters) do
    ecto_query = ensure_query_has_binding(ecto_query)
    left_join(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, filters, or_filters)
  end

  @doc ~S"""
  Allows to pass a list of operations through a keyword list.

  Example:
  ```
  QueryBuilder.from_opts(query, [
    where: [name: "John", city: "Anytown"],
    preload: [articles: :comments]
  ])
  ```
  """
  def from_opts(query, nil), do: query
  def from_opts(query, []), do: query

  def from_opts(query, [{operation, arguments} | tail]) do
    arguments =
      cond do
        is_tuple(arguments) -> Tuple.to_list(arguments)
        is_list(arguments) -> [arguments]
        true -> List.wrap(arguments)
      end

    arity = 1 + length(arguments)

    unless function_exported?(__MODULE__, operation, arity) do
      available =
        __MODULE__.__info__(:functions)
        |> Enum.map(&elem(&1, 0))
        |> Enum.uniq()
        |> Enum.sort()
        |> Enum.join(", ")

      raise ArgumentError,
            "unknown operation #{inspect(operation)}/#{arity} in from_opts/2; " <>
              "expected a public function on #{inspect(__MODULE__)}. Available operations: #{available}"
    end

    apply(__MODULE__, operation, [query | arguments]) |> from_opts(tail)
  end

  def from_list(_query, _opts) do
    raise ArgumentError,
          "from_list/2 was renamed to from_opts/2; please update your call sites"
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

  defp ensure_query_has_binding(query) do
    ecto_query = Ecto.Queryable.to_query(query)
    schema = QueryBuilder.Utils.root_schema(ecto_query)
    binding = schema._binding()

    if Query.has_named_binding?(ecto_query, binding) do
      ecto_query
    else
      case ecto_query.from.as do
        nil ->
          Ecto.Query.from(ecto_query, as: ^binding)

        other ->
          raise ArgumentError,
                "expected root query to have named binding #{inspect(binding)} (#{inspect(schema)}), " <>
                  "but it already has named binding #{inspect(other)}. " <>
                  "Use `from(query, as: #{inspect(binding)})` before passing it to QueryBuilder."
      end
    end
  end
end
