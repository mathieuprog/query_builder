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

    unless cursor_direction in [:after, :before] do
      raise ArgumentError, "cursor direction #{inspect(cursor_direction)} is invalid"
    end

    page_size =
      if max_page_size = Keyword.get(opts, :max_page_size) do
        min(max_page_size, page_size)
      else
        page_size
      end

    cursor =
      case Keyword.get(opts, :cursor) || %{} do
        cursor when is_map(cursor) ->
          cursor

        cursor ->
          with {:ok, decoded_string} <- Base.url_decode64(cursor),
               {:ok, cursor} <- Jason.decode(decoded_string) do
            cursor
          else
            _ -> %{}
          end
      end

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

    # reverse sorting order if direction is before
    operations =
      if cursor_direction == :before do
        query.operations
        |> Enum.map(fn
          %{type: :order_by, args: [keyword_list]} = operation ->
            updated_keyword_list =
              Enum.map(keyword_list, fn {direction, field} ->
                if String.contains?(to_string(field), "@") do
                  {direction, field}
                else
                  case direction do
                    :asc -> {:desc, field}
                    :desc -> {:asc, field}
                  end
                end
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
      |> Enum.flat_map(&Map.fetch!(&1, :args))
      |> Enum.flat_map(
        &Enum.reject(&1, fn {_direction, field} -> String.contains?(to_string(field), "@") end)
      )
      |> Enum.uniq_by(fn {_direction, field} -> field end)

    cursor_fields = Map.keys(cursor)

    valid_cursor? =
      Enum.all?(Keyword.values(order_by_list), &Enum.member?(cursor_fields, to_string(&1)))

    query =
      if valid_cursor? do
        {_, filters} =
          Enum.reduce(order_by_list, {[], []}, fn {order_direction, field},
                                                  {prev_fields, filters} ->
            operator =
              cond do
                order_direction == :desc && cursor_direction == :after ->
                  :lt

                order_direction == :asc && cursor_direction == :after ->
                  :gt

                # we reversed the sorting order when the cursor direction is :before
                order_direction == :desc && cursor_direction == :before ->
                  :lt

                order_direction == :asc && cursor_direction == :before ->
                  :gt
              end

            filter =
              Enum.map(prev_fields, &{&1, cursor[to_string(&1)]})
              |> Enum.concat([{field, operator, cursor[to_string(field)]}])

            {prev_fields ++ [field], filters ++ [filter]}
          end)

        [first_filter | rest_filters] = filters

        or_filters = Enum.map(rest_filters, &{:or, &1})

        where(query, [], first_filter, or_filters)
      else
        query
      end

    entries = repo.all(query)

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
          :before ->
            tl(entries)

          :after ->
            List.delete_at(entries, -1)
        end
      else
        entries
      end

    first_entry = List.first(entries)
    last_entry = List.last(entries)

    build_cursor = fn entry ->
      if entry do
        order_by_list
        |> Enum.map(fn {_, field} -> {field, Map.get(entry, field)} end)
        |> Enum.into(%{})
        |> Jason.encode!()
        |> Base.url_encode64()
      end
    end

    %{
      pagination: %{
        cursor_direction: cursor_direction,
        cursor_for_entries_before: build_cursor.(first_entry),
        cursor_for_entries_after: build_cursor.(last_entry),
        has_more_entries: has_more?,
        max_page_size: page_size
      },
      paginated_entries: entries
    }
  end

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
    offset(%QueryBuilder.Query{ecto_query: ecto_query}, value)
  end

  @doc ~S"""
  A select query expression.

  Specifies which fields will be selected from the schema. If no select is given,
  the entire schema is returned.

  ## Examples

  Select specific fields as a list (returns struct with only those fields):
  ```
  QueryBuilder.select(query, [:id, :name])
  ```

  Select fields and rename them using a map:
  ```
  QueryBuilder.select(query, %{user_id: :id, user_name: :name})
  ```

  Select fields as a tuple:
  ```
  QueryBuilder.select(query, {:id, :name})
  ```

  Select a single field:
  ```
  QueryBuilder.select(query, :name)
  ```

  Select with string field names:
  ```
  QueryBuilder.select(query, ["id", "name"])
  ```
  """
  def select(query, selection) do
    select(query, [], selection)
  end

  @doc ~S"""
  A select query expression with associations.

  Similar to `select/2` but allows selecting fields from associations. 
  Association fields are referenced using the `@` syntax.

  For more about the second argument, see `where/3`.

  ## Examples

  Select fields from an association:
  ```
  QueryBuilder.select(query, :articles, [:id, :title@articles])
  ```

  Select and rename association fields:
  ```
  QueryBuilder.select(query, :role, %{
    user_id: :id,
    user_name: :name,
    role_name: :name@role
  })
  ```
  """
  def select(%QueryBuilder.Query{} = query, assoc_fields, selection) do
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
  A select merge query expression.

  Merges new fields into existing selections. Unlike `select/2`, which replaces
  any previous selection, `select_merge/2` adds to the existing selection.

  If no previous selection exists, it defaults to selecting all fields from
  the source schema.

  ## Examples

  Merge additional fields:
  ```
  query
  |> QueryBuilder.select(%{id: :id})
  |> QueryBuilder.select_merge(%{name: :name})
  ```

  Add computed fields:
  ```
  QueryBuilder.select_merge(query, %{
    total_comments: fragment("count(?)", c.id)
  })
  ```

  Merge a single field:
  ```
  QueryBuilder.select_merge(query, :email)
  ```

  Merge a list of fields:
  ```
  QueryBuilder.select_merge(query, [:created_at, :updated_at])
  ```
  """
  def select_merge(query, selection) do
    select_merge(query, [], selection)
  end

  @doc ~S"""
  A select merge query expression with associations.

  Similar to `select_merge/2` but allows merging fields from associations.
  Association fields are referenced using the `@` syntax.

  For more about the second argument, see `where/3`.

  ## Examples

  Merge association fields:
  ```
  QueryBuilder.select_merge(query, :articles, %{
    article_title: :title@articles,
    article_id: :id@articles
  })
  ```

  Build complex selections incrementally:
  ```
  query
  |> QueryBuilder.select(:role, %{user_id: :id})
  |> QueryBuilder.select_merge(:role, %{role_name: :name@role})
  |> QueryBuilder.select_merge([:articles], %{article_count: fragment("count(?)", a.id)})
  ```
  """
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
  QueryBuilder.from_list(query, [
    where: [name: "John", city: "Anytown"],
    preload: [articles: :comments]
  ])
  ```
  """
  def from_list(query, []), do: query

  def from_list(query, [{operation, arguments} | tail]) do
    arguments =
      cond do
        is_tuple(arguments) -> Tuple.to_list(arguments)
        is_list(arguments) -> [arguments]
        true -> List.wrap(arguments)
      end

    apply(__MODULE__, operation, [query | arguments])
    |> from_list(tail)
  end

  defp ensure_query_has_binding(query) do
    schema = QueryBuilder.Utils.root_schema(query)

    unless Query.has_named_binding?(query, schema._binding()) do
      schema._query()
    else
      query
    end
  end
end
