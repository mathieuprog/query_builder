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

  def where(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    %{query | operations: [%{type: :where, assocs: assoc_fields, args: [filters, or_filters]} | query.operations]}
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
  def order_by(%QueryBuilder.Query{} = query, assoc_fields, value) do
    %{query | operations: [%{type: :order_by, assocs: assoc_fields, args: [value]} | query.operations]}
  end

  def order_by(ecto_query, assoc_fields, value) do
    ecto_query = ensure_query_has_binding(ecto_query)
    order_by(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, value)
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
    %{query | operations: query.operations ++ [%{type: :limit, assocs: [], args: [value]}]}
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
    %{query | operations: query.operations ++ [%{type: :offset, assocs: [], args: [value]}]}
  end

  def offset(ecto_query, value) do
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
    %{query | operations: [%{type: :left_join, assocs: assoc_fields, join_filters: [List.wrap(filters), List.wrap(or_filters)]} | query.operations]}
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
