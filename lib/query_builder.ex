defmodule QueryBuilder do
  require Ecto.Query
  alias Ecto.Query

  defmacro __using__(opts) do
    quote do
      require QueryBuilder.Schema
      QueryBuilder.Schema.__using__(unquote(opts))
    end
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
  def preload(query, assoc_fields) do
    ensure_query_has_binding(query)
    |> QueryBuilder.Query.Preload.preload(assoc_fields)
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
  """
  def where(query, assoc_fields, filters, _opts \\ []) do
    ensure_query_has_binding(query)
    |> QueryBuilder.Query.Where.where(assoc_fields, filters)
  end

  @doc ~S"""
  An OR where query expression.

  Example:
  ```
  QueryBuilder.or_where(query, firstname: "John")
  ```
  """
  def or_where(query, filters) do
    or_where(query, [], filters)
  end

  @doc ~S"""
  An OR where query expression.

  Associations are passed in second argument; fields from these associations can then
  be referenced by writing the field name, followed by the "@" character and the
  association name, as an atom. For example: `:name@users`.

  Example:
  ```
  QueryBuilder.or_where(query, [role: :permissions], name@permissions: :write)
  ```
  """
  def or_where(query, assoc_fields, filters) do
    ensure_query_has_binding(query)
    |> QueryBuilder.Query.Where.where(assoc_fields, filters, :or)
  end

  @doc ~S"""
  An order by query expression.

  Example:
  ```
  QueryBuilder.order_by(query, lastname: :asc, firstname: :asc)
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
  QueryBuilder.order_by(query, :articles, title@articles: :asc)
  ```
  """
  def order_by(query, assoc_fields, value) do
    ensure_query_has_binding(query)
    |> QueryBuilder.Query.OrderBy.order_by(assoc_fields, value)
  end

  @doc ~S"""
  A join query expression.

  Third argument `type` may be passed one of the possible values for
  `Ecto.Query.join/5`'s qualifier argument.

  Example:
  ```
  QueryBuilder.join(query, :articles, :left)
  ```
  """
  def join(query, assoc_fields, type) do
    ensure_query_has_binding(query)
    |> QueryBuilder.Query.Join.join(assoc_fields, type)
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
