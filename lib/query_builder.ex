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

  # ----------------------------------------------------------------------------
  # Pagination
  # ----------------------------------------------------------------------------

  def paginate(%QueryBuilder.Query{} = query, repo, opts \\ []) do
    paginate_cursor(query, repo, opts)
  end

  def paginate_cursor(%QueryBuilder.Query{} = query, repo, opts \\ []) do
    QueryBuilder.Pagination.paginate_cursor(query, repo, opts)
  end

  def paginate_offset(%QueryBuilder.Query{} = query, repo, opts \\ []) do
    QueryBuilder.Pagination.paginate_offset(query, repo, opts)
  end

  def default_page_size() do
    Application.get_env(:query_builder, :default_page_size, 100)
  end

  # ----------------------------------------------------------------------------
  # Preloads
  # ----------------------------------------------------------------------------

  @doc ~S"""
  Preloads associations using *separate* queries (Ecto's default preload behavior).

  This always performs query-preload, even if the association is joined in SQL.

  Example:
  ```
  QueryBuilder.preload_separate(query, [role: :permissions, articles: [:stars, comments: :user]])
  ```
  """
  def preload_separate(%QueryBuilder.Query{} = query, assoc_fields) do
    QueryBuilder.PreloadOps.preload_separate(query, assoc_fields)
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
    QueryBuilder.PreloadOps.preload_separate_scoped(query, assoc_field, opts)
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
    QueryBuilder.PreloadOps.preload_through_join(query, assoc_fields)
  end

  def preload_through_join(ecto_query, assoc_fields) do
    ecto_query = ensure_query_has_binding(ecto_query)
    preload_through_join(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields)
  end

  # ----------------------------------------------------------------------------
  # Filtering
  # ----------------------------------------------------------------------------

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
          {:where, assoc_fields, [filters, or_filters]} | query.operations
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
    or_groups =
      QueryBuilder.Filters.normalize_or_groups!(
        or_groups,
        :where_any,
        "where_any/2 and where_any/3"
      )

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

  # ----------------------------------------------------------------------------
  # Selecting
  # ----------------------------------------------------------------------------

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

  Note: `paginate_cursor/3` / `paginate_offset/3` require selecting the root struct; using `select/*` will make
  pagination raise (fail-fast).
  """
  def select(query, selection) do
    select(query, [], selection)
  end

  def select(query, assoc_fields, selection)

  def select(%QueryBuilder.Query{} = query, assoc_fields, selection) do
    if Enum.any?(query.operations, fn
         {type, _assocs, _args}
         when type in [:select, :select_merge, :left_join_latest, :left_join_top_n] ->
           true

         _ ->
           false
       end) do
      raise ArgumentError,
            "only one select expression is allowed in query; " <>
              "call `select/*` at most once and use `select_merge/*` to add fields"
    end

    %{
      query
      | operations: [{:select, assoc_fields, [selection]} | query.operations]
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
  - `paginate_cursor/3` / `paginate_offset/3` require selecting the root struct; any custom select expression
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
    if Enum.any?(query.operations, fn
         {type, _assocs, _args} when type in [:left_join_latest, :left_join_top_n] -> true
         _ -> false
       end) do
      raise ArgumentError,
            "select_merge/* cannot be combined with left_join_latest/3 or left_join_top_n/3; " <>
              "these functions set a custom select (`{root, assoc}`), so select_merge is not supported"
    end

    %{
      query
      | operations: [
          {:select_merge, assoc_fields, [selection]} | query.operations
        ]
    }
  end

  def select_merge(ecto_query, assoc_fields, selection) do
    ecto_query = ensure_query_has_binding(ecto_query)
    select_merge(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, selection)
  end

  # ----------------------------------------------------------------------------
  # Aggregates
  # ----------------------------------------------------------------------------

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
  Aggregate helper: `array_agg` (Postgres-only).

  This returns an aggregate expression that can be used in `select/*`, `order_by/*`,
  and `having/*` (typically with `group_by/*`).

  Options:
  - `:distinct?` (optional) - boolean (default: `false`)
  - `:order_by` (optional) - a keyword list like `order_by/*` (default: `[]`).
    When `distinct?: true`, `order_by` is restricted to ordering by the aggregated token
    itself (Postgres restriction for `DISTINCT` aggregates).
    Supports up to 5 order terms.
  - `:filter` (optional) - a filter DSL value (like `where/*`), an Ecto dynamic, or a 1-arity
    function that returns a dynamic. This is compiled into `FILTER (WHERE ...)` (Postgres).
    The DSL form is AND-only; for OR logic, use a dynamic/function.

  Examples:

  ```elixir
  Article
  |> QueryBuilder.group_by(:author_id)
  |> QueryBuilder.select(%{
    author_id: :author_id,
    publisher_ids: QueryBuilder.array_agg(:publisher_id, distinct?: true, order_by: [asc: :publisher_id])
  })
  |> Repo.all()
  ```
  """
  def array_agg(token, opts \\ [])

  def array_agg(token, opts) when (is_atom(token) or is_binary(token)) and is_list(opts) do
    validate_array_agg_opts!(token, opts)

    distinct? = Keyword.get(opts, :distinct?, false)
    order_by = Keyword.get(opts, :order_by, [])
    filter = Keyword.get(opts, :filter, nil)

    modifier = if distinct?, do: :distinct, else: nil

    %QueryBuilder.Aggregate{
      op: :array_agg,
      arg: token,
      modifier: modifier,
      order_by: order_by,
      filter: filter
    }
  end

  def array_agg(token, opts) do
    raise ArgumentError,
          "array_agg/2 expects a token (atom/string) and a keyword list, got: " <>
            "#{inspect(token)}, #{inspect(opts)}"
  end

  defp validate_array_agg_opts!(token, opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "array_agg/2 expects `opts` to be a keyword list, got: #{inspect(opts)}"
    end

    allowed_keys = [:distinct?, :order_by, :filter]
    unknown_keys = opts |> Keyword.keys() |> Enum.uniq() |> Kernel.--(allowed_keys)

    if unknown_keys != [] do
      raise ArgumentError,
            "array_agg/2 got unknown options: #{inspect(unknown_keys)} (supported: #{inspect(allowed_keys)})"
    end

    distinct? = Keyword.get(opts, :distinct?, false)

    unless is_boolean(distinct?) do
      raise ArgumentError,
            "array_agg/2 expects `:distinct?` to be a boolean, got: #{inspect(distinct?)}"
    end

    order_by = Keyword.get(opts, :order_by, [])

    if is_nil(order_by) do
      raise ArgumentError,
            "array_agg/2 expects `:order_by` to be a keyword list (or list of order expressions), got nil"
    end

    if order_by != [] and not (is_list(order_by) and Keyword.keyword?(order_by)) do
      raise ArgumentError,
            "array_agg/2 expects `:order_by` to be a keyword list (or list of order expressions), got: #{inspect(order_by)}"
    end

    if length(order_by) > 5 do
      raise ArgumentError,
            "array_agg/2 supports up to 5 order_by terms, got: #{inspect(length(order_by))}"
    end

    Enum.each(List.wrap(order_by), fn
      {direction, expr} when is_atom(direction) ->
        if distinct? and
             not ((is_atom(expr) or is_binary(expr)) and to_string(expr) == to_string(token)) do
          raise ArgumentError,
                "array_agg/2 with `distinct?: true` requires `order_by` expressions to match the aggregated token " <>
                  "(Postgres restriction for DISTINCT aggregates); got: #{inspect(expr)} (expected: #{inspect(token)})"
        end

        :ok

      other ->
        raise ArgumentError,
              "array_agg/2 expects `:order_by` entries to look like `{direction, expr}`, got: #{inspect(other)}"
    end)

    filter = Keyword.get(opts, :filter, nil)
    validate_array_agg_filter_opt!(filter)

    :ok
  end

  defp validate_array_agg_filter_opt!(nil), do: :ok
  defp validate_array_agg_filter_opt!([]), do: :ok

  defp validate_array_agg_filter_opt!(%Ecto.Query.DynamicExpr{}), do: :ok

  defp validate_array_agg_filter_opt!(fun) when is_function(fun, 1), do: :ok

  defp validate_array_agg_filter_opt!(filters) when is_list(filters) do
    if Keyword.keyword?(filters) and Keyword.has_key?(filters, :or) do
      raise ArgumentError,
            "array_agg/2 filter DSL is AND-only (does not support `or:` groups); " <>
              "use `filter: dynamic(...)` or `filter: fn resolve -> ... end` for OR logic"
    end

    Enum.each(filters, fn
      {:or, _} ->
        raise ArgumentError,
              "array_agg/2 filter DSL is AND-only (does not support `{:or, ...}` groups); " <>
                "use `filter: dynamic(...)` or `filter: fn resolve -> ... end` for OR logic"

      _ ->
        :ok
    end)

    :ok
  end

  defp validate_array_agg_filter_opt!(filter) when is_tuple(filter) do
    if tuple_size(filter) > 0 and elem(filter, 0) == :or do
      raise ArgumentError,
            "array_agg/2 filter DSL is AND-only (does not support `{:or, ...}` groups); " <>
              "use `filter: dynamic(...)` or `filter: fn resolve -> ... end` for OR logic"
    end

    :ok
  end

  defp validate_array_agg_filter_opt!(other) do
    raise ArgumentError,
          "array_agg/2 expects `:filter` to be a keyword list, a list of filters, a filter tuple, a dynamic, or a 1-arity function; " <>
            "got: #{inspect(other)}"
  end

  # ----------------------------------------------------------------------------
  # Exists / Has Helpers
  # ----------------------------------------------------------------------------

  @doc ~S"""
  A shorthand for `where_exists_subquery/3` (“has associated rows”).

  It applies a correlated `EXISTS(...)` filter for the given association path.
  `filters` are AND-ed inside the subquery.
  Filter fields must be explicit association tokens (contain `@`), because the
  predicate runs inside the association subquery.

  Examples:

  ```elixir
  # Equivalent to where_exists_subquery(:authored_articles, where: [published@authored_articles: true], scope: [])
  User |> QueryBuilder.where_has(:authored_articles, published@authored_articles: true)
  ```

  ```elixir
  User |> QueryBuilder.where_has([authored_articles: :comments], title@comments: "It's great!")
  ```
  """
  def where_has(query, assoc_fields, filters \\ [])

  def where_has(%QueryBuilder.Query{} = query, assoc_fields, filters) do
    if assoc_fields in [nil, []] do
      raise ArgumentError,
            "where_has/3 requires a non-empty assoc_fields argument " <>
              "(e.g. `where_has(:comments, ...)` or `where_has([articles: :comments], ...)`)"
    end

    if is_nil(filters) do
      raise ArgumentError,
            "where_has/3 expects filters to be a keyword list (or a list of filters); got nil"
    end

    filters = List.wrap(filters)
    validate_where_has_filters!(filters, "where_has/3")

    where_exists_subquery(query, assoc_fields, where: filters, scope: [])
  end

  def where_has(ecto_query, assoc_fields, filters) do
    ecto_query = ensure_query_has_binding(ecto_query)
    where_has(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, filters)
  end

  @doc ~S"""
  A shorthand for `where_not_exists_subquery/3` (“missing associated rows”).

  This is the `NOT EXISTS(...)` counterpart to `where_has/3`.
  Filter fields must be explicit association tokens (contain `@`), because the
  predicate runs inside the association subquery.
  """
  def where_missing(query, assoc_fields, filters \\ [])

  def where_missing(%QueryBuilder.Query{} = query, assoc_fields, filters) do
    if assoc_fields in [nil, []] do
      raise ArgumentError,
            "where_missing/3 requires a non-empty assoc_fields argument " <>
              "(e.g. `where_missing(:comments, ...)` or `where_missing([articles: :comments], ...)`)"
    end

    if is_nil(filters) do
      raise ArgumentError,
            "where_missing/3 expects filters to be a keyword list (or a list of filters); got nil"
    end

    filters = List.wrap(filters)
    validate_where_has_filters!(filters, "where_missing/3")

    where_not_exists_subquery(query, assoc_fields, where: filters, scope: [])
  end

  def where_missing(ecto_query, assoc_fields, filters) do
    ecto_query = ensure_query_has_binding(ecto_query)
    where_missing(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, filters)
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

  Tuple filters inside the subquery must target association fields (use `field@assoc`).
  Root-field filters belong on the outer query. For per-parent comparisons, use
  field-to-field values via the `@self` marker (e.g. `{:inserted_at@articles, :gt, :inserted_at@self}`).

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

    validate_exists_subquery_filters!(
      scope,
      "where_exists_subquery/3 does not allow root field filters (no `@`) in `scope:`"
    )

    validate_exists_subquery_filters!(
      where_filters,
      "where_exists_subquery/3 does not allow root field filters (no `@`) in `where:`"
    )

    where_any_groups =
      case where_any do
        :__missing__ ->
          []

        other ->
          QueryBuilder.Filters.normalize_or_groups!(other, :where_any, "where_exists_subquery/3")
      end

    where_any_groups = Enum.reject(where_any_groups, &(&1 == []))

    Enum.each(where_any_groups, fn group ->
      validate_exists_subquery_filters!(
        group,
        "where_exists_subquery/3 does not allow root field filters (no `@`) in `where_any:`"
      )
    end)

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
          {:where_exists_subquery, [],
           [assoc_fields, effective_scope_filters, predicate_filters, predicate_or_filters]}
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
        :__missing__ ->
          []

        other ->
          QueryBuilder.Filters.normalize_or_groups!(
            other,
            :where_any,
            "where_not_exists_subquery/3"
          )
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
          {:where_not_exists_subquery, [],
           [assoc_fields, effective_scope_filters, predicate_filters, predicate_or_filters]}
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

  # ----------------------------------------------------------------------------
  # Distinct
  # ----------------------------------------------------------------------------

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
      | operations: [{:distinct, assoc_fields, [value]} | query.operations]
    }
  end

  def distinct(ecto_query, assoc_fields, value) do
    ecto_query = ensure_query_has_binding(ecto_query)
    distinct(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, value)
  end

  @doc ~S"""
  Ensures the query returns unique root rows (dedupe by root primary key).

  This is primarily useful when you must join a to-many association (e.g. for
  filtering/order_by) but still want a unique list of root structs.

  Postgres-only: uses `DISTINCT ON (root_pk...)` (via Ecto distinct expressions).
  In join-multiplying queries, `order_by` determines which joined row “wins” for
  each root.

  Notes:
  - Requires the root schema to have a primary key.
  - Requires a database that supports `DISTINCT ON` (Postgres).
  - Cannot be combined with `preload_through_join` on to-many associations (it
    would drop association rows).
  """
  def distinct_roots(query, enabled \\ true)

  def distinct_roots(%QueryBuilder.Query{} = query, true) do
    %{
      query
      | operations: [{:distinct_roots, [], []} | query.operations]
    }
  end

  def distinct_roots(%QueryBuilder.Query{} = query, false), do: query
  def distinct_roots(%QueryBuilder.Query{} = query, []), do: query

  def distinct_roots(%QueryBuilder.Query{}, nil) do
    raise ArgumentError, "distinct_roots/2 does not accept nil; omit the call or pass true/false"
  end

  def distinct_roots(%QueryBuilder.Query{}, enabled) do
    raise ArgumentError,
          "distinct_roots/2 expects a boolean (or [] as a no-op), got: #{inspect(enabled)}"
  end

  def distinct_roots(ecto_query, enabled) do
    ecto_query = ensure_query_has_binding(ecto_query)
    distinct_roots(%QueryBuilder.Query{ecto_query: ecto_query}, enabled)
  end

  # ----------------------------------------------------------------------------
  # Grouping & Having
  # ----------------------------------------------------------------------------

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
      | operations: [{:group_by, assoc_fields, [expr]} | query.operations]
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
          {:having, assoc_fields, [filters, or_filters]} | query.operations
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
    or_groups =
      QueryBuilder.Filters.normalize_or_groups!(
        or_groups,
        :having_any,
        "having_any/2 and having_any/3"
      )

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
      | operations: [{:order_by, assoc_fields, [value]} | query.operations]
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
    %{query | operations: [{:limit, [], [value]} | query.operations]}
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
    %{query | operations: [{:offset, [], [value]} | query.operations]}
  end

  def offset(ecto_query, value) do
    ecto_query = ensure_query_has_binding(ecto_query)
    offset(%QueryBuilder.Query{ecto_query: ecto_query}, value)
  end

  # ----------------------------------------------------------------------------
  # Ranking Helpers (Postgres)
  # ----------------------------------------------------------------------------

  @doc ~S"""
  Keeps the top N rows per group (window-function helper).

  This ranks rows with `row_number() OVER (PARTITION BY ... ORDER BY ...)` and
  filters to `rn <= n`, while returning the original root rows.

  Postgres-only: uses window functions for `n > 1`; uses `DISTINCT ON` for `n: 1`
  when the query has no `distinct`.

  Options:
  - `:partition_by` (required) - a token or list of tokens/expressions
  - `:order_by` (required) - a keyword list like `order_by/*`
  - `:n` (required) - a positive integer
  - `:disable_distinct_on?` (optional) - when `true` and `n: 1`, forces the window-function plan (no `DISTINCT ON`)

  Notes:
  - Requires the root schema to have a primary key (used as a deterministic tie-breaker, and to join back for window-function ranking).
  - `order_by` must include the root primary key fields as a tie-breaker.
  - Must be applied before `order_by/*` (apply the final ordering after `top_n_per/*`).
  - Must be applied before `limit/2` and `offset/2`.

  Examples:
  ```elixir
  # latest order per user
  Order
  |> QueryBuilder.top_n_per(partition_by: [:user_id], order_by: [desc: :created_at, desc: :id], n: 1)
  |> Repo.all()
  ```
  """
  def top_n_per(query, opts) do
    top_n_per(query, [], opts)
  end

  def top_n_per(query, assoc_fields, opts)

  def top_n_per(%QueryBuilder.Query{}, _assoc_fields, nil) do
    raise ArgumentError,
          "top_n_per/2 and top_n_per/3 expect `opts` to be a keyword list, got: nil"
  end

  def top_n_per(%QueryBuilder.Query{} = query, assoc_fields, opts) when is_list(opts) do
    %{
      query
      | operations: [{:top_n_per, assoc_fields, [opts]} | query.operations]
    }
  end

  def top_n_per(%QueryBuilder.Query{}, _assoc_fields, opts) do
    raise ArgumentError,
          "top_n_per/2 and top_n_per/3 expect `opts` to be a keyword list, got: #{inspect(opts)}"
  end

  def top_n_per(ecto_query, assoc_fields, opts) do
    ecto_query = ensure_query_has_binding(ecto_query)
    top_n_per(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, opts)
  end

  @doc ~S"""
  A shorthand for `top_n_per/2` with `n: 1`.

  Accepts the same options as `top_n_per/2` (except `:n` is fixed to 1).

  Postgres-only.

  Example:
  ```elixir
  # one latest post per subreddit
  Post
  |> QueryBuilder.first_per(partition_by: [:subreddit_id], order_by: [desc: :score, desc: :id])
  |> Repo.all()
  ```
  """
  def first_per(query, opts) do
    first_per(query, [], opts)
  end

  def first_per(query, assoc_fields, opts)

  def first_per(_query, _assoc_fields, nil) do
    raise ArgumentError,
          "first_per/2 and first_per/3 expect `opts` to be a keyword list, got: nil"
  end

  def first_per(query, assoc_fields, opts) when is_list(opts) do
    case Keyword.fetch(opts, :n) do
      :error ->
        top_n_per(query, assoc_fields, Keyword.put(opts, :n, 1))

      {:ok, 1} ->
        top_n_per(query, assoc_fields, opts)

      {:ok, other} ->
        raise ArgumentError,
              "first_per/2 is `top_n_per/2` with `n: 1`; got n: #{inspect(other)}"
    end
  end

  def first_per(_query, _assoc_fields, opts) do
    raise ArgumentError,
          "first_per/2 and first_per/3 expect `opts` to be a keyword list, got: #{inspect(opts)}"
  end

  # ----------------------------------------------------------------------------
  # Joins
  # ----------------------------------------------------------------------------

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
    QueryBuilder.JoinOps.inner_join(query, assoc_fields)
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
    QueryBuilder.JoinOps.left_join(query, assoc_fields, filters, or_filters)
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
    QueryBuilder.JoinOps.left_join_leaf(query, assoc_fields, filters, or_filters)
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
    QueryBuilder.JoinOps.left_join_path(query, assoc_fields, filters, or_filters)
  end

  def left_join_path(ecto_query, assoc_fields, filters, or_filters) do
    ecto_query = ensure_query_has_binding(ecto_query)
    left_join_path(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_fields, filters, or_filters)
  end

  # ----------------------------------------------------------------------------
  # LATERAL Join Helpers (Postgres)
  # ----------------------------------------------------------------------------

  @doc ~S"""
  Left-joins the latest row of a `has_many` association and selects `{root, assoc}`.

  This is a helper for “parent rows + latest child row joined” without multiplying
  parent rows.

  Postgres-only: emits `LEFT JOIN LATERAL (...) LIMIT 1`.

  Supported options:
  - `order_by:` (required) - a keyword list like `order_by/2` (applied in the assoc subquery)
  - `where:` (optional) - filters like `where/2` (applied in the assoc subquery)
  - `child_assoc_fields:` (optional) - assoc tree to join inside the assoc subquery (to support tokens like `field@assoc`)

  Notes:
  - Only supports a single, direct `has_many` association (no nested paths).
  - The `order_by:` must include the assoc schema primary key fields as a tie-breaker.
  - This sets a custom select (`{root, assoc}`), so it cannot be used with `paginate_cursor/3` or `paginate_offset/3`.

  Example:
  ```elixir
  User
  |> QueryBuilder.order_by(asc: :id)
  |> QueryBuilder.left_join_latest(:authored_articles, order_by: [desc: :inserted_at, desc: :id])
  |> Repo.all()
  # => [{%User{}, %Article{} | nil}, ...]
  ```
  """
  def left_join_latest(query, assoc_field, opts \\ [])

  def left_join_latest(_query, nil, _opts) do
    raise ArgumentError, "left_join_latest/3 expects an association field, got nil"
  end

  def left_join_latest(_query, _assoc_field, nil) do
    raise ArgumentError, "left_join_latest/3 expects opts to be a keyword list, got nil"
  end

  def left_join_latest(%QueryBuilder.Query{} = query, assoc_field, opts)
      when is_atom(assoc_field) and is_list(opts) do
    if Enum.any?(query.operations, fn
         {type, _assocs, _args}
         when type in [:select, :select_merge, :left_join_latest, :left_join_top_n] ->
           true

         _ ->
           false
       end) do
      raise ArgumentError,
            "only one select expression is allowed in query; " <>
              "call `left_join_latest/3` at most once and avoid mixing it with `select/*`, `select_merge/*`, or `left_join_top_n/3`"
    end

    %{
      query
      | operations: [
          {:left_join_latest, [], [assoc_field, opts]} | query.operations
        ]
    }
  end

  def left_join_latest(query, assoc_field, opts) when is_atom(assoc_field) and is_list(opts) do
    ecto_query = ensure_query_has_binding(query)
    left_join_latest(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_field, opts)
  end

  def left_join_latest(_query, assoc_field, _opts) do
    raise ArgumentError,
          "left_join_latest/3 expects `assoc_field` to be an atom (direct association), got: #{inspect(assoc_field)}"
  end

  @doc ~S"""
  Left-joins the top N rows of a `has_many` association and selects `{root, assoc}`.

  This is a helper for “parent rows + top N child rows joined”, returning multiple
  `{parent, child}` rows per parent.

  Postgres-only: emits `LEFT JOIN LATERAL (...) LIMIT n`.

  Supported options:
  - `n:` (required) - a positive integer (applied as a `LIMIT` in the assoc subquery)
  - `order_by:` (required) - a keyword list like `order_by/2` (applied in the assoc subquery)
  - `where:` (optional) - filters like `where/2` (applied in the assoc subquery)
  - `child_assoc_fields:` (optional) - assoc tree to join inside the assoc subquery (to support tokens like `field@assoc`)

  Notes:
  - Only supports a single, direct `has_many` association (no nested paths).
  - The `order_by:` must include the assoc schema primary key fields as a tie-breaker.
  - This multiplies parent rows (up to `n` rows per parent).
  - This sets a custom select (`{root, assoc}`), so it cannot be used with `paginate_cursor/3` or `paginate_offset/3`.

  Example:
  ```elixir
  User
  |> QueryBuilder.left_join_top_n(:authored_articles, n: 3, order_by: [desc: :inserted_at, desc: :id])
  |> Repo.all()
  # => [{%User{}, %Article{} | nil}, ...]
  ```
  """
  def left_join_top_n(query, assoc_field, opts \\ [])

  def left_join_top_n(_query, nil, _opts) do
    raise ArgumentError, "left_join_top_n/3 expects an association field, got nil"
  end

  def left_join_top_n(_query, _assoc_field, nil) do
    raise ArgumentError, "left_join_top_n/3 expects opts to be a keyword list, got nil"
  end

  def left_join_top_n(%QueryBuilder.Query{} = query, assoc_field, opts)
      when is_atom(assoc_field) and is_list(opts) do
    if Enum.any?(query.operations, fn
         {type, _assocs, _args}
         when type in [:select, :select_merge, :left_join_latest, :left_join_top_n] ->
           true

         _ ->
           false
       end) do
      raise ArgumentError,
            "only one select expression is allowed in query; " <>
              "call `left_join_top_n/3` at most once and avoid mixing it with `select/*`, `select_merge/*`, or `left_join_latest/3`"
    end

    %{
      query
      | operations: [
          {:left_join_top_n, [], [assoc_field, opts]} | query.operations
        ]
    }
  end

  def left_join_top_n(query, assoc_field, opts) when is_atom(assoc_field) and is_list(opts) do
    ecto_query = ensure_query_has_binding(query)
    left_join_top_n(%QueryBuilder.Query{ecto_query: ecto_query}, assoc_field, opts)
  end

  def left_join_top_n(_query, assoc_field, _opts) do
    raise ArgumentError,
          "left_join_top_n/3 expects `assoc_field` to be an atom (direct association), got: #{inspect(assoc_field)}"
  end

  # ----------------------------------------------------------------------------
  # from_opts
  # ----------------------------------------------------------------------------

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
    QueryBuilder.FromOpts.apply(query, opts, apply_module, from_opts_opts)
  end

  # Migration shim: v1 exposed from_list/2. Keep it to raise with a clear upgrade hint.
  def from_list(_query, _opts) do
    raise ArgumentError,
          "from_list/2 was renamed to from_opts/2; please update your call sites"
  end

  defp validate_exists_subquery_filters!(filters, context) do
    Enum.each(filters, fn
      fun when is_function(fun) ->
        :ok

      {field, value} ->
        validate_exists_subquery_filter_tuple!(field, value, context, {field, value})

      {field, operator, value} ->
        validate_exists_subquery_filter_tuple!(field, value, context, {field, operator, value})

      {field, operator, value, operator_opts} ->
        validate_exists_subquery_filter_tuple!(
          field,
          value,
          context,
          {field, operator, value, operator_opts}
        )

      _other ->
        :ok
    end)
  end

  defp validate_exists_subquery_filter_tuple!(field, value, context, filter_tuple)
       when is_atom(field) or is_binary(field) do
    token = to_string(field)

    if not String.contains?(token, "@") and not exists_subquery_value_is_field?(value) do
      raise ArgumentError,
            "#{context}. Move root filters to the outer query (e.g. `QueryBuilder.where/2`). " <>
              "Got: #{inspect(filter_tuple)}"
    end

    :ok
  end

  defp validate_exists_subquery_filter_tuple!(_field, _value, _context, _filter_tuple), do: :ok

  defp exists_subquery_value_is_field?(val) when val in [nil, false, true], do: false

  defp exists_subquery_value_is_field?(val) when is_atom(val) or is_binary(val) do
    val |> to_string() |> String.ends_with?("@self")
  end

  defp exists_subquery_value_is_field?(_val), do: false

  defp validate_where_has_filters!(filters, context) do
    Enum.each(filters, fn
      fun when is_function(fun) ->
        :ok

      {field, _value} ->
        validate_where_has_field_token!(field, context)

      {field, _operator, _value} ->
        validate_where_has_field_token!(field, context)

      {field, _operator, _value, _operator_opts} ->
        validate_where_has_field_token!(field, context)

      other ->
        raise ArgumentError,
              "#{context} got an invalid filter entry: #{inspect(other)}. " <>
                "Expected `{field, value}`, `{field, operator, value}`, `{field, operator, value, operator_opts}`, " <>
                "or a 1-arity function."
    end)
  end

  defp validate_where_has_field_token!(field, context) when is_atom(field) or is_binary(field) do
    token = to_string(field)

    if not String.contains?(token, "@") do
      raise ArgumentError,
            "#{context} requires explicit association tokens (containing `@`) in filters; " <>
              "got: #{inspect(field)}. " <>
              "Example: `#{token}@assoc: value`."
    end

    :ok
  end

  defp validate_where_has_field_token!(other, context) do
    raise ArgumentError,
          "#{context} expects field tokens to be atoms or strings, got: #{inspect(other)}"
  end

  defp ensure_query_has_binding(%Ecto.Query{} = ecto_query) do
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

  defp ensure_query_has_binding(query) when is_atom(query) do
    if Code.ensure_loaded?(query) and function_exported?(query, :__schema__, 1) do
      query
      |> Ecto.Queryable.to_query()
      |> ensure_query_has_binding()
    else
      raise ArgumentError,
            "expected an Ecto.Queryable (schema module, Ecto.Query, or QueryBuilder.Query), got: #{inspect(query)}"
    end
  end

  defp ensure_query_has_binding(query) do
    if Ecto.Queryable.impl_for(query) do
      query
      |> Ecto.Queryable.to_query()
      |> ensure_query_has_binding()
    else
      raise ArgumentError,
            "expected an Ecto.Queryable (schema module, Ecto.Query, or QueryBuilder.Query), got: #{inspect(query)}"
    end
  end
end
