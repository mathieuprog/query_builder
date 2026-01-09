# Query Builder

QueryBuilder is a thin layer over Ecto that builds composable queries from plain Elixir data structures.

## Index

- [Key Features](#key-features)
- [Setup](#setup)
- [Feature Overview](#feature-overview)
- [Examples](#examples)

## Key Features

### Controller/Resolver‑Driven Query Options

Controllers/GraphQL resolvers can pass filter/sort/page options into a single context list function via `from_opts/2`, without creating new context functions per option or writing custom option-handling logic in the context.

```elixir
def list_users(opts \\ []) do
  User
  |> QueryBuilder.where(deleted: false)
  |> QueryBuilder.from_opts(opts)
  |> Repo.all()
end

# controller/resolver
list_users(where: [name: "Alice"], order_by: [desc: :inserted_at], limit: 50)
```

For optional params, use `maybe_where/*` / `maybe_order_by/*` to conditionally apply clauses.

```elixir
def list_users(opts \\ []) do
  include_deleted? = Keyword.get(opts, :include_deleted?, false)
  qb_opts = Keyword.drop(opts, [:include_deleted?])

  User
  |> QueryBuilder.maybe_where(not include_deleted?, deleted: false)
  |> QueryBuilder.maybe_order_by(not Keyword.has_key?(qb_opts, :order_by), desc: :inserted_at, desc: :id)
  |> QueryBuilder.from_opts(qb_opts)
  |> Repo.all()
end
```

### Data‑Driven Query Composition

QueryBuilder lets you express complex filtering and composition as plain Elixir data, without positional binding gymnastics and without manually building `dynamic/2` trees for the common cases.

QueryBuilder lets you express “OR of AND groups” directly as nested lists:

```elixir
# (name == "Alice") OR (name == "Bob" AND deleted == false)
or_groups = [[name: "Alice"], [name: "Bob", deleted: false]]

User
|> QueryBuilder.where(active: true)
|> QueryBuilder.where_any(or_groups)
|> Repo.all()
```

In Ecto, when the OR groups come from runtime data (e.g. controller params), you typically have to reduce them into a `dynamic/2` expression:

```elixir
# (name == "Alice") OR (name == "Bob" AND deleted == false)
or_groups = [[name: "Alice"], [name: "Bob", deleted: false]]

or_dynamic =
  Enum.reduce(or_groups, dynamic([u], false), fn group, or_acc ->
    and_dynamic =
      Enum.reduce(group, dynamic([u], true), fn {field, value}, and_acc ->
        dynamic([u], ^and_acc and field(u, ^field) == ^value)
      end)

    dynamic([u], ^or_acc or ^and_dynamic)
  end)

User
|> where([u], u.active == true)
|> where(^or_dynamic)
```

### Assoc Queries Without Binding Boilerplate

QueryBuilder lets you reference association fields with `@` tokens (e.g. `:name@role`) instead of manually writing joins and positional binding lists.

```elixir
User
|> QueryBuilder.order_by(:role, asc: :name@role, asc: :nickname)
|> Repo.all()
```

Ecto:

```elixir
User
|> join(:left, [u], r in assoc(u, :role))
|> order_by([u, r], asc: r.name, asc: u.nickname)
|> Repo.all()
```

### Rich Filter DSL (Operators + Field Comparisons)

Beyond `{field, value}` equality, you can use `{field, operator, value}` for common operators (ranges, membership, text search). For field-to-field comparisons, use the `@self` marker as the value.

```elixir
nickname_query = "admin"

filters = [
  {:nickname, :contains, nickname_query, [case: :i]},
  {:inserted_at, :ge, from},
  {:id, :in, ids}
]

User
|> QueryBuilder.where(filters)
|> Repo.all()
```

```elixir
User
|> QueryBuilder.where_exists_subquery([authored_articles: :comments],
  scope: [],
  where: [
    {:body@comments, :contains, :nickname@self, [case: :insensitive]}
  ]
)
|> Repo.all()
```

### Keyset/Cursor-Based Pagination

`paginate/3` returns an opaque cursor derived from your `order_by`; pass it back unchanged to fetch the next/previous page.

```elixir
# First page (no cursor)
pagination_opts = [page_size: 10]

%{paginated_entries: users, pagination: page} =
  User
  |> QueryBuilder.order_by(asc: :nickname, desc: :email)
  |> QueryBuilder.paginate(Repo, pagination_opts)

# Next page: pass back the opaque cursor returned in pagination
pagination_opts =
  Keyword.merge(pagination_opts,
    cursor: page.cursor_for_entries_after,
    direction: :after
  )

%{paginated_entries: next_users, pagination: next_page} =
  User
  |> QueryBuilder.order_by(asc: :nickname, desc: :email)
  |> QueryBuilder.paginate(Repo, pagination_opts)
```

### Higher‑Level Query Helpers

QueryBuilder also includes higher-level helpers that are verbose to write correctly in raw Ecto.

```elixir
alias QueryBuilder, as: QB

# Latest child row per parent
User
|> QB.left_join_latest(:authored_articles, order_by: [desc: :inserted_at, desc: :id])
|> Repo.all()
# => [{%User{}, %Article{} | nil}, ...]

# Top N rows per group
Post
|> QB.top_n_per(partition_by: [:subreddit_id], order_by: [desc: :score, desc: :id], n: 3)
|> Repo.all()
```

### Custom, User-Defined Query Operations (Extension)

`QueryBuilder.Extension` lets you build an app-specific “QB module” that adds your own query operations on top of QueryBuilder.

```elixir
defmodule MyApp.QB do
  use QueryBuilder.Extension, from_opts_full_ops: [:where_initcap]
  import Ecto.Query

  def where_initcap(query, field, value) do
    where(query, fn resolve ->
      {field, binding} = resolve.(field)
      dynamic([{^binding, x}], fragment("initcap(?)", field(x, ^field)) == ^value)
    end)
  end
end

# trusted/internal (full mode)
alias MyApp.QB

MyApp.User
|> QB.where_initcap(:name, "Alice")
|> Repo.all()
```

## Setup

Add `query_builder` as a dependency:

```elixir
def deps do
  [
    {:query_builder, "~> 2.0.0"}
  ]
end
```

## Feature Overview

### Operations

- Filtering: `where/*`, `where_any/*`, `maybe_where/*`
- Sorting: `order_by/*`, `maybe_order_by/*`
- Offset pagination: `limit/2`, `offset/2`
- Keyset pagination: `paginate/3` (cursor-based)
- Joins: `inner_join/2`, `left_join/4`, `left_join_leaf/4`, `left_join_path/4`
- To-many existence filters: `where_exists_subquery/3`, `where_not_exists_subquery/3`, `where_has/3`, `where_missing/3`
- Preloads: `preload_separate/2`, `preload_separate_scoped/3`, `preload_through_join/2`
- Selection & distinctness: `select/*`, `select_merge/*`, `distinct/*`, `distinct_roots/1` (Postgres-only)
- Grouping & aggregates: `group_by/*`, `having/*`, `having_any/*`, aggregates (`count/*`, `count_distinct/1`, `avg/1`, `sum/1`, `min/1`, `max/1`, `array_agg/*` (Postgres-only))
- Postgres query patterns: `top_n_per/*`, `first_per/*`, `left_join_latest/3`, `left_join_top_n/3`

### Tokens, assoc paths, and join intent

- Tokens are atoms/strings: `:field`, `:field@assoc`, or full paths like `:field@assoc@nested_assoc...`.
- `field@assoc` is shorthand and raises if `@assoc` is ambiguous; use a full-path token to disambiguate.
- Assoc paths (`assoc_fields`) support join markers:
  - `:role` (neutral): reuse an existing join qualifier if already joined; otherwise QueryBuilder defaults to `LEFT`
  - `:role?`: force `LEFT`
  - `:role!`: force `INNER`
- `@self` marks field-to-field comparisons (e.g. `{:inserted_at@comments, :gt, :inserted_at@self}`).

### `from_opts`

- `from_opts/2` defaults to boundary mode (for controllers/resolvers): allowlists `where`, `where_any`, `order_by`, `limit`, `offset`.
- `from_opts/3` with `mode: :full` enables the full QueryBuilder surface (use when the caller knows the base query’s implementation/shape).
- `args/*` wraps multiple arguments for `from_opts(..., mode: :full)` (e.g. calling `where/4`, `order_by/3`, `select/3`, or extension ops).

### Extensions

- `QueryBuilder.Extension` lets you define an app-specific module that wraps QueryBuilder and adds custom operations you can call directly.
- If you use `from_opts` on that module, you must explicitly allowlist which custom operations are callable via `from_opts_full_ops: [...]` (full mode) and optionally `boundary_ops_user_asserted: [...]` (boundary mode).

### Utilities

- `new/1`: wrap an existing Ecto queryable into a `%QueryBuilder.Query{}`.
- `subquery/2`: build an `Ecto.SubQuery` using QueryBuilder operations (`from_opts(..., mode: :full)` + `Ecto.Query.subquery/1`).
- `default_page_size/0`: reads `config :query_builder, :default_page_size`.

## Examples

### Filter “has related rows” (to-many) without duplicates

Filter root rows through a to-many association via correlated `EXISTS(...)` without join-multiplying roots.

```elixir
alias QueryBuilder, as: QB

User
|> QB.where_has(:authored_articles, published@authored_articles: true)
|> Repo.all()
```

### Filter “missing related rows” (to-many)

Filter root rows through a to-many association via correlated `NOT EXISTS(...)`.

```elixir
alias QueryBuilder, as: QB

User
|> QB.where_missing(:authored_articles)
|> Repo.all()
```

### Ensure unique roots after joining a to-many association (Postgres)

When you must join a to-many association and still want unique root rows (especially with `limit/offset`), use `distinct_roots/1`.

```elixir
alias QueryBuilder, as: QB

User
|> QB.left_join(:authored_articles)
|> QB.order_by(asc: :id)
|> QB.order_by(:authored_articles, desc: :inserted_at@authored_articles, desc: :id@authored_articles)
|> QB.distinct_roots()
|> QB.offset(20)
|> QB.limit(10)
|> Repo.all()
```

### Scoped separate preload (Ecto query-preload equivalent)

Preload a direct association with an explicit scope using a separate query (`preload_separate_scoped/3`).

```elixir
alias QueryBuilder, as: QB

User
|> QB.preload_separate_scoped(:authored_articles,
  where: [published: true],
  order_by: [desc: :inserted_at]
)
|> Repo.all()
```

### Join-scoped preload (preload only joined rows)

Preload an association *through its join binding* so preloaded rows reflect the join (including join `on:` filters).

```elixir
alias QueryBuilder, as: QB

User
|> QB.left_join(:authored_articles, published@authored_articles: true)
|> QB.preload_through_join(:authored_articles)
|> Repo.all()
```

### Nested join semantics: LEFT every hop vs INNER path + LEFT leaf

Choose whether intermediate hops in a nested path are `INNER` (`left_join_leaf/4`) or `LEFT` (`left_join_path/4`).

```elixir
alias QueryBuilder, as: QB

# INNER authored_articles, LEFT comments
q1 = User |> QB.left_join_leaf([authored_articles: :comments])

# LEFT authored_articles, LEFT comments
q2 = User |> QB.left_join_path([authored_articles: :comments])
```

### Grouping + HAVING with aggregate helpers

Group and filter groups using `group_by/*` + `having/*` with aggregate helpers like `count/0`.

```elixir
alias QueryBuilder, as: QB

User
|> QB.group_by(:role, :name@role)
|> QB.having([{QB.count(:id), :gt, 10}])
|> QB.select({:name@role, QB.count(:id)})
|> Repo.all()
```

### `array_agg` with `DISTINCT`, `ORDER BY`, and `FILTER` (Postgres)

Build grouped results with Postgres aggregates like `array_agg` (including `FILTER (WHERE ...)`).

```elixir
alias QueryBuilder, as: QB

Article
|> QB.group_by(:author_id)
|> QB.select(%{
  author_id: :author_id,
  publisher_ids:
    QB.array_agg(:publisher_id,
      distinct?: true,
      order_by: [asc: :publisher_id],
      filter: [{:publisher_id, :ne, nil}]
    )
})
|> Repo.all()
```

### Build an `IN (subquery)` using QueryBuilder ops

Use `subquery/2` to build an `Ecto.SubQuery` from QueryBuilder options and use it in filters.

```elixir
alias QueryBuilder, as: QB

active_user_ids =
  QB.subquery(User,
    where: [active: true],
    select: :id
  )

Article
|> QB.where({:author_id, :in, active_user_ids})
|> Repo.all()
```

### Top N children per parent (Postgres, LATERAL)

Fetch up to N association rows per parent via `LEFT JOIN LATERAL` and group `{parent, child}` rows in Elixir.

```elixir
alias QueryBuilder, as: QB

rows =
  User
  |> QB.left_join_top_n(:authored_articles, n: 3, order_by: [desc: :inserted_at, desc: :id])
  |> Repo.all()

top_articles_by_user_id =
  Enum.group_by(rows, fn {u, _a} -> u.id end, fn {_u, a} -> a end)
```
