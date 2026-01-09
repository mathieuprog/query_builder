# Changelog

## 2.0.0

### Breaking changes

- The `:assoc_fields` option has been removed from `use QueryBuilder` (association bindings are now generated automatically).
- `from_list/2` was renamed to `from_opts/2`.
- `from_opts/2` defaults to boundary mode (only `where`, `where_any`, `order_by`, `limit`, `offset` are allowed); use `from_opts/3` with `mode: :full` for the full surface.
- `preload/2` is removed; use `preload_separate/2`, `preload_separate_scoped/3`, or `preload_through_join/2`.
- Implicit association joins default to `LEFT` (use `inner_join/2` or `:assoc!` markers when you need `INNER`).
- `left_join/4` no longer accepts nested association paths; use `left_join_leaf/4` or `left_join_path/4`.
- Removed the authorizer hook.

**Why boundary `from_opts/2` disallows preloads**

`from_opts/2` exists mainly so external callers (controllers/resolvers) can pass query options like filtering, sorting, and pagination.

Preloads are excluded because letting an external caller request preloads is unsafe and can yield unexpected association data: what gets loaded can depend on how the context function built the base query (joins vs EXISTS, scoped joins, etc.). In other words, callers would need to know the context function’s implementation details to know whether a given preload is safe and what it will actually return.

Example (EXISTS root filter + preload overfetch):

Assume Alice has:
- Article A: tenant_id=1, published=true
- Article B: tenant_id=1, published=false
- Article C: tenant_id=2, published=true

Context wants: “users who have a published article in tenant 1”:

```elixir
def list_users_with_published_article(tenant_id, opts \\ []) do
  User
  |> QueryBuilder.where_exists_subquery(:authored_articles,
    scope: [],
    where: [tenant_id@authored_articles: tenant_id, published@authored_articles: true]
  )
  |> QueryBuilder.from_opts(opts)
  |> Repo.all()
end
```

If boundary allowed to preload `:authored_articles`, this call would be surprising / potentially unsafe, because a preload would run a second query like “load all authored_articles for these users”, and that query does not inherit the EXISTS predicate. So Alice would be returned with A, B, and C (including unpublished and other-tenant rows). The root query only guarantees “at least one matching article exists”, not “the preloaded association is limited to `tenant_id = 1` and `published = true`”, so the result depends on how the context query was implemented.

Alternative: expose explicit `include` options handled inside the context (strategy + scope is part of the context contract):

```elixir
def list_users(tenant_id, opts \\ []) do
  include = Keyword.get(opts, :include, [])
  qb_opts = Keyword.drop(opts, [:include])

  query =
    User
    |> QueryBuilder.where(tenant_id: tenant_id)
    |> QueryBuilder.from_opts(qb_opts)

  query =
    if :role in include do
      QueryBuilder.preload_separate(query, :role)
    else
      query
    end

  Repo.all(query)
end

def list_users_with_published_article(tenant_id, opts \\ []) do
  include = Keyword.get(opts, :include, [])
  qb_opts = Keyword.drop(opts, [:include])

  query =
    User
    |> QueryBuilder.where_exists_subquery(:authored_articles,
      scope: [],
      where: [tenant_id@authored_articles: tenant_id, published@authored_articles: true]
    )
    |> QueryBuilder.from_opts(qb_opts)

  if :authored_articles in include do
    QueryBuilder.preload_separate_scoped(query, :authored_articles,
      where: [tenant_id: tenant_id, published: true],
      order_by: [desc: :inserted_at]
    )
    |> Repo.all()
  else
    Repo.all(query)
  end
end
```

If the association selection depends on parent fields (correlated predicates), use join-preload explicitly:

```elixir
def list_users_having_featured_article(tenant_id, opts \\ []) do
  include = Keyword.get(opts, :include, [])
  qb_opts = Keyword.drop(opts, [:include])

  featured_filters = [
    tenant_id@authored_articles: tenant_id,
    published@authored_articles: true,
    title@authored_articles: :nickname@self
  ]

  base =
    User
    |> QueryBuilder.where(tenant_id: tenant_id)
    |> QueryBuilder.from_opts(qb_opts)

  if :featured_authored_articles in include do
    base
    |> QueryBuilder.inner_join(:authored_articles)
    |> QueryBuilder.where(:authored_articles, featured_filters)
    |> QueryBuilder.preload_through_join(:authored_articles)
    |> Repo.all()
  else
    base
    |> QueryBuilder.where_exists_subquery(:authored_articles,
      scope: [],
      where: featured_filters
    )
    |> Repo.all()
  end
end
```

**Why did implicit associations change from INNER to LEFT?**

When QueryBuilder needs to join an association implicitly (because you reference assoc fields via `field@assoc`), it now uses LEFT JOIN by default. Previously, implicit INNER JOIN could silently drop root rows for optional associations; especially surprising with OR logic (`where_any/*` / `having_any/*`).

Example:

```elixir
# Alice has no role; Bob has role "admin"
User
|> QB.where_any([[name: "Alice"], [name@role: "admin"]])
|> Repo.all()

# previously => [Bob]        (Alice was dropped by the implicit INNER JOIN)
# now        => [Alice, Bob]
```

If you need “association must exist”, make it explicit with `inner_join/2` or a join marker like `:role!`.

### New features

- `where_exists_subquery/3` / `where_not_exists_subquery/3`: filter roots through associations using correlated `EXISTS`/`NOT EXISTS`.
- `where_has/3` / `where_missing/3`: shorthands for common `EXISTS`/`NOT EXISTS` cases.
- `distinct_roots/1`: de-duplicate root rows by primary key (Postgres `DISTINCT ON`).
- `array_agg/1` / `array_agg/2`: Postgres `array_agg` aggregate (supports `DISTINCT` + `ORDER BY` + `FILTER`).
- `top_n_per/2` / `first_per/2`: top N (or first) row per group.
- `left_join_latest/3`: left-join the latest `has_many` row per parent and select `{root, assoc}`.
- `left_join_top_n/3`: left-join the top N `has_many` rows per parent and select `{root, assoc}`.
- Full-path tokens (`field@assoc@nested_assoc...`): disambiguate in case of ambiguity.
- Join markers (`:assoc?` / `:assoc!`): declare optional vs required association joins (`LEFT` vs `INNER`).

### Bug fixes

A large number of bugs and edge cases have been fixed.
