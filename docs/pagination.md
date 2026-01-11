# Pagination

QueryBuilder provides two explicit pagination APIs:

- `paginate_cursor/3` (alias: `paginate/3`): cursor/keyset pagination.
- `paginate_offset/3`: offset/row pagination (no cursor).

Both return:

```elixir
%{paginated_entries: [...], pagination: %{...}}
```

Both APIs fetch `page_size + 1` rows in their page query (a lookahead row) to compute `has_more_entries`.

This document calls out:

- **Root queries**: the number of `repo.all/1` calls for the paginated root query.
- **Separate preload queries**: extra queries performed by Ecto/`Repo.preload` for `preload_separate/*` (typically ~1 per assoc path).

Notes:

- Both `paginate_cursor/3` and `paginate_offset/3` require the root schema to have a primary key.
  - If you truly need raw SQL-row pagination for a schema with no primary key, use `limit/2` + `offset/2` directly on an Ecto query.

## Cursor Pagination (`paginate_cursor/3`)

Return shape:

```elixir
%{
  paginated_entries: [...],
  pagination: %{
    cursor_direction: :after | :before,
    cursor_for_entries_before: binary | nil,
    cursor_for_entries_after: binary | nil,
    has_more_entries: boolean,
    max_page_size: pos_integer
  }
}
```

### Key properties

- Requires a primary key on the root schema (for a stable tie-breaker and to reload unique root rows).
- Requires `order_by/*` fields to be cursorable (atoms/strings/tokens like `:name@role`, with supported directions).
- Rejects base Ecto `order_by` clauses (ordering must be expressed via QueryBuilder).
- Rejects custom `select` (must select the root struct).
- Automatically appends the root primary key fields to `order_by` (ascending) if they aren’t already present, so ordering is deterministic.

### Cursor input

- First page: omit `cursor:` (or pass `nil`).
- Next/previous pages: pass back the opaque cursor returned in the pagination map.
  - Use `cursor: page.cursor_for_entries_after, direction: :after` or `cursor: page.cursor_for_entries_before, direction: :before`.
- Cursor can be passed either as the returned string, or as a decoded map (keys may be strings or atoms).
- Cursor values are opaque base64url-encoded JSON maps (max 8KB) whose keys must match the query’s effective `order_by` fields exactly.

### Execution strategies

`paginate_cursor/3` uses one of three execution strategies depending on query shape.

#### Strategy: Single query (fast path)

Used when:

- The SQL joins in the compiled query are provably **to-one only** (no `has_many` / `many_to_many` joins).
  - This check is conservative: QueryBuilder can only prove “to-one” for Ecto association joins; non-assoc joins are treated as unsafe and will use keys-first.
- Cursor values can be built from the returned structs:
  - root fields are always available
  - `field@assoc` cursor fields require that `assoc` is a **preloaded to-one root association**
    (via `preload_separate/2` or `preload_through_join/2`).

Behavior:

1. Executes one root query with `LIMIT page_size + 1`.
2. Trims the lookahead row to get `page_size` entries.
3. If the base query has no preloads/assocs, QueryBuilder defers any *separate* preloads until **after trimming**
   via `Repo.preload/2` (so the lookahead row isn’t preloaded). If the base query already has preloads, preloads run
   in-query.
4. Builds cursors from the returned structs.

Queries:

- Root queries: **1**
- Separate preload queries: **0** if none; otherwise extra queries (deferred to the trimmed page when possible)

Examples:

```elixir
alias QueryBuilder, as: QB

# Root-only ordering → fast path (1 root query)
User
|> QB.order_by(asc: :nickname, desc: :id)
|> QB.paginate_cursor(Repo, page_size: 20)
```

```elixir
alias QueryBuilder, as: QB

# order_by uses a to-one token, and the assoc is preloaded → fast path (1 root query)
User
|> QB.preload_separate(:role)
|> QB.order_by(:role, asc: :name@role, asc: :id)
|> QB.paginate_cursor(Repo, page_size: 20)
```

#### Strategy: Cursor projection (single query)

Used when:

- The SQL joins in the compiled query are provably **to-one only** (no `has_many` / `many_to_many` joins).
  - This check is conservative: QueryBuilder can only prove “to-one” for Ecto association joins; non-assoc joins are treated as unsafe and will use keys-first.
- Cursor fields can’t be extracted from returned structs (e.g. `field@assoc` but the assoc isn’t preloaded, or deeper tokens).
- The base query has no preloads/assocs, and the query has no `preload_through_join/*` preloads (cursor projection needs a preload-free SQL query).

Behavior:

1. Executes one root query with `LIMIT page_size + 1`, selecting the root struct *and* the cursor/order_by field values.
2. Trims the lookahead row to get `page_size` entries.
3. If QueryBuilder added any *separate* preloads, they are applied **after trimming** via `Repo.preload/2`
   (so the lookahead row isn’t preloaded).
4. Builds cursors from the projected cursor values (so they match SQL ordering semantics without requiring preloads).

Queries:

- Root queries: **1**
- Separate preload queries: extra queries (always deferred to the trimmed page)

Examples:

```elixir
alias QueryBuilder, as: QB

# order_by uses a to-one token, but the assoc is not preloaded → cursor projection (1 root query)
User
|> QB.order_by(:role, asc: :name@role, asc: :id)
|> QB.paginate_cursor(Repo, page_size: 20)
```

#### Strategy: Keys-first (page keys then load entries)

Used when neither the single-query nor cursor-projection strategies are safe, typically because:

- A to-many join exists (root uniqueness is not guaranteed under `LIMIT`), or
- The query has preloads that must remain in-query (notably: `preload_through_join`), or
- QueryBuilder can’t prove the join graph is “to-one only”.

Behavior:

1. Runs a **page keys** query (no preloads): selects only the cursor fields + root PK, `DISTINCT true`, `LIMIT page_size + 1`.
2. Trims the lookahead row, computes `has_more_entries`, and extracts the page’s root PKs.
3. Ensures the PKs are unique; if not, raises with an actionable error (often “ordering depends on a to-many join field”).
4. Runs a second query to **load entries** for those PKs (no `LIMIT`/`OFFSET`/`ORDER BY`) and re-orders them in Elixir to match the keys order.
   - Preloads are preserved here because this query loads exactly the page entries.
5. Builds cursors from the page-keys rows.

Optimization:

- When the query has no through-join preloads, QueryBuilder reloads the page entries by PK list with a join-free query (it drops the join graph) to avoid repeating expensive joins in both the keys query and the entries query.
- If the query includes `preload_through_join/*`, joins are preserved in the entries query to keep join-preload semantics.

Queries:

- Root queries: **2** (keys + entries)
- Separate preload queries: extra queries (only for the page entries, during the entries load query)

Examples:

```elixir
alias QueryBuilder, as: QB

# to-many joins present → IDs-first (2 root queries)
User
|> QB.where([authored_articles: :comments], title@comments: "Hello")
|> QB.order_by(asc: :id)
|> QB.paginate_cursor(Repo, page_size: 20)
```

### When cursor pagination isn’t possible

If `order_by` isn’t cursorable (for example, you use a custom SQL expression), use `paginate_offset/3`:

```elixir
import Ecto.Query
alias QueryBuilder, as: QB

character_length = fn field, resolve ->
  {field, binding} = resolve.(field)
  dynamic([{^binding, x}], fragment("character_length(?)", field(x, ^field)))
end

User
|> QB.order_by(asc: &character_length.(:nickname, &1), asc: :id)
|> QB.paginate_offset(Repo, page_size: 20)
```

## Offset/Row Pagination (`paginate_offset/3`)

### Key properties

- No cursor and no `direction:`. To move pages, use `offset/2` (and always provide a stable `order_by/*`).
- Rejects base Ecto `order_by` clauses (ordering must be expressed via QueryBuilder).
- Rejects custom `select` (must select the root struct).
- Appends the root primary key fields to `order_by` (ascending) if missing, to stabilize ordering.
- Requires the root schema to have a primary key.
- Returns unique root rows (or raises if the query’s `order_by` makes that impossible, e.g. ordering by a to-many field).

Return shape:

```elixir
%{
  paginated_entries: [...],
  pagination: %{
    has_more_entries: boolean,
    max_page_size: pos_integer
  }
}
```

### Execution strategies

`paginate_offset/3` uses one of two execution strategies depending on whether the query is “unique-roots safe”.

#### Strategy: Single query (+ deferred separate preloads)

Used when:

- Root rows are provably unique at SQL level (only to-one association joins).
  - This check is conservative: QueryBuilder can only prove “to-one” for Ecto association joins; non-assoc joins are treated as unsafe and will use keys-first.

Behavior:

1. Executes one root query with `LIMIT page_size + 1`.
2. Trims the lookahead row to get `page_size` entries.
3. If the base query has no preloads/assocs, QueryBuilder defers any *separate* preloads until **after trimming**
   via `Repo.preload/2` (so the lookahead row isn’t preloaded). If the base query already has preloads, preloads run in-query.

Queries:

- Root queries: **1**
- Separate preload queries: **0** if none; otherwise extra queries (deferred to the trimmed page when possible)

Example:

```elixir
alias QueryBuilder, as: QB

User
|> QB.order_by(asc: :id)
|> QB.offset(40)
|> QB.preload_separate(:role)
|> QB.paginate_offset(Repo, page_size: 20)
```

#### Strategy: Keys-first pagination (PKs-first, then load entries)

Used when root uniqueness is not guaranteed (notably: any to-many join, or any join shape QueryBuilder can’t prove is to-one).

Behavior:

1. Runs a “page keys” query (no preloads): selects the root primary key(s) + the `order_by` expressions, `DISTINCT true`, `OFFSET/LIMIT page_size + 1`.
2. Trims the lookahead row, computes `has_more_entries` from the unique root keys, and extracts the page PK list.
3. Loads the page entries by primary key list (no `LIMIT`/`OFFSET`/`ORDER BY`) and re-orders them in Elixir to match the keys order.
   - Preloads are preserved here because this query loads exactly the page entries.

Optimization:

- When the query has no through-join preloads, QueryBuilder reloads the page entries by PK list with a join-free query (it drops the join graph) to avoid repeating expensive joins in both the keys query and the entries query.
- If the query includes `preload_through_join/*`, joins are preserved in the entries query to keep join-preload semantics.

Queries:

- Root queries: **2** (keys + entries)
- Separate preload queries: extra queries (only for the page entries, during the entries load query)

If the page keys query yields duplicate root PKs, `paginate_offset/3` raises. This usually means your `order_by` depends on a to-many join (ambiguous root ordering).

Example:

```elixir
alias QueryBuilder, as: QB

User
|> QB.inner_join(:authored_articles)
|> QB.preload_through_join(:authored_articles) # to-many join-preload
|> QB.order_by(asc: :id)
|> QB.offset(0)
|> QB.paginate_offset(Repo, page_size: 20)
```

## Practical guidance

- Prefer `paginate_cursor/3` / `paginate/3` for feeds and APIs: it’s stable and avoids offset scaling.
- Use `paginate_offset/3` when you can’t do cursor pagination (for example: non-cursorable `order_by`).
- For pagination correctness and performance, keep the root pagination query “unique-roots safe”:
  - avoid to-many joins in the paginated root query when possible
  - use `where_exists_subquery/*` / `where_has/3` for “has related rows” filters
  - use to-many join-preload sparingly in paginated endpoints.
