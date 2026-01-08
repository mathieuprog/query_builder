defmodule QueryBuilder.ArrayAggTest do
  use ExUnit.Case, async: true

  import Ecto.Query
  import QueryBuilder.Factory

  alias QueryBuilder.{Article, Repo}
  alias QueryBuilder, as: QB

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
  end

  test "array_agg aggregates distinct values with ordering" do
    author = insert(:user, %{name: "ArrayAggAuthor"})
    pub1 = insert(:user, %{name: "ArrayAggPublisher1"})
    pub2 = insert(:user, %{name: "ArrayAggPublisher2"})

    _ = insert(:article, author: author, publisher: pub2)
    _ = insert(:article, author: author, publisher: pub1)
    _ = insert(:article, author: author, publisher: pub1)

    query =
      Article
      |> QB.group_by(:author_id)
      |> QB.select(%{
        author_id: :author_id,
        publisher_ids:
          QB.array_agg(:publisher_id, distinct?: true, order_by: [asc: :publisher_id])
      })

    {sql, _params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)
    assert Regex.match?(~r/\barray_agg\b/i, sql)
    assert Regex.match?(~r/\bDISTINCT\b/i, sql)
    assert Regex.match?(~r/\bORDER BY\b/i, sql)

    [%{author_id: author_id, publisher_ids: publisher_ids}] = Repo.all(query)

    assert author_id == author.id
    assert publisher_ids == Enum.sort([pub1.id, pub2.id])
  end

  test "array_agg supports FILTER (WHERE ...) via filter DSL" do
    author = insert(:user, %{name: "ArrayAggFilterAuthor"})
    pub1 = insert(:user, %{name: "ArrayAggFilterPublisher1"})
    pub2 = insert(:user, %{name: "ArrayAggFilterPublisher2"})

    _ = insert(:article, author: author, publisher: pub2)
    _ = insert(:article, author: author, publisher: pub1)
    _ = insert(:article, author: author, publisher: pub1)

    query =
      Article
      |> QB.group_by(:author_id)
      |> QB.select(%{
        author_id: :author_id,
        publisher_ids:
          QB.array_agg(:publisher_id,
            distinct?: true,
            order_by: [asc: :publisher_id],
            filter: [publisher_id: pub1.id]
          )
      })

    {sql, _params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)
    assert Regex.match?(~r/\bFILTER\s*\(\s*WHERE\b/i, sql)

    [%{author_id: author_id, publisher_ids: publisher_ids}] = Repo.all(query)
    assert author_id == author.id
    assert publisher_ids == [pub1.id]
  end

  test "array_agg supports FILTER (WHERE ...) via dynamic" do
    author = insert(:user, %{name: "ArrayAggFilterDynamicAuthor"})
    pub1 = insert(:user, %{name: "ArrayAggFilterDynamicPublisher1"})
    pub2 = insert(:user, %{name: "ArrayAggFilterDynamicPublisher2"})

    _ = insert(:article, author: author, publisher: pub2)
    _ = insert(:article, author: author, publisher: pub1)
    _ = insert(:article, author: author, publisher: pub1)

    query =
      Article
      |> QB.group_by(:author_id)
      |> QB.select(%{
        author_id: :author_id,
        publisher_ids:
          QB.array_agg(:publisher_id,
            distinct?: true,
            order_by: [asc: :publisher_id],
            filter: dynamic([a], a.publisher_id == ^pub1.id)
          )
      })

    [%{author_id: author_id, publisher_ids: publisher_ids}] = Repo.all(query)
    assert author_id == author.id
    assert publisher_ids == [pub1.id]
  end

  test "array_agg filter DSL rejects OR groups" do
    assert_raise ArgumentError, ~r/filter DSL is AND-only.*or/i, fn ->
      QB.array_agg(:publisher_id, filter: [or: [publisher_id: 1]])
    end

    assert_raise ArgumentError, ~r/filter DSL is AND-only.*or/i, fn ->
      QB.array_agg(:publisher_id, filter: [{:or, [publisher_id: 1]}])
    end
  end

  test "array_agg distinct requires order_by expressions to match the aggregated token" do
    assert_raise ArgumentError, ~r/distinct\?: true.*requires.*order_by.*match/i, fn ->
      QB.array_agg(:publisher_id, distinct?: true, order_by: [asc: :id])
    end
  end

  test "array_agg rejects unknown options" do
    assert_raise ArgumentError, ~r/unknown options/i, fn ->
      QB.array_agg(:publisher_id, foo: :bar)
    end
  end

  test "array_agg supports up to 5 order_by terms" do
    assert_raise ArgumentError, ~r/supports up to 5 order_by terms/i, fn ->
      QB.array_agg(:publisher_id,
        order_by: [
          asc: :publisher_id,
          asc: :publisher_id,
          asc: :publisher_id,
          asc: :publisher_id,
          asc: :publisher_id,
          asc: :publisher_id
        ]
      )
    end
  end
end
