defmodule QueryBuilder.PaginationStrategyTest do
  use ExUnit.Case

  import Ecto.Query
  import QueryBuilder.Factory
  import QueryBuilder.RepoTelemetryHelpers, only: [with_repo_query_count: 1, with_repo_queries: 1]

  alias QueryBuilder.{Repo, User}
  alias QueryBuilder, as: QB

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
  end

  defp queries_for_from_table(queries, table) when is_list(queries) and is_binary(table) do
    Enum.filter(queries, fn metadata ->
      query = to_string(metadata[:query] || "")
      String.contains?(query, ~s(FROM "#{table}"))
    end)
  end

  defp query_has_join?(metadata) when is_map(metadata) do
    query = to_string(metadata[:query] || "")
    String.contains?(query, " JOIN ")
  end

  defp normalize_ids_param(ids) when is_list(ids), do: ids
  defp normalize_ids_param(id) when is_integer(id), do: [id]

  defp decode_cursor(cursor) when is_binary(cursor) do
    cursor
    |> Base.url_decode64!()
    |> Jason.decode!()
  end

  defp character_length_order_by_fun do
    fn field, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      dynamic([{^binding, x}], fragment("character_length(?)", field(x, ^field)))
    end
  end

  describe "cursor pagination strategy selection" do
    test "uses a single root query (fast path) when no joins/preloads are present" do
      u1 = insert(:user, %{nickname: "CursorRootOnly1"})
      u2 = insert(:user, %{nickname: "CursorRootOnly2"})
      u3 = insert(:user, %{nickname: "CursorRootOnly3"})

      query = User |> QB.order_by(asc: :id)

      {%{paginated_entries: entries, pagination: page}, query_count} =
        with_repo_query_count(fn ->
          QB.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)
        end)

      assert query_count == 1
      assert Enum.map(entries, & &1.id) == [u1.id, u2.id]
      assert page.has_more_entries == true

      assert decode_cursor(page.cursor_for_entries_after) == %{"id" => u2.id}

      %{paginated_entries: next_entries, pagination: next_page} =
        QB.paginate(query, Repo,
          page_size: 2,
          cursor: page.cursor_for_entries_after,
          direction: :after
        )

      assert Enum.map(next_entries, & &1.id) == [u3.id]
      assert next_page.has_more_entries == false

      %{paginated_entries: prev_entries} =
        QB.paginate(query, Repo,
          page_size: 2,
          cursor: next_page.cursor_for_entries_before,
          direction: :before
        )

      assert Enum.map(prev_entries, & &1.id) == [u1.id, u2.id]
    end

    test "uses a single root query when only separate preloads are present (preloads deferred)" do
      u1 = insert(:user, %{nickname: "CursorSeparatePreload1"})
      _ = insert(:article, author: u1, publisher: u1, title: "CursorSeparatePreloadA1")

      u2 = insert(:user, %{nickname: "CursorSeparatePreload2"})
      _ = insert(:article, author: u2, publisher: u2, title: "CursorSeparatePreloadA2")

      query =
        User
        |> QB.preload_separate(:authored_articles)
        |> QB.order_by(asc: :id)

      {%{paginated_entries: [first], pagination: page}, queries} =
        with_repo_queries(fn ->
          QB.paginate(query, Repo, page_size: 1, cursor: nil, direction: :after)
        end)

      assert page.has_more_entries == true
      assert first.id == u1.id
      assert Ecto.assoc_loaded?(first.authored_articles)

      assert length(queries_for_from_table(queries, "users")) == 1
      assert [preload_query] = queries_for_from_table(queries, "articles")

      [user_ids_param] = preload_query[:params]
      assert Enum.sort(normalize_ids_param(user_ids_param)) == [first.id]
    end

    test "uses a single root query when order_by depends on a to-one assoc token (cursor projection)" do
      role_a = insert(:role, %{name: "CursorRoleA"})
      role_b = insert(:role, %{name: "CursorRoleB"})

      u1 = insert(:user, %{role: role_a, nickname: "CursorRoleUser1"})
      u2 = insert(:user, %{role: role_b, nickname: "CursorRoleUser2"})

      query = User |> QB.order_by(:role, asc: :name@role, asc: :id)

      {%{paginated_entries: [first], pagination: page}, queries} =
        with_repo_queries(fn ->
          QB.paginate(query, Repo, page_size: 1, cursor: nil, direction: :after)
        end)

      assert first.id == u1.id
      refute Ecto.assoc_loaded?(first.role)

      assert length(queries_for_from_table(queries, "users")) == 1
      assert queries_for_from_table(queries, "roles") == []

      cursor_map = decode_cursor(page.cursor_for_entries_after)
      assert cursor_map["name@role"] == role_a.name
      assert cursor_map["id"] == u1.id

      %{paginated_entries: [second]} =
        QB.paginate(query, Repo,
          page_size: 1,
          cursor: page.cursor_for_entries_after,
          direction: :after
        )

      assert second.id == u2.id
    end

    test "uses a single root query when order_by uses a to-one assoc token and separate preloads are present (preloads deferred)" do
      role_a = insert(:role, %{name: "CursorRolePreloadA"})
      role_b = insert(:role, %{name: "CursorRolePreloadB"})

      u1 = insert(:user, %{role: role_a, nickname: "CursorRolePreloadUser1"})
      _ = insert(:article, author: u1, publisher: u1, title: "CursorRolePreloadA1")

      u2 = insert(:user, %{role: role_b, nickname: "CursorRolePreloadUser2"})
      _ = insert(:article, author: u2, publisher: u2, title: "CursorRolePreloadA2")

      query =
        User
        |> QB.preload_separate(:role)
        |> QB.preload_separate(:authored_articles)
        |> QB.order_by(:role, asc: :name@role, asc: :id)

      {%{paginated_entries: [first], pagination: page}, queries} =
        with_repo_queries(fn ->
          QB.paginate(query, Repo, page_size: 1, cursor: nil, direction: :after)
        end)

      assert page.has_more_entries == true
      assert first.id == u1.id
      assert Ecto.assoc_loaded?(first.role)
      assert Ecto.assoc_loaded?(first.authored_articles)

      assert length(queries_for_from_table(queries, "users")) == 1
      assert length(queries_for_from_table(queries, "roles")) == 1
      assert [preload_query] = queries_for_from_table(queries, "articles")

      [user_ids_param] = preload_query[:params]
      assert Enum.sort(normalize_ids_param(user_ids_param)) == [first.id]
    end

    test "uses ids-first when to-many joins are present (root uniqueness not guaranteed)" do
      u1 = insert(:user, %{nickname: "CursorToManyJoin1"})
      a1 = insert(:article, author: u1, publisher: u1, title: "CursorToManyJoinArticle1")
      _ = insert(:comment, article: a1, user: u1, title: "CursorToManyJoinComment")

      u2 = insert(:user, %{nickname: "CursorToManyJoin2"})
      a2 = insert(:article, author: u2, publisher: u2, title: "CursorToManyJoinArticle2")
      _ = insert(:comment, article: a2, user: u2, title: "CursorToManyJoinComment")

      query =
        User
        |> QB.where([authored_articles: :comments], title@comments: "CursorToManyJoinComment")
        |> QB.order_by(asc: :id)

      {%{paginated_entries: [first], pagination: page}, queries} =
        with_repo_queries(fn ->
          QB.paginate(query, Repo, page_size: 1, cursor: nil, direction: :after)
        end)

      assert page.has_more_entries == true
      assert first.id == u1.id

      users_queries = queries_for_from_table(queries, "users")
      assert length(users_queries) == 2

      # keys query keeps joins; load query is join-free (reload by PK list)
      assert Enum.count(users_queries, &query_has_join?/1) == 1

      %{paginated_entries: [second]} =
        QB.paginate(query, Repo,
          page_size: 1,
          cursor: page.cursor_for_entries_after,
          direction: :after
        )

      assert second.id == u2.id
    end

    test "keeps joins in keys-first load when a to-many through-join preload is present" do
      u1 = insert(:user, %{nickname: "CursorThroughJoin1"})
      _ = insert(:article, author: u1, publisher: u1, title: "CursorThroughJoinA1")
      _ = insert(:article, author: u1, publisher: u1, title: "CursorThroughJoinA2")

      query =
        User
        |> QB.inner_join(:authored_articles)
        |> QB.preload_through_join(:authored_articles)
        |> QB.order_by(asc: :id)

      {%{paginated_entries: [first]}, queries} =
        with_repo_queries(fn ->
          QB.paginate(query, Repo, page_size: 1, cursor: nil, direction: :after)
        end)

      assert first.id == u1.id
      assert Ecto.assoc_loaded?(first.authored_articles)
      assert first.authored_articles != []

      users_queries = queries_for_from_table(queries, "users")
      assert length(users_queries) == 2

      # both keys and load queries must keep joins to preserve join-preload semantics
      assert Enum.count(users_queries, &query_has_join?/1) == 2
    end
  end

  describe "offset pagination strategy selection" do
    test "uses a single root query when only separate preloads are present (preloads deferred)" do
      order_by_fun = character_length_order_by_fun()

      u1 = insert(:user, %{nickname: "OffsetSeparatePreload1"})
      _ = insert(:article, author: u1, publisher: u1, title: "OffsetSeparatePreloadA1")

      _u2 = insert(:user, %{nickname: "OffsetSeparatePreload2"})

      query =
        User
        |> QB.preload_separate(:authored_articles)
        |> QB.order_by(asc: &order_by_fun.(:nickname, &1), asc: :id)

      {%{paginated_entries: [first], pagination: page}, queries} =
        with_repo_queries(fn ->
          QB.paginate_offset(query, Repo, page_size: 1)
        end)

      assert page.has_more_entries == true
      assert Ecto.assoc_loaded?(first.authored_articles)

      assert length(queries_for_from_table(queries, "users")) == 1
      assert [preload_query] = queries_for_from_table(queries, "articles")

      [user_ids_param] = preload_query[:params]
      assert Enum.sort(normalize_ids_param(user_ids_param)) == [first.id]
    end

    test "uses keys-first when a to-many through-join preload is present (unique roots)" do
      order_by_fun = character_length_order_by_fun()

      u1 = insert(:user, %{nickname: "OffsetThroughJoin1"})
      _ = insert(:article, author: u1, publisher: u1, title: "OffsetThroughJoinA1")
      _ = insert(:article, author: u1, publisher: u1, title: "OffsetThroughJoinA2")

      query =
        User
        |> QB.inner_join(:authored_articles)
        |> QB.preload_through_join(:authored_articles)
        |> QB.order_by(asc: &order_by_fun.(:nickname, &1), asc: :id)

      {%{paginated_entries: [first], pagination: page}, queries} =
        with_repo_queries(fn ->
          QB.paginate_offset(query, Repo, page_size: 1)
        end)

      assert first.id == u1.id
      assert Ecto.assoc_loaded?(first.authored_articles)
      assert first.authored_articles != []

      assert queries_for_from_table(queries, "articles") == []
      assert page.has_more_entries == false
      users_queries = queries_for_from_table(queries, "users")
      assert length(users_queries) == 2

      assert Enum.count(users_queries, &query_has_join?/1) == 2
    end

    test "uses keys-first when to-many joins are present (unique roots across offsets)" do
      u1 = insert(:user, %{nickname: "OffsetToManyJoin1"})
      _ = insert(:article, author: u1, publisher: u1, title: "OffsetToManyJoinA1")
      _ = insert(:article, author: u1, publisher: u1, title: "OffsetToManyJoinA2")

      u2 = insert(:user, %{nickname: "OffsetToManyJoin2"})
      _ = insert(:article, author: u2, publisher: u2, title: "OffsetToManyJoinB1")

      query =
        User
        |> QB.inner_join(:authored_articles)
        |> QB.order_by(asc: :id)

      {%{paginated_entries: [first], pagination: page1}, queries} =
        with_repo_queries(fn ->
          query
          |> QB.offset(0)
          |> QB.paginate_offset(Repo, page_size: 1)
        end)

      assert first.id == u1.id
      assert page1.has_more_entries == true

      users_queries = queries_for_from_table(queries, "users")
      assert length(users_queries) == 2

      # keys query keeps joins; load query is join-free (reload by PK list)
      assert Enum.count(users_queries, &query_has_join?/1) == 1
      assert queries_for_from_table(queries, "articles") == []

      %{paginated_entries: [second], pagination: page2} =
        query
        |> QB.offset(1)
        |> QB.paginate_offset(Repo, page_size: 1)

      assert second.id == u2.id
      assert page2.has_more_entries == false
    end

    test "raises when order_by depends on a to-many join (ambiguous root ordering)" do
      u1 = insert(:user, %{nickname: "OffsetToManyOrderBy1"})
      _ = insert(:article, author: u1, publisher: u1, title: "OffsetToManyOrderByA1")
      _ = insert(:article, author: u1, publisher: u1, title: "OffsetToManyOrderByA2")

      _u2 = insert(:user, %{nickname: "OffsetToManyOrderBy2"})

      query =
        User
        |> QB.inner_join(:authored_articles)
        |> QB.order_by(:authored_articles, asc: :id@authored_articles)
        |> QB.order_by(asc: :id)

      assert_raise ArgumentError,
                   ~r/paginate_offset\/3 could not produce a page of unique root rows/i,
                   fn ->
                     QB.paginate_offset(query, Repo, page_size: 1)
                   end
    end
  end
end
