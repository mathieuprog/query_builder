defmodule QueryBuilderTest do
  use ExUnit.Case
  import QueryBuilder.Factory
  alias QueryBuilder.{Repo, User, Article}
  require Ecto.Query
  import Ecto.Query

  doctest QueryBuilder

  defmodule UnknownAdapterRepo do
    def __adapter__, do: UnknownAdapter
    def all(_query), do: raise("Repo.all/1 should not be called for unknown adapters")
  end

  defmodule UnknownAdapter do
  end

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(QueryBuilder.Repo)
  end

  setup :insert_demo_data

  def insert_demo_data(_) do
    Application.put_env(:query_builder, :authorizer, nil)

    role_admin = insert(:role, %{name: "admin"})
    role_author = insert(:role, %{name: "author"})
    role_publisher = insert(:role, %{name: "publisher"})
    role_reader = insert(:role, %{name: "reader"})

    insert(:permission, %{role: role_admin, name: "read"})
    insert(:permission, %{role: role_admin, name: "delete"})
    insert(:permission, %{role: role_author, name: "read"})
    insert(:permission, %{role: role_author, name: "write"})
    insert(:permission, %{role: role_publisher, name: "read"})
    insert(:permission, %{role: role_publisher, name: "publish"})
    insert(:permission, %{role: role_reader, name: "read"})

    author1 =
      insert(:user, %{
        id: 100,
        name: "Alice",
        email: "alice@example.com",
        role: role_author,
        nickname: "Alice"
      })

    author2 =
      insert(:user, %{
        id: 101,
        name: "Bob",
        email: "the_bob@example.com",
        role: role_author,
        nickname: "Bobby"
      })

    author3 =
      insert(:user, %{
        id: 103,
        name: "Charlie",
        email: "charlie@example.com",
        role: role_author,
        nickname: "Lee"
      })

    reader =
      insert(:user, %{
        id: 102,
        name: "Eric",
        email: nil,
        role: role_reader,
        nickname: "Eric",
        deleted: true
      })

    insert(:user, %{
      id: 200,
      name: "Dave",
      email: "dave@example.com",
      role: role_admin,
      nickname: "Dave"
    })

    insert(:user, %{
      id: 201,
      name: "Richard",
      email: "richard@example.com",
      role: role_admin,
      nickname: "Rich"
    })

    insert(:user, %{
      id: 202,
      name: "An% we_ird %name_%",
      email: "weirdo@example.com",
      role: role_reader,
      nickname: "John"
    })

    insert(:user, %{
      id: 203,
      name: "An_ we_ird %name_%",
      email: "weirdo@example.com",
      role: role_reader,
      nickname: "James"
    })

    publisher =
      insert(:user, %{
        id: 300,
        name: "Calvin",
        email: "calvin@example.com",
        role: role_publisher,
        nickname: "Calvin"
      })

    insert(:acl, %{grantee: author1, grantor: author2})
    insert(:acl, %{grantee: reader, grantor: author1})

    title1 = "ELIXIR V1.9 RELEASED"
    title2 = "MINT, A NEW HTTP CLIENT FOR ELIXIR"
    title3 = "ELIXIR V1.8 RELEASED"
    title4 = "INTEGRATING TRAVEL WITH ELIXIR AT DUFFEL"

    articles = [
      insert(:article, %{
        title: title1,
        author: author1,
        publisher: publisher,
        tags: ["baz", "qux"]
      }),
      insert(:article, %{title: title2, author: author1, publisher: publisher, tags: ["baz"]}),
      insert(:article, %{title: title3, author: author2, publisher: publisher}),
      insert(:article, %{title: title4, author: author3, publisher: publisher})
    ]

    for article <- articles do
      comments = insert_list(2, :comment, article: article, user: reader)
      insert_list(2, :article_like, article: article, user: reader)
      insert_list(3, :article_star, article: article, user: reader)

      for comment <- comments do
        insert_list(2, :comment_like, comment: comment, article: article, user: reader)
        insert_list(3, :comment_star, comment: comment, article: article, user: reader)
      end
    end

    :ok
  end

  defp with_repo_query_count(fun) when is_function(fun, 0) do
    parent = self()
    handler_id = "repo-query-count-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:query_builder, :repo, :query],
      fn _event, _measurements, _metadata, parent ->
        send(parent, {:repo_query, handler_id})
      end,
      parent
    )

    result =
      try do
        fun.()
      after
        :telemetry.detach(handler_id)
      end

    {result, drain_repo_query_messages(handler_id)}
  end

  defp drain_repo_query_messages(handler_id, count \\ 0) do
    receive do
      {:repo_query, ^handler_id} ->
        drain_repo_query_messages(handler_id, count + 1)
    after
      0 ->
        count
    end
  end

  defp with_repo_queries(fun) when is_function(fun, 0) do
    parent = self()
    handler_id = "repo-queries-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:query_builder, :repo, :query],
      fn _event, _measurements, metadata, parent ->
        send(parent, {:repo_query, handler_id, metadata})
      end,
      parent
    )

    result =
      try do
        fun.()
      after
        :telemetry.detach(handler_id)
      end

    {result, drain_repo_query_metadata(handler_id)}
  end

  defp drain_repo_query_metadata(handler_id, acc \\ []) do
    receive do
      {:repo_query, ^handler_id, metadata} ->
        drain_repo_query_metadata(handler_id, [metadata | acc])
    after
      0 ->
        Enum.reverse(acc)
    end
  end

  test "authorizer" do
    Application.put_env(:query_builder, :authorizer, QueryBuilder.Authorizer)

    query =
      User
      |> QueryBuilder.where(id: 101)
      |> QueryBuilder.preload(:authored_articles)

    assert Repo.one!(query).authored_articles != []

    query =
      User
      |> QueryBuilder.where(id: 103)
      |> QueryBuilder.preload(:authored_articles)

    assert Repo.one!(query).authored_articles == []

    assert length(Repo.all(Article)) == 4

    assert length(Repo.all(QueryBuilder.new(Article))) == 3
  end

  test "where" do
    assert User
           |> QueryBuilder.where(name: "Bob")
           |> Repo.one()

    assert User
           |> QueryBuilder.where(name: "Bob", email: "the_bob@example.com")
           |> Repo.one()

    refute User
           |> QueryBuilder.where(name: "John")
           |> Repo.one()

    assert User
           |> QueryBuilder.where(name: "Bob")
           |> QueryBuilder.where(email: "the_bob@example.com")
           |> Repo.one()

    refute User
           |> QueryBuilder.where(name: "Bob")
           |> QueryBuilder.where(email: "alice@example.com")
           |> Repo.one()

    all_users_but_bob =
      User
      |> QueryBuilder.where({:name, :ne, "Bob"})
      |> Repo.all()

    assert 8 == length(all_users_but_bob)

    all_users_but_bob =
      User
      |> QueryBuilder.where({:name, :other_than, "Bob"})
      |> Repo.all()

    assert 8 == length(all_users_but_bob)

    users_containing_ri =
      User
      |> QueryBuilder.where({:name, :contains, "ri", case: :i})
      |> Repo.all()

    assert 2 == length(users_containing_ri)

    users_containing_ri =
      User
      |> QueryBuilder.where({:name, :contains, "ri", case: :insensitive})
      |> Repo.all()

    assert 2 == length(users_containing_ri)

    users_containing_ri =
      User
      |> QueryBuilder.where({:name, :ilike, "%ri%"})
      |> Repo.all()

    assert 2 == length(users_containing_ri)

    users_starts_with_ri =
      User
      |> QueryBuilder.where({:name, :starts_with, "ri", case: :insensitive})
      |> Repo.all()

    assert 1 == length(users_starts_with_ri)

    users_starts_with_an =
      User
      |> QueryBuilder.where({:name, :starts_with, "an%", case: :insensitive})
      |> Repo.all()

    assert 1 == length(users_starts_with_an)

    users_starts_with_ri =
      User
      |> QueryBuilder.where({:name, :starts_with, "ri"})
      |> Repo.all()

    assert 0 == length(users_starts_with_ri)

    users_starts_with_ri =
      User
      |> QueryBuilder.where({:name, :like, "ri%"})
      |> Repo.all()

    assert 0 == length(users_starts_with_ri)

    users_ends_with_ob =
      User
      |> QueryBuilder.where({:name, :ends_with, "ob"})
      |> Repo.all()

    assert 1 == length(users_ends_with_ob)

    users_containing_ri =
      User
      |> QueryBuilder.where({:name, :contains, "ri"})
      |> Repo.all()

    assert 1 == length(users_containing_ri)

    users_containing_ri =
      User
      |> QueryBuilder.where({:name, :contains, "Ri", case: :sensitive})
      |> Repo.all()

    assert 1 == length(users_containing_ri)

    users_in_list =
      User
      |> QueryBuilder.where({:name, :in, ["Alice", "Bob"]})
      |> Repo.all()

    assert 2 == length(users_in_list)

    users_not_in_list =
      User
      |> QueryBuilder.where({:name, :not_in, ["Alice", "Bob"]})
      |> Repo.all()

    assert 7 == length(users_not_in_list)

    articles_including_tags =
      Article
      |> QueryBuilder.where({:tags, :include, "baz"})
      |> Repo.all()

    assert 2 == length(articles_including_tags)

    articles_excluding_tags =
      Article
      |> QueryBuilder.where({:tags, :exclude, "baz"})
      |> Repo.all()

    assert 2 == length(articles_excluding_tags)
  end

  test "empty where" do
    all_users =
      User
      |> QueryBuilder.where([])
      |> Repo.all()

    assert 9 == length(all_users)

    result =
      User
      |> QueryBuilder.where([], [], or: [name: "Bob", deleted: false])
      |> Repo.all()

    assert 1 == length(result)
  end

  test "where with or groups" do
    result =
      User
      |> QueryBuilder.where([], [name: "Alice", deleted: false],
        or: [name: "Bob", deleted: false]
      )
      |> Repo.all()

    assert 2 == length(result)

    result =
      User
      |> QueryBuilder.where(deleted: false)
      |> QueryBuilder.where([], [name: "Alice"],
        or: [name: "Bob"],
        or: [name: "Eric"],
        or: [name: "Dave"]
      )
      |> Repo.all()

    assert 3 == length(result)

    result =
      User
      |> QueryBuilder.where(:role, [name@role: "author"], or: [name@role: "publisher"])
      |> Repo.all()

    assert 4 == length(result)
  end

  test "where multiple conditions" do
    alice =
      User
      |> QueryBuilder.where(deleted: false, name: "Alice")
      |> Repo.all()

    assert 1 == length(alice)
  end

  test "where with custom query" do
    text_equals_condition = fn field, value, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      Ecto.Query.dynamic([{^binding, x}], fragment("initcap(?)", ^value) == field(x, ^field))
    end

    alice =
      User
      |> QueryBuilder.where(&text_equals_condition.(:name, "alice", &1))
      |> Repo.all()

    assert 1 == length(alice)
  end

  test "maybe where" do
    maybe_bob =
      User
      |> QueryBuilder.maybe_where(true, name: "Bob")
      |> Repo.all()

    assert 1 == length(maybe_bob)

    maybe_bob =
      User
      |> QueryBuilder.maybe_where(false, name: "Bob")
      |> Repo.all()

    assert 9 == length(maybe_bob)
  end

  test "where boolean" do
    deleted_users =
      User
      |> QueryBuilder.where({:deleted, :eq, true})
      |> Repo.all()

    assert 1 == length(deleted_users)

    not_deleted_users =
      User
      |> QueryBuilder.where({:deleted, :ne, true})
      |> Repo.all()

    assert 8 == length(not_deleted_users)

    not_deleted_users =
      User
      |> QueryBuilder.where({:deleted, :eq, false})
      |> Repo.all()

    assert 8 == length(not_deleted_users)

    not_deleted_users =
      User
      |> QueryBuilder.where(deleted: false)
      |> Repo.all()

    assert 8 == length(not_deleted_users)
  end

  test "where is (not) null" do
    users_without_email =
      User
      |> QueryBuilder.where({:email, :eq, nil})
      |> Repo.all()

    assert 1 == length(users_without_email)

    users_with_email =
      User
      |> QueryBuilder.where({:email, :ne, nil})
      |> Repo.all()

    assert 8 == length(users_with_email)

    users_without_email =
      User
      |> QueryBuilder.where(email: nil)
      |> Repo.all()

    assert 1 == length(users_without_email)
  end

  test "where comparing two fields" do
    users_where_name_matches_nickname =
      User
      |> QueryBuilder.where({:name, :eq, :nickname@self})
      |> Repo.all()

    assert 4 == length(users_where_name_matches_nickname)

    users_where_name_matches_raw_nickname =
      User
      |> QueryBuilder.where({:name, :eq, :nickname})
      |> Repo.all()

    assert 0 == length(users_where_name_matches_raw_nickname)

    users_where_name_included_in_email =
      User
      |> QueryBuilder.where({:email, :contains, :name@self, case: :insensitive})
      |> Repo.all()

    assert 6 == length(users_where_name_included_in_email)

    users_where_name_included_in_email =
      User
      |> QueryBuilder.where({:email, :starts_with, :name@self, case: :insensitive})
      |> Repo.all()

    assert 5 == length(users_where_name_included_in_email)
  end

  test "where with assocs" do
    all_authors =
      User
      |> QueryBuilder.where(:role, name@role: "author")
      |> Repo.all()

    assert 3 == length(all_authors)

    all_users_with_write_role =
      User
      |> QueryBuilder.where([role: :permissions], name@permissions: "write")
      |> Repo.all()

    assert 3 == length(all_users_with_write_role)
  end

  test "order_by" do
    users_ordered_asc =
      User
      |> QueryBuilder.order_by(asc: :name)
      |> Repo.all()

    assert "Alice" == hd(users_ordered_asc).name

    users_ordered_desc =
      User
      |> QueryBuilder.order_by(desc: :name)
      |> Repo.all()

    assert "Richard" == hd(users_ordered_desc).name
  end

  test "order_by with assocs" do
    alice =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.order_by(:authored_articles, asc: :title@authored_articles)
      |> QueryBuilder.preload(:authored_articles)
      |> Repo.one!()

    assert hd(alice.authored_articles).title == "ELIXIR V1.9 RELEASED"

    alice =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.order_by(:authored_articles, desc: :title@authored_articles)
      |> QueryBuilder.preload(:authored_articles)
      |> Repo.one!()

    assert hd(alice.authored_articles).title == "MINT, A NEW HTTP CLIENT FOR ELIXIR"
  end

  test "empty order_by" do
    all_users =
      User
      |> QueryBuilder.order_by([])
      |> Repo.all()

    assert 9 == length(all_users)
  end

  test "order_by with fragment" do
    character_length = fn field, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      Ecto.Query.dynamic([{^binding, x}], fragment("character_length(?)", field(x, ^field)))
    end

    ordered_users =
      User
      |> QueryBuilder.order_by(asc: &character_length.(:nickname, &1))
      |> Repo.all()

    assert hd(ordered_users).nickname == "Lee"
  end

  test "left_join" do
    # Eric is not an author
    assert User
           |> QueryBuilder.left_join(:authored_articles)
           |> QueryBuilder.where(name: "Eric")
           |> Repo.one()

    assert User
           |> QueryBuilder.where(name: "Eric")
           |> QueryBuilder.left_join(:authored_articles,
             title@authored_articles: "ELIXIR V1.9 RELEASED"
           )
           |> Repo.one()

    refute User
           |> QueryBuilder.where(name: "Eric")
           |> QueryBuilder.where(:authored_articles,
             title@authored_articles: "ELIXIR V1.9 RELEASED"
           )
           |> Repo.one()
  end

  test "preload" do
    query =
      Ecto.Query.from(u in User,
        join: r in assoc(u, :role),
        join: a in assoc(u, :authored_articles)
      )
      |> Ecto.Query.where([u, r, a], a.title == ^"ELIXIR V1.9 RELEASED")
      |> Ecto.Query.preload([u, r, a], [
        :published_articles,
        authored_articles:
          {a,
           [:article_likes, :article_stars, {:comments, [:comment_stars, comment_likes: :user]}]}
      ])
      |> Ecto.Query.preload([u, r, a], role: r)

    preload = [
      :role,
      :published_articles,
      {
        :authored_articles,
        [
          :article_likes,
          :article_stars,
          {:comments, [:comment_stars, comment_likes: :user]}
        ]
      }
    ]

    built_query =
      User
      |> QueryBuilder.where(:authored_articles, title@authored_articles: "ELIXIR V1.9 RELEASED")
      |> QueryBuilder.preload(preload)

    assert %{changed: :equal} = MapDiff.diff(Repo.all(query), Repo.all(built_query))

    built_query =
      User
      |> QueryBuilder.preload(preload)
      |> QueryBuilder.where(:authored_articles, title@authored_articles: "ELIXIR V1.9 RELEASED")

    assert %{changed: :equal} = MapDiff.diff(Repo.all(query), Repo.all(built_query))
  end

  test "cursor pagination" do
    query = from(u in User, order_by: [asc: u.nickname, desc: u.email])
    query = from(u in query, order_by: [desc: u.email])
    all_users = Repo.all(query)

    assert ["Alice", "Bobby", "Calvin", "Dave", "Eric", "James", "John", "Lee", "Rich"] =
             all_users |> Enum.map(& &1.nickname)

    all_users =
      User
      |> QueryBuilder.order_by(asc: :nickname, desc: :email)
      |> QueryBuilder.order_by(desc: :email)
      |> Repo.all()

    assert ["Alice", "Bobby", "Calvin", "Dave", "Eric", "James", "John", "Lee", "Rich"] =
             all_users |> Enum.map(& &1.nickname)

    assert 9 == length(all_users)

    query =
      User
      |> QueryBuilder.order_by(asc: :nickname, desc: :email)
      |> QueryBuilder.order_by(desc: :email)

    %{paginated_entries: paginated_entries, pagination: pagination} =
      QueryBuilder.paginate(query, Repo, page_size: 3, cursor: nil, direction: :after)

    assert %{
             cursor_direction: :after,
             cursor_for_entries_before: _cursor_for_entries_before,
             cursor_for_entries_after: cursor_for_entries_after,
             has_more_entries: true,
             max_page_size: 3
           } = pagination

    assert ["Alice", "Bobby", "Calvin"] = paginated_entries |> Enum.map(& &1.nickname)

    %{paginated_entries: paginated_entries, pagination: pagination} =
      QueryBuilder.paginate(query, Repo,
        page_size: 3,
        cursor: cursor_for_entries_after,
        direction: :after
      )

    assert %{
             cursor_direction: :after,
             cursor_for_entries_before: _cursor_for_entries_before,
             cursor_for_entries_after: cursor_for_entries_after,
             has_more_entries: true,
             max_page_size: 3
           } = pagination

    assert ["Dave", "Eric", "James"] = paginated_entries |> Enum.map(& &1.nickname)

    %{paginated_entries: paginated_entries, pagination: pagination} =
      QueryBuilder.paginate(query, Repo,
        page_size: 3,
        cursor: cursor_for_entries_after,
        direction: :after
      )

    assert %{
             cursor_direction: :after,
             cursor_for_entries_before: cursor_for_entries_before,
             cursor_for_entries_after: _cursor_for_entries_after,
             has_more_entries: false,
             max_page_size: 3
           } = pagination

    assert ["John", "Lee", "Rich"] = paginated_entries |> Enum.map(& &1.nickname)

    %{paginated_entries: paginated_entries, pagination: pagination} =
      QueryBuilder.paginate(query, Repo,
        page_size: 3,
        cursor: cursor_for_entries_before,
        direction: :before
      )

    assert %{
             cursor_direction: :before,
             cursor_for_entries_before: cursor_for_entries_before,
             cursor_for_entries_after: _cursor_for_entries_after,
             has_more_entries: true,
             max_page_size: 3
           } = pagination

    assert ["Dave", "Eric", "James"] = paginated_entries |> Enum.map(& &1.nickname)

    %{paginated_entries: paginated_entries, pagination: pagination} =
      QueryBuilder.paginate(query, Repo,
        page_size: 3,
        cursor: cursor_for_entries_before,
        direction: :before
      )

    assert %{
             cursor_direction: :before,
             cursor_for_entries_before: _cursor_for_entries_before,
             cursor_for_entries_after: _cursor_for_entries_after,
             has_more_entries: false,
             max_page_size: 3
           } = pagination

    assert ["Alice", "Bobby", "Calvin"] = paginated_entries |> Enum.map(& &1.nickname)
  end

  test "paginate raises when the base ecto_query already has order_by clauses (ordering must be expressed via QueryBuilder.order_by)" do
    base_query = from(u in User, order_by: [asc: u.nickname])

    query = QueryBuilder.new(base_query)

    assert_raise ArgumentError, ~r/base.*order_by|QueryBuilder\.order_by/i, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)
    end
  end

  test "cursor pagination preserves preloads" do
    %{
      paginated_entries: [first | _],
      pagination: %{cursor_for_entries_after: cursor}
    } =
      User
      |> QueryBuilder.preload(:role)
      |> QueryBuilder.order_by(:role, asc: :name@role)
      |> QueryBuilder.paginate(Repo, page_size: 2, cursor: nil, direction: :after)

    assert Ecto.assoc_loaded?(first.role)

    %{paginated_entries: [first2 | _]} =
      User
      |> QueryBuilder.preload(:role)
      |> QueryBuilder.order_by(:role, asc: :name@role)
      |> QueryBuilder.paginate(Repo, page_size: 2, cursor: cursor, direction: :after)

    assert Ecto.assoc_loaded?(first2.role)
  end

  test "cursor pagination uses a single query in the happy flow (root cursor fields + no joins)" do
    query =
      User
      |> QueryBuilder.order_by(asc: :nickname, desc: :email)
      |> QueryBuilder.order_by(desc: :email)

    {_result, query_count} =
      with_repo_query_count(fn ->
        QueryBuilder.paginate(query, Repo, page_size: 3, cursor: nil, direction: :after)
      end)

    assert query_count == 1
  end

  test "cursor pagination stays single-query with to-one joins (e.g. belongs_to)" do
    query =
      User
      |> QueryBuilder.where(:role, name@role: "author")
      |> QueryBuilder.order_by(asc: :nickname)

    {_result, query_count} =
      with_repo_query_count(fn ->
        QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)
      end)

    assert query_count == 1
  end

  test "cursor pagination stays single-query when ordering by a to-one association field token and the assoc is preloaded" do
    query =
      User
      |> QueryBuilder.preload(:role)
      |> QueryBuilder.order_by(:role, asc: :name@role)

    {_result, query_count} =
      with_repo_query_count(fn ->
        QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)
      end)

    assert query_count == 1
  end

  test "cursor pagination does not use the single-query fast path when an @token resolves to a nested association with the same name as a root association" do
    query =
      QueryBuilder.CommentLike
      |> QueryBuilder.preload(comment: :user)
      |> QueryBuilder.order_by([comment: :user], asc: :nickname@user)

    {_result, query_count} =
      with_repo_query_count(fn ->
        QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)
      end)

    assert query_count == 2
  end

  test "cursor pagination avoids preloading the sentinel row for to-many preloads (uses ids-first)" do
    query =
      User
      |> QueryBuilder.preload(:authored_articles)
      |> QueryBuilder.order_by(asc: :nickname)

    {%{paginated_entries: [first]}, queries} =
      with_repo_queries(fn ->
        QueryBuilder.paginate(query, Repo, page_size: 1, cursor: nil, direction: :after)
      end)

    assert Ecto.assoc_loaded?(first.authored_articles)

    preload_queries =
      Enum.filter(queries, fn metadata ->
        query = to_string(metadata[:query] || "")
        String.contains?(query, ~s(FROM "articles"))
      end)

    assert [preload_query] = preload_queries

    [user_ids_param] = preload_query[:params]

    user_ids =
      case user_ids_param do
        ids when is_list(ids) -> ids
        id when is_integer(id) -> [id]
      end

    assert Enum.sort(user_ids) == [first.id]
  end

  test "cursor pagination uses the ids-first strategy when to-many joins are present" do
    query =
      User
      |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
      |> QueryBuilder.order_by(asc: :nickname)

    {_result, query_count} =
      with_repo_query_count(fn ->
        QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)
      end)

    assert query_count == 2
  end

  test "paginate raises when cursor pagination is disabled and to-many joins are present (unless unsafe opt-in)" do
    character_length = fn field, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      Ecto.Query.dynamic([{^binding, x}], fragment("character_length(?)", field(x, ^field)))
    end

    query =
      User
      |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
      |> QueryBuilder.order_by(asc: &character_length.(:nickname, &1))

    assert_raise ArgumentError, ~r/unsafe_sql_row_pagination/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)
    end

    %{paginated_entries: entries} =
      QueryBuilder.paginate(query, Repo,
        page_size: 2,
        cursor: nil,
        direction: :after,
        unsafe_sql_row_pagination?: true
      )

    assert is_list(entries)
  end

  test "paginate raises when cursor pagination is disabled (custom order_by), unless unsafe opt-in" do
    character_length = fn field, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      Ecto.Query.dynamic([{^binding, x}], fragment("character_length(?)", field(x, ^field)))
    end

    query =
      User
      |> QueryBuilder.order_by(asc: &character_length.(:nickname, &1))

    assert_raise ArgumentError, ~r/unsafe_sql_row_pagination/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)
    end

    %{paginated_entries: entries} =
      QueryBuilder.paginate(query, Repo,
        page_size: 2,
        cursor: nil,
        direction: :after,
        unsafe_sql_row_pagination?: true
      )

    assert is_list(entries)
  end

  test "cursor pagination with invalid direction" do
    query = QueryBuilder.order_by(User, asc: :nickname)

    assert_raise ArgumentError, ~r/cursor direction/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 3, direction: :invalid)
    end
  end

  test "cursor pagination raises on invalid cursor string" do
    query = QueryBuilder.order_by(User, asc: :nickname)

    assert_raise ArgumentError, ~r/invalid cursor/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 3, cursor: "not-a-cursor", direction: :after)
    end
  end

  test "cursor pagination raises on empty cursor string" do
    query = QueryBuilder.order_by(User, asc: :nickname)

    assert_raise ArgumentError, ~r/empty string/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 3, cursor: "", direction: :after)
    end
  end

  test "cursor pagination raises on unknown repo adapter when using :asc/:desc (NULL ordering)" do
    query = QueryBuilder.order_by(User, asc: :nickname)

    assert_raise ArgumentError, ~r/NULL|adapter/, fn ->
      QueryBuilder.paginate(query, UnknownAdapterRepo,
        page_size: 1,
        cursor: %{"nickname" => "Alice", "id" => 100},
        direction: :after
      )
    end
  end

  test "cursor pagination raises when cursor keys do not match the query order_by fields" do
    query = QueryBuilder.order_by(User, asc: :nickname)

    assert_raise ArgumentError, ~r/missing/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 3, cursor: %{"id" => 100}, direction: :after)
    end
  end

  test "cursor pagination raises on empty cursor map" do
    query = QueryBuilder.order_by(User, asc: :nickname)

    assert_raise ArgumentError, ~r/cursor map cannot be empty/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 3, cursor: %{}, direction: :after)
    end
  end

  test "limit" do
    all_users_but_bob =
      User
      |> QueryBuilder.where({:name, :ne, "Bob"})
      |> Repo.all()

    assert 8 == length(all_users_but_bob)

    three_users_not_bob =
      User
      |> QueryBuilder.where({:name, :ne, "Bob"})
      |> QueryBuilder.limit(3)
      |> Repo.all()

    assert 3 == length(three_users_not_bob)

    query = from(u in User, limit: 4)
    query = from(u in query, limit: 3)
    query = from(u in query, limit: 2)
    entries = Repo.all(query)
    assert 2 == length(entries)

    two_users_not_bob =
      User
      |> QueryBuilder.where({:name, :ne, "Bob"})
      |> QueryBuilder.limit(4)
      |> QueryBuilder.limit(3)
      |> QueryBuilder.limit(2)
      |> Repo.all()

    assert 2 == length(two_users_not_bob)

    two_users_not_bob =
      User
      |> QueryBuilder.where({:name, :ne, "Bob"})
      |> QueryBuilder.limit("2")
      |> Repo.all()

    assert 2 == length(two_users_not_bob)
  end

  test "offset" do
    all_users_count =
      User
      |> Repo.all()
      |> length()

    users_minus_three_count =
      User
      |> QueryBuilder.offset(3)
      |> Repo.all()
      |> length()

    assert all_users_count - 3 == users_minus_three_count

    users_minus_two_count =
      User
      |> QueryBuilder.offset(4)
      |> QueryBuilder.offset(3)
      |> QueryBuilder.offset(2)
      |> Repo.all()
      |> length()

    assert all_users_count - 2 == users_minus_two_count

    users_minus_two_count =
      User
      |> QueryBuilder.offset("2")
      |> Repo.all()
      |> length()

    assert all_users_count - 2 == users_minus_two_count
  end

  test "from_opts" do
    alice =
      User
      |> QueryBuilder.from_opts(
        where: [{:email, :equal_to, "alice@example.com"}],
        where: [name: "Alice", nickname: "Alice"],
        where: {[role: :permissions], name@permissions: "write"},
        order_by: {:authored_articles, asc: :title@authored_articles},
        preload: :authored_articles
      )
      |> Repo.one!()

    assert hd(alice.authored_articles).title == "ELIXIR V1.9 RELEASED"

    not_bob_count =
      User
      |> QueryBuilder.from_opts(where: [{:name, :ne, "Bob"}])
      |> Repo.all()
      |> length()

    skip_two_not_bob =
      User
      |> QueryBuilder.from_opts(
        where: [{:name, :ne, "Bob"}],
        offset: 2
      )
      |> Repo.all()

    assert not_bob_count - 2 == length(skip_two_not_bob)

    only_three_not_bob =
      User
      |> QueryBuilder.from_opts(
        where: [{:name, :ne, "Bob"}],
        limit: 3
      )
      |> Repo.all()

    assert 3 == length(only_three_not_bob)

    skip_two_only_one_not_bob =
      User
      |> QueryBuilder.from_opts(
        where: [{:name, :ne, "Bob"}],
        offset: 2,
        limit: 1
      )
      |> Repo.all()

    assert 1 == length(skip_two_only_one_not_bob)
  end

  test "extension" do
    # Call custom query functionality directly
    alice =
      User
      |> CustomQueryBuilder.where_initcap(:name, "alice")
      |> Repo.all()

    assert 1 == length(alice)

    # Test from_opts
    alice =
      User
      |> CustomQueryBuilder.from_opts(
        where_initcap: {:name, "alice"},
        where: {[role: :permissions], name@permissions: "write"},
        order_by: {:authored_articles, asc: :title@authored_articles},
        preload: :authored_articles
      )
      |> Repo.one!()

    assert hd(alice.authored_articles).title == "ELIXIR V1.9 RELEASED"
  end

  test "from_list raises and points to from_opts" do
    assert_raise ArgumentError, ~r/from_opts\/2/, fn ->
      QueryBuilder.from_list(User, where: [name: "Alice"])
    end
  end
end
