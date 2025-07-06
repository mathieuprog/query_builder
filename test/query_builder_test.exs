defmodule QueryBuilderTest do
  use ExUnit.Case
  import QueryBuilder.Factory
  alias QueryBuilder.{Repo, User, Article}
  require Ecto.Query
  import Ecto.Query

  doctest QueryBuilder

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

  test "cursor pagination with invalid direction" do
    query = QueryBuilder.order_by(User, asc: :nickname)

    assert_raise ArgumentError, ~r/cursor direction/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 3, direction: :invalid)
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

  test "from list" do
    alice =
      User
      |> QueryBuilder.from_list(
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
      |> QueryBuilder.from_list(where: [{:name, :ne, "Bob"}])
      |> Repo.all()
      |> length()

    skip_two_not_bob =
      User
      |> QueryBuilder.from_list(
        where: [{:name, :ne, "Bob"}],
        offset: 2
      )
      |> Repo.all()

    assert not_bob_count - 2 == length(skip_two_not_bob)

    only_three_not_bob =
      User
      |> QueryBuilder.from_list(
        where: [{:name, :ne, "Bob"}],
        limit: 3
      )
      |> Repo.all()

    assert 3 == length(only_three_not_bob)

    skip_two_only_one_not_bob =
      User
      |> QueryBuilder.from_list(
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

    # Test from_list
    alice =
      User
      |> CustomQueryBuilder.from_list(
        where_initcap: {:name, "alice"},
        where: {[role: :permissions], name@permissions: "write"},
        order_by: {:authored_articles, asc: :title@authored_articles},
        preload: :authored_articles
      )
      |> Repo.one!()

    assert hd(alice.authored_articles).title == "ELIXIR V1.9 RELEASED"
  end

  test "extension with select and select_merge" do
    # Test that select is available through the extension
    users =
      User
      |> CustomQueryBuilder.select([:id, :name])
      |> CustomQueryBuilder.limit(1)
      |> Repo.all()

    assert length(users) == 1
    user = hd(users)
    assert Map.has_key?(user, :id)
    assert Map.has_key?(user, :name)
    assert Map.get(user, :email) == nil

    # Test select_merge through extension
    users =
      User
      |> CustomQueryBuilder.select(%{id: :id})
      |> CustomQueryBuilder.select_merge(%{name: :name})
      |> CustomQueryBuilder.limit(1)
      |> Repo.all()

    assert length(users) == 1
    user = hd(users)
    assert Map.has_key?(user, :id)
    assert Map.has_key?(user, :name)
    assert map_size(user) == 2
  end

  test "extension from_list with select" do
    users =
      User
      |> CustomQueryBuilder.from_list(
        where: [deleted: false],
        select: [:id, :name],
        order_by: [asc: :id],
        limit: 2
      )
      |> Repo.all()

    assert length(users) == 2
    assert Map.has_key?(hd(users), :id)
    assert Map.has_key?(hd(users), :name)
    assert Map.get(hd(users), :email) == nil
  end

  test "select with list of fields" do
    users =
      User
      |> QueryBuilder.select([:id, :name])
      |> Repo.all()

    assert length(users) == 9

    # Check that only selected fields are loaded
    first_user = hd(users)
    assert Map.has_key?(first_user, :id)
    assert Map.has_key?(first_user, :name)
    assert Map.get(first_user, :email) == nil
  end

  test "select with map" do
    users =
      User
      |> QueryBuilder.select(%{user_id: :id, user_name: :name})
      |> Repo.all()

    assert length(users) == 9

    first_user = hd(users)
    assert Map.has_key?(first_user, :user_id)
    assert Map.has_key?(first_user, :user_name)
    assert not Map.has_key?(first_user, :id)
    assert not Map.has_key?(first_user, :name)
  end

  test "select with tuple" do
    users =
      User
      |> QueryBuilder.select({:id, :name})
      |> QueryBuilder.order_by(asc: :id)
      |> Repo.all()

    assert length(users) == 9
    assert is_tuple(hd(users))
    assert tuple_size(hd(users)) == 2
    {id, name} = hd(users)
    assert id == 100
    assert name == "Alice"
  end

  test "select single field" do
    names =
      User
      |> QueryBuilder.select(:name)
      |> QueryBuilder.order_by(asc: :name)
      |> Repo.all()

    assert length(names) == 9
    assert hd(names) == "Alice"
  end

  test "select with associations" do
    users =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.select([:role], [:id, :name, :name@role])
      |> Repo.all()

    assert length(users) == 1

    user = hd(users)
    assert Map.has_key?(user, :id)
    assert Map.has_key?(user, :name)
    assert Map.has_key?(user, :name@role)
    # The association field should be available in the result
    assert user.id == 100
    assert user.name == "Alice"
    assert user[:name@role] == "author"
  end

  test "select_merge basic usage" do
    query =
      User
      |> QueryBuilder.select(%{id: :id})
      |> QueryBuilder.select_merge(%{name: :name})

    users = Repo.all(query)

    assert length(users) == 9
    first_user = hd(users)
    assert Map.has_key?(first_user, :id)
    assert Map.has_key?(first_user, :name)
    assert map_size(first_user) == 2
  end

  test "select_merge with associations" do
    query =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.select(:role, %{user_id: :id})
      |> QueryBuilder.select_merge(:role, %{role_name: :name@role})

    users = Repo.all(query)

    assert length(users) == 1
    user = hd(users)
    assert user.user_id == 100
    assert user.role_name == "author"
  end

  test "select_merge with list" do
    query =
      User
      |> QueryBuilder.select(%{})
      |> QueryBuilder.select_merge([:id, :name])

    users = Repo.all(query)

    assert length(users) == 9
    first_user = hd(users)
    assert Map.has_key?(first_user, :id)
    assert Map.has_key?(first_user, :name)
  end

  test "select with custom function" do
    users =
      User
      |> QueryBuilder.select(fn get_binding_fun ->
        {field, binding} = get_binding_fun.(:name)
        Ecto.Query.dynamic([{^binding, x}], %{upper_name: fragment("UPPER(?)", field(x, ^field))})
      end)
      |> QueryBuilder.limit(1)
      |> Repo.all()

    assert length(users) == 1
    user = hd(users)
    assert Map.has_key?(user, :upper_name)
    assert user.upper_name == String.upcase(user.upper_name)
  end

  test "select in from_list" do
    users =
      User
      |> QueryBuilder.from_list(
        where: [deleted: false],
        select: [:id, :name],
        order_by: [asc: :name],
        limit: 3
      )
      |> Repo.all()

    assert length(users) == 3
    assert Map.has_key?(hd(users), :id)
    assert Map.has_key?(hd(users), :name)
    assert Map.get(hd(users), :email) == nil
  end

  test "select with string field names" do
    users =
      User
      |> QueryBuilder.select("name")
      |> QueryBuilder.order_by(asc: :name)
      |> Repo.all()

    assert length(users) == 9
    assert hd(users) == "Alice"

    # List with strings
    users =
      User
      |> QueryBuilder.select(["id", "name"])
      |> Repo.all()

    assert length(users) == 9
    first_user = hd(users)
    assert Map.has_key?(first_user, :id)
    assert Map.has_key?(first_user, :name)
    assert Map.get(first_user, :email) == nil

    # Map with string keys
    users =
      User
      |> QueryBuilder.select(%{"user_id" => "id", "user_name" => "name"})
      |> Repo.all()

    assert length(users) == 9
    first_user = hd(users)
    assert Map.has_key?(first_user, "user_id")
    assert Map.has_key?(first_user, "user_name")
  end

  test "select with invalid field raises error" do
    assert_raise Ecto.QueryError, ~r/field `nonexistent_field` in `select` does not exist/, fn ->
      User
      |> QueryBuilder.select([:id, :nonexistent_field])
      |> Repo.all()
    end
  end

  test "select with unsupported type raises error" do
    assert_raise FunctionClauseError, fn ->
      User
      |> QueryBuilder.select(123)
      |> Repo.all()
    end
  end

  test "select_merge with single field" do
    query =
      User
      |> QueryBuilder.select(%{id: :id})
      |> QueryBuilder.select_merge(:name)

    users = Repo.all(query)

    assert length(users) == 9
    first_user = hd(users)
    assert Map.has_key?(first_user, :id)
    assert Map.has_key?(first_user, :name)
  end

  test "select_merge with invalid selection type" do
    assert_raise ArgumentError, ~r/select_merge expects a map, list, or single field/, fn ->
      User
      |> QueryBuilder.select_merge({:id, :name})
      |> Repo.all()
    end

    assert_raise ArgumentError, ~r/select_merge expects a map, list, or single field/, fn ->
      User
      |> QueryBuilder.select_merge(123)
      |> Repo.all()
    end
  end

  test "select with associations and string fields" do
    users =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.select(["role"], ["id", "name", "name@role"])
      |> Repo.all()

    assert length(users) == 1

    user = hd(users)
    assert Map.has_key?(user, :id)
    assert Map.has_key?(user, :name)
    assert Map.has_key?(user, :name@role)
    assert user[:name@role] == "author"
  end

  test "select with nested associations" do
    users =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.select([role: :permissions], [:id, :name, :name@role, :name@permissions])
      |> Repo.all()

    # Alice's role (author) has 2 permissions: read and write
    assert length(users) == 2

    # Check that we get the permissions
    permissions = Enum.map(users, & &1[:name@permissions]) |> Enum.sort()
    assert permissions == ["read", "write"]

    # All results should have the same user data
    Enum.each(users, fn user ->
      assert user.id == 100
      assert user.name == "Alice"
      assert user[:name@role] == "author"
      assert Map.has_key?(user, :name@permissions)
    end)
  end

  test "select with multiple associations" do
    articles =
      Article
      |> QueryBuilder.where(:author, name@author: "Alice")
      |> QueryBuilder.select([:author, :publisher], [
        :id,
        :title,
        :name@author,
        :name@publisher
      ])
      |> Repo.all()

    assert length(articles) == 2
    article = hd(articles)
    assert article[:name@author] == "Alice"
    assert article[:name@publisher] == "Calvin"
  end

  test "select tuple with association fields from same binding" do
    # This should work - both fields from root
    users =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.select({:id, :name})
      |> Repo.all()

    assert length(users) == 1
    assert {100, "Alice"} = hd(users)
  end

  test "select tuple with fields from different bindings raises error" do
    # This should fail - fields from different bindings
    assert_raise ArgumentError,
                 ~r/Tuple selection with fields from different associations is not yet supported/,
                 fn ->
                   User
                   |> QueryBuilder.where(name: "Alice")
                   |> QueryBuilder.select(:role, {:name, :name@role})
                   |> Repo.all()
                 end
  end

  test "select with invalid association name raises error" do
    assert_raise RuntimeError, ~r/association :nonexistent not found/, fn ->
      User
      |> QueryBuilder.select([:nonexistent], [:id, :name])
      |> Repo.all()
    end
  end

  test "select_merge with string fields" do
    query =
      User
      |> QueryBuilder.select(%{id: :id})
      |> QueryBuilder.select_merge("name")

    users = Repo.all(query)

    assert length(users) == 9
    first_user = hd(users)
    assert Map.has_key?(first_user, :id)
    assert Map.has_key?(first_user, :name)
  end

  test "select_merge with string association fields" do
    query =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.select(["role"], %{user_id: :id})
      |> QueryBuilder.select_merge(["role"], ["name@role"])

    users = Repo.all(query)

    assert length(users) == 1
    user = hd(users)
    assert user.user_id == 100
    assert user[:name@role] == "author"
  end

  test "select with empty list returns empty map" do
    users =
      User
      |> QueryBuilder.select([])
      |> Repo.all()

    assert length(users) == 9
    assert hd(users) == %{}
  end

  test "select with empty map returns empty map" do
    users =
      User
      |> QueryBuilder.select(%{})
      |> Repo.all()

    assert length(users) == 9
    assert hd(users) == %{}
  end

  test "select_merge after empty select" do
    query =
      User
      |> QueryBuilder.select(%{})
      |> QueryBuilder.select_merge(%{id: :id, name: :name})

    users = Repo.all(query)

    assert length(users) == 9
    first_user = hd(users)
    assert Map.has_key?(first_user, :id)
    assert Map.has_key?(first_user, :name)
    assert map_size(first_user) == 2
  end

  test "from_list with select and select_merge" do
    users =
      User
      |> QueryBuilder.from_list(
        where: [deleted: false],
        select: %{id: :id},
        select_merge: %{name: :name, email: :email},
        order_by: [asc: :id],
        limit: 3
      )
      |> Repo.all()

    assert length(users) == 3
    first_user = hd(users)
    assert Map.has_key?(first_user, :id)
    assert Map.has_key?(first_user, :name)
    assert Map.has_key?(first_user, :email)
    assert map_size(first_user) == 3
  end

  test "select tuple with mixed string and atom fields" do
    users =
      User
      |> QueryBuilder.select({:id, "name"})
      |> QueryBuilder.order_by(asc: :id)
      |> Repo.all()

    assert length(users) == 9
    assert is_tuple(hd(users))
    assert tuple_size(hd(users)) == 2
    {id, name} = hd(users)
    assert id == 100
    assert name == "Alice"
  end

  test "select with map mixing atoms and strings" do
    users =
      User
      |> QueryBuilder.select(%{"user_name" => "name", user_id: :id})
      |> QueryBuilder.order_by(asc: :id)
      |> Repo.all()

    assert length(users) == 9
    first_user = hd(users)
    assert Map.has_key?(first_user, :user_id)
    assert Map.has_key?(first_user, "user_name")
    assert first_user.user_id == 100
    assert first_user["user_name"] == "Alice"
  end

  test "select with 3-field tuple" do
    users =
      User
      |> QueryBuilder.select({:id, :name, :email})
      |> QueryBuilder.order_by(asc: :id)
      |> Repo.all()

    assert length(users) == 9
    assert is_tuple(hd(users))
    assert tuple_size(hd(users)) == 3
    {id, name, email} = hd(users)
    assert id == 100
    assert name == "Alice"
    assert email == "alice@example.com"
  end

  test "select tuple with more than 3 fields raises error" do
    assert_raise ArgumentError, ~r/Tuple selection currently supports only 2 or 3 fields/, fn ->
      User
      |> QueryBuilder.select({:id, :name, :email, :nickname})
      |> Repo.all()
    end
  end

  test "select tuple with fields from different associations raises error" do
    assert_raise ArgumentError,
                 ~r/Tuple selection with fields from different associations is not yet supported/,
                 fn ->
                   User
                   |> QueryBuilder.where(name: "Alice")
                   |> QueryBuilder.select(
                     [:role, :authored_articles],
                     {:name, :name@role, :title@authored_articles}
                   )
                   |> Repo.all()
                 end
  end

  test "select with custom function and associations" do
    users =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.select(:role, fn get_binding_fun ->
        {name_field, name_binding} = get_binding_fun.(:name)
        {role_field, role_binding} = get_binding_fun.(:name@role)

        Ecto.Query.dynamic(
          [{^name_binding, n}, {^role_binding, r}],
          %{
            upper_name: fragment("UPPER(?)", field(n, ^name_field)),
            upper_role: fragment("UPPER(?)", field(r, ^role_field))
          }
        )
      end)
      |> Repo.all()

    assert length(users) == 1
    user = hd(users)
    assert user.upper_name == "ALICE"
    assert user.upper_role == "AUTHOR"
  end

  test "select_merge with custom function" do
    users =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.select(%{id: :id})
      |> QueryBuilder.select_merge(fn get_binding_fun ->
        {field, binding} = get_binding_fun.(:name)
        Ecto.Query.dynamic([{^binding, x}], %{lower_name: fragment("LOWER(?)", field(x, ^field))})
      end)
      |> Repo.all()

    assert length(users) == 1
    user = hd(users)
    assert Map.has_key?(user, :id)
    assert Map.has_key?(user, :lower_name)
    assert user.lower_name == "alice"
  end

  test "select with list creates map with all fields" do
    users =
      User
      |> QueryBuilder.select([:email, :name, :id])
      |> QueryBuilder.limit(1)
      |> Repo.all()

    user = hd(users)
    # All fields should be present
    assert Map.has_key?(user, :email)
    assert Map.has_key?(user, :name)
    assert Map.has_key?(user, :id)
    assert map_size(user) == 3
  end
end
