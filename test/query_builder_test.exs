defmodule QueryBuilderTest do
  use ExUnit.Case
  import QueryBuilder.Factory
  alias QueryBuilder.{Repo, User, Article}
  require Ecto.Query

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

    author1 = insert(:user, %{id: 100, name: "Alice", email: "alice@example.com", role: role_author, nickname: "Alice"})
    author2 = insert(:user, %{id: 101, name: "Bob", email: "the_bob@example.com", role: role_author, nickname: "Bobby"})
    author3 = insert(:user, %{id: 103, name: "Charlie", email: "charlie@example.com", role: role_author, nickname: "Lee"})
    reader = insert(:user, %{id: 102, name: "Eric", email: nil, role: role_reader, nickname: "Eric", deleted: true})
    insert(:user, %{name: "Dave", email: "dave@example.com", role: role_admin, nickname: "Dave"})
    insert(:user, %{name: "Richard", email: "richard@example.com", role: role_admin, nickname: "Rich"})
    insert(:user, %{name: "An% we_ird %name_%", email: "weirdo@example.com", role: role_reader, nickname: "John"})
    insert(:user, %{name: "An_ we_ird %name_%", email: "weirdo@example.com", role: role_reader, nickname: "James"})

    publisher =
      insert(:user, %{name: "Calvin", email: "calvin@example.com", role: role_publisher, nickname: "Calvin"})

    insert(:acl, %{grantee: author1, grantor: author2})
    insert(:acl, %{grantee: reader, grantor: author1})

    title1 = "ELIXIR V1.9 RELEASED"
    title2 = "MINT, A NEW HTTP CLIENT FOR ELIXIR"
    title3 = "ELIXIR V1.8 RELEASED"
    title4 = "INTEGRATING TRAVEL WITH ELIXIR AT DUFFEL"

    articles = [
      insert(:article, %{title: title1, author: author1, publisher: publisher, tags: ["baz", "qux"]}),
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

  test "where with or groups" do
    result =
      User
      |> QueryBuilder.where([], [name: "Alice", deleted: false], or: [name: "Bob", deleted: false])
      |> Repo.all()

    assert 2 == length(result)

    result =
      User
      |> QueryBuilder.where(deleted: false)
      |> QueryBuilder.where([], [name: "Alice"], or: [name: "Bob"], or: [name: "Eric"], or: [name: "Dave"])
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
    text_equals_condition = fn (field, value, get_binding_fun) ->
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

  test "order_by with fragment" do
    character_length = fn (field, get_binding_fun) ->
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
           |> QueryBuilder.left_join(:authored_articles, title@authored_articles: "ELIXIR V1.9 RELEASED")
           |> Repo.one()

    refute User
           |> QueryBuilder.where(name: "Eric")
           |> QueryBuilder.where(:authored_articles, title@authored_articles: "ELIXIR V1.9 RELEASED")
           |> Repo.one()
  end

  test "preload" do
    query =
      Ecto.Query.from(u in User, join: r in assoc(u, :role), join: a in assoc(u, :authored_articles))
      |> Ecto.Query.where([u, r, a], a.title == ^"ELIXIR V1.9 RELEASED")
      |> Ecto.Query.preload([u, r, a], [:published_articles, authored_articles: {a, [:article_likes, :article_stars, {:comments, [:comment_stars, comment_likes: :user]}]}])
      |> Ecto.Query.preload([u, r, a], role: r)

    preload = [
      :role, :published_articles,
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
end
