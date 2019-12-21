defmodule QueryBuilderTest do
  use ExUnit.Case
  import QueryBuilder.Factory
  alias QueryBuilder.Repo
  alias QueryBuilder.User
  require Ecto.Query

  doctest QueryBuilder

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(QueryBuilder.Repo)
  end

  setup :insert_demo_data

  def insert_demo_data(_) do
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

    author1 = insert(:user, %{name: "Alice", email: "alice@example.com", role: role_author})
    author2 = insert(:user, %{name: "Bob", email: "bob@example.com", role: role_author})
    reader = insert(:user, %{name: "Eric", email: "eric@example.com", role: role_reader})
    insert(:user, %{name: "Dave", email: "dave@example.com", role: role_admin})

    publisher =
      insert(:user, %{name: "Calvin", email: "calvin@example.com", role: role_publisher})

    title1 = "ELIXIR V1.9 RELEASED"
    title2 = "MINT, A NEW HTTP CLIENT FOR ELIXIR"
    title3 = "ELIXIR V1.8 RELEASED"

    articles = [
      insert(:article, %{title: title1, author: author1, publisher: publisher}),
      insert(:article, %{title: title2, author: author1, publisher: publisher}),
      insert(:article, %{title: title3, author: author2, publisher: publisher})
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

  test "where" do
    assert User
           |> QueryBuilder.where(name: "Bob")
           |> Repo.one()

    assert User
           |> QueryBuilder.where(name: "Bob", email: "bob@example.com")
           |> Repo.one()

    refute User
           |> QueryBuilder.where(name: "John")
           |> Repo.one()

    assert User
           |> QueryBuilder.where(name: "Bob")
           |> QueryBuilder.where(email: "bob@example.com")
           |> Repo.one()

    refute User
           |> QueryBuilder.where(name: "Bob")
           |> QueryBuilder.where(email: "alice@example.com")
           |> Repo.one()

    all_users_but_bob =
      User
      |> QueryBuilder.where({:name, :ne, "Bob"})
      |> Repo.all()

    assert 4 == length(all_users_but_bob)
  end

  test "where with assocs" do
    all_authors =
      User
      |> QueryBuilder.where(:role, name@role: "author")
      |> Repo.all()

    assert 2 == length(all_authors)

    all_users_with_write_role =
      User
      |> QueryBuilder.where([role: :permissions], name@permissions: "write")
      |> Repo.all()

    assert 2 == length(all_users_with_write_role)
  end

  test "order_by" do
    users_ordered_asc =
      User
      |> QueryBuilder.order_by(name: :asc)
      |> Repo.all()

    assert "Alice" == hd(users_ordered_asc).name

    users_ordered_desc =
      User
      |> QueryBuilder.order_by(name: :desc)
      |> Repo.all()

    assert "Eric" == hd(users_ordered_desc).name
  end

  test "order_by with assocs" do
    alice =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.order_by(:authored_articles, title@authored_articles: :asc)
      |> QueryBuilder.preload(:authored_articles)
      |> Repo.one!()

    assert hd(alice.authored_articles).title == "ELIXIR V1.9 RELEASED"

    alice =
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.order_by(:authored_articles, title@authored_articles: :desc)
      |> QueryBuilder.preload(:authored_articles)
      |> Repo.one!()

    assert hd(alice.authored_articles).title == "MINT, A NEW HTTP CLIENT FOR ELIXIR"
  end

  test "join" do
    # joining on authored articles but Bob is a publisher; not an author
    refute User
           |> QueryBuilder.where(:authored_articles, name: "Eric")
           |> Repo.one()

    assert User
           |> QueryBuilder.join(:authored_articles, :left)
           |> QueryBuilder.where(:authored_articles, name: "Eric")
           |> Repo.one()
  end

  test "preload" do
    preload = [
      {:authored_articles,
       [
         :article_likes,
         :article_stars,
         {:comments, [:comment_stars, comment_likes: :user]}
       ]},
      :published_articles
    ]

    query =
      Ecto.Query.from(u in User, join: r in assoc(u, :role))
      |> Ecto.Query.preload(^preload)
      |> Ecto.Query.preload([u, r], role: r)

    preload = [:role | preload]

    built_query = QueryBuilder.preload(User, preload)

    assert %{changed: :equal} = MapDiff.diff(Repo.all(query), Repo.all(built_query))
  end

  test "from list" do
    alice =
      User
      |> QueryBuilder.from_list(
        where: [name: "Alice", email: "alice@example.com"],
        order_by: {:authored_articles, title@authored_articles: :asc},
        preload: [:authored_articles]
      )
      |> Repo.one!()

    assert hd(alice.authored_articles).title == "ELIXIR V1.9 RELEASED"
  end
end
