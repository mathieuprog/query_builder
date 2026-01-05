defmodule QueryBuilderTest do
  use ExUnit.Case
  import QueryBuilder.Factory
  alias QueryBuilder.{Repo, User, Article, Event, CustomPkUser, CompositeUser}
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

  test "where with custom filter function supports association tokens" do
    lower_role_name_equals_admin = fn resolve ->
      {field, role_binding} = resolve.(:name@role)
      dynamic([{^role_binding, r}], fragment("lower(?)", field(r, ^field)) == ^"admin")
    end

    admin_users =
      User
      |> QueryBuilder.where(:role, [lower_role_name_equals_admin])
      |> Repo.all()
      |> Enum.map(& &1.name)
      |> Enum.sort()

    assert admin_users == ["Dave", "Richard"]
  end

  test "where_any/2 builds OR filters from groups" do
    users =
      User
      |> QueryBuilder.where_any([[name: "Alice"], [name: "Bob"]])
      |> Repo.all()
      |> Enum.map(& &1.name)
      |> Enum.sort()

    assert users == ["Alice", "Bob"]
  end

  test "where_any/3 supports association tokens" do
    users =
      User
      |> QueryBuilder.where_any(:role, [[name@role: "admin"], [name@role: "publisher"]])
      |> Repo.all()
      |> Enum.map(& &1.name)
      |> Enum.sort()

    assert users == ["Calvin", "Dave", "Richard"]
  end

  test "from_opts/2 supports where_any without a tuple" do
    users =
      User
      |> QueryBuilder.from_opts(where_any: [[name: "Alice"], [name: "Bob"]])
      |> Repo.all()
      |> Enum.map(& &1.name)
      |> Enum.sort()

    assert users == ["Alice", "Bob"]
  end

  test "where_exists avoids duplicate root rows for to-many association filters" do
    users_with_join_multiplication =
      User
      |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
      |> Repo.all()

    assert length(users_with_join_multiplication) >
             length(Enum.uniq_by(users_with_join_multiplication, & &1.id))

    users =
      User
      |> QueryBuilder.where_exists_subquery(
        [authored_articles: :comments],
        where: [title@comments: "It's great!"],
        scope: []
      )
      |> Repo.all()

    assert users == Enum.uniq_by(users, & &1.id)

    assert users
           |> Enum.map(& &1.name)
           |> Enum.sort() == ["Alice", "Bob", "Charlie"]
  end

  test "distinct(true) avoids duplicate root rows for to-many association filters" do
    users =
      User
      |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
      |> QueryBuilder.distinct(true)
      |> Repo.all()

    assert users == Enum.uniq_by(users, & &1.id)

    assert users
           |> Enum.map(& &1.name)
           |> Enum.sort() == ["Alice", "Bob", "Charlie"]
  end

  test "distinct/2 preserves Ecto DISTINCT ON ordering semantics" do
    query =
      User
      |> QueryBuilder.distinct(:nickname)
      |> QueryBuilder.order_by(asc: :email)

    {sql, _params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

    assert sql =~ "DISTINCT ON"
    assert sql =~ "\"nickname\""

    [_before_order_by, order_by_sql] = String.split(sql, "ORDER BY", parts: 2)
    {nickname_pos, _len} = :binary.match(order_by_sql, "\"nickname\"")
    {email_pos, _len} = :binary.match(order_by_sql, "\"email\"")
    assert nickname_pos < email_pos
  end

  test "group_by/3 supports association tokens" do
    rows =
      User
      |> QueryBuilder.group_by(:role, :name@role)
      |> QueryBuilder.select({:name@role, QueryBuilder.count(:id)})
      |> Repo.all()
      |> Enum.sort()

    assert rows == [{"admin", 2}, {"author", 3}, {"publisher", 1}, {"reader", 3}]
  end

  test "having/2 filters grouped queries" do
    rows =
      User
      |> QueryBuilder.group_by(:role, :name@role)
      |> QueryBuilder.having([{QueryBuilder.count(:id), :gt, 2}])
      |> QueryBuilder.select({:name@role, QueryBuilder.count(:id)})
      |> Repo.all()
      |> Enum.sort()

    assert rows == [{"author", 3}, {"reader", 3}]
  end

  test "having_any/2 filters grouped queries with an OR of AND groups" do
    rows =
      User
      |> QueryBuilder.group_by(:role, :name@role)
      |> QueryBuilder.having_any([
        [{QueryBuilder.count(:id), :gt, 2}],
        [{QueryBuilder.count(:id), :eq, 1}]
      ])
      |> QueryBuilder.select({:name@role, QueryBuilder.count(:id)})
      |> Repo.all()
      |> Enum.sort()

    assert rows == [{"author", 3}, {"publisher", 1}, {"reader", 3}]
  end

  test "aggregate helpers support min/max/sum/avg in grouped selects" do
    rows =
      User
      |> QueryBuilder.group_by(:role, :name@role)
      |> QueryBuilder.select(
        {:name@role, QueryBuilder.min(:id), QueryBuilder.max(:id), QueryBuilder.sum(:id),
         QueryBuilder.avg(:id)}
      )
      |> Repo.all()

    {_, min_id, max_id, sum_id, avg_id} =
      Enum.find(rows, fn {role_name, _, _, _, _} -> role_name == "admin" end)

    assert {min_id, max_id} == {200, 201}
    assert sum_id in [401, Decimal.new("401")]
    assert Decimal.equal?(avg_id, Decimal.new("200.5"))
  end

  test "count_distinct/1 generates COUNT(DISTINCT ...)" do
    role_author = Repo.get_by!(QueryBuilder.Role, name: "author")

    _ =
      insert(:user, %{
        id: 104,
        name: "Alice 2",
        email: "alice2@example.com",
        role: role_author,
        nickname: "Alice"
      })

    rows =
      User
      |> QueryBuilder.group_by(:role, :name@role)
      |> QueryBuilder.select(
        {:name@role, QueryBuilder.count(:id), QueryBuilder.count_distinct(:nickname)}
      )
      |> Repo.all()

    assert {"author", 4, 3} =
             Enum.find(rows, fn {role_name, _count, _distinct} -> role_name == "author" end)
  end

  test "where/2 fails fast when passed aggregate expressions (use having instead)" do
    assert_raise ArgumentError, ~r/aggregate.*WHERE.*HAVING/i, fn ->
      User
      |> QueryBuilder.where([{QueryBuilder.count(:id), :gt, 1}])
      |> Repo.all()
    end
  end

  test "having/2 fails fast on malformed aggregate tuples" do
    assert_raise ArgumentError, ~r/invalid having filter/i, fn ->
      User
      |> QueryBuilder.group_by(:role, :name@role)
      |> QueryBuilder.having([{QueryBuilder.count(:id), :gt, 2, :oops}])
      |> Repo.all()
    end
  end

  test "where_exists_subquery requires an explicit scope option" do
    assert_raise ArgumentError, ~r/requires an explicit `scope:` option/, fn ->
      User
      |> QueryBuilder.where_exists_subquery(
        :authored_articles,
        where: [title@authored_articles: "ELIXIR V1.9 RELEASED"]
      )
    end
  end

  test "where_not_exists_subquery requires an explicit scope option" do
    assert_raise ArgumentError, ~r/requires an explicit `scope:` option/, fn ->
      User
      |> QueryBuilder.where_not_exists_subquery(
        :authored_articles,
        where: [title@authored_articles: "ELIXIR V1.9 RELEASED"]
      )
    end
  end

  test "where_exists_subquery does not support or: (use where_any: instead)" do
    assert_raise ArgumentError, ~r/does not support `or:`/, fn ->
      User
      |> QueryBuilder.where_exists_subquery(
        [authored_articles: :comments],
        where: [title@comments: "It's great!"],
        or: [title@comments: "Not great!"],
        scope: []
      )
    end
  end

  test "where_not_exists_subquery does not support or: (use where_any: instead)" do
    assert_raise ArgumentError, ~r/does not support `or:`/, fn ->
      User
      |> QueryBuilder.where_not_exists_subquery(
        [authored_articles: :comments],
        where: [title@comments: "It's great!"],
        or: [title@comments: "Not great!"],
        scope: []
      )
    end
  end

  test "where_exists_subquery supports where_any" do
    users =
      User
      |> QueryBuilder.where_exists_subquery(
        [authored_articles: :comments],
        where_any: [[title@comments: "It's great!"], [title@comments: "Not great!"]],
        scope: []
      )
      |> Repo.all()

    assert users == Enum.uniq_by(users, & &1.id)
  end

  test "where_exists_subquery where + where_any applies where to all OR branches" do
    reader = Repo.get!(User, 102)
    bob = Repo.get!(User, 101)

    zed = insert(:user, %{name: "Zed"})
    zed_article = insert(:article, %{author: zed, publisher: zed})
    _ = insert(:comment, %{article: zed_article, user: bob, title: "Not great!"})

    users =
      User
      |> QueryBuilder.where_exists_subquery(
        [authored_articles: :comments],
        where: [user_id@comments: reader.id],
        where_any: [[title@comments: "It's great!"], [title@comments: "Not great!"]],
        scope: []
      )
      |> Repo.all()

    assert users == Enum.uniq_by(users, & &1.id)

    assert users
           |> Enum.map(& &1.name)
           |> Enum.sort() == ["Alice", "Bob", "Charlie"]
  end

  test "where_not_exists filters out roots that have matching associated rows" do
    users =
      User
      |> QueryBuilder.where_not_exists_subquery(
        [authored_articles: :comments],
        where: [title@comments: "It's great!"],
        scope: []
      )
      |> Repo.all()

    assert users == Enum.uniq_by(users, & &1.id)

    assert users
           |> Enum.map(& &1.name)
           |> Enum.sort() ==
             [
               "An% we_ird %name_%",
               "An_ we_ird %name_%",
               "Calvin",
               "Dave",
               "Eric",
               "Richard"
             ]
  end

  test "where_exists supports filtering on the first association level via tokens" do
    users =
      User
      |> QueryBuilder.where_exists_subquery(
        :authored_articles,
        where: [title@authored_articles: "ELIXIR V1.9 RELEASED"],
        scope: []
      )
      |> Repo.all()

    assert users == Enum.uniq_by(users, & &1.id)
    assert Enum.map(users, & &1.name) == ["Alice"]
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

  test "where supports :in with a subquery" do
    ids =
      from(u in User,
        where: u.id in [100, 101],
        select: u.id
      )

    users =
      User
      |> QueryBuilder.where({:id, :in, ids})
      |> Repo.all()

    assert users |> Enum.map(& &1.id) |> Enum.sort() == [100, 101]
  end

  test "where supports :in with a QueryBuilder subquery (QB-style select)" do
    ids =
      User
      |> QueryBuilder.where({:id, :in, [100, 101]})
      |> QueryBuilder.select(:id)

    users =
      User
      |> QueryBuilder.where({:id, :in, ids})
      |> Repo.all()

    assert users |> Enum.map(& &1.id) |> Enum.sort() == [100, 101]
  end

  test "where supports :in with an Ecto.SubQuery value" do
    ids_query =
      from(u in User,
        where: u.id in [100, 101],
        select: u.id
      )

    ids_subquery = Ecto.Query.subquery(ids_query)

    users =
      User
      |> QueryBuilder.where({:id, :in, ids_subquery})
      |> Repo.all()

    assert users |> Enum.map(& &1.id) |> Enum.sort() == [100, 101]
  end

  describe "full-path tokens (field@assoc@nested_assoc...)" do
    test "full-path tokens disambiguate repeated assoc names without renaming associations" do
      commenter = Repo.get!(User, 100)
      liker = Repo.get!(User, 101)

      article = insert(:article, %{author: commenter, publisher: commenter})
      _ = insert(:comment, %{article: article, user: commenter, title: "x"})
      _ = insert(:article_like, %{article: article, user: liker})

      assert_raise ArgumentError, ~r/ambiguous association token @user/i, fn ->
        Article
        |> QueryBuilder.where([comments: :user, article_likes: :user], name@user: commenter.name)
        |> Repo.all()
      end

      assert Article
             |> QueryBuilder.where([comments: :user, article_likes: :user],
               name@comments@user: commenter.name
             )
             |> Repo.all()
             |> Enum.map(& &1.id) == [article.id]

      assert Article
             |> QueryBuilder.where([comments: :user, article_likes: :user],
               name@article_likes@user: liker.name
             )
             |> Repo.all()
             |> Enum.map(& &1.id) == [article.id]
    end

    test "select supports full-path association tokens" do
      commenter = Repo.get!(User, 100)
      liker = Repo.get!(User, 101)

      article = insert(:article, %{author: commenter, publisher: commenter})
      _ = insert(:comment, %{article: article, user: commenter, title: "x"})
      _ = insert(:article_like, %{article: article, user: liker})

      assert Article
             |> QueryBuilder.where(id: article.id)
             |> QueryBuilder.select([comments: :user, article_likes: :user], %{
               comment_user_name: :name@comments@user,
               like_user_name: :name@article_likes@user
             })
             |> Repo.one() == %{comment_user_name: commenter.name, like_user_name: liker.name}
    end

    test "paginate supports full-path order_by tokens" do
      query =
        Article
        |> QueryBuilder.order_by([author: :role], asc: :name@author@role)

      %{paginated_entries: entries, pagination: %{cursor_for_entries_after: cursor}} =
        QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)

      assert length(entries) == 2
      assert is_binary(cursor) and cursor != ""
    end

    test "preload is not dropped when filtering via a full-path token" do
      commenter = insert(:user, %{name: "FullPathCommenter"})
      article = insert(:article, %{author: commenter, publisher: commenter})
      _ = insert(:comment, %{article: article, user: commenter, title: "x"})

      articles =
        Article
        |> QueryBuilder.where([comments: :user], name@comments@user: commenter.name)
        |> QueryBuilder.preload(:comments)
        |> Repo.all()

      assert Enum.any?(articles, &Ecto.assoc_loaded?(&1.comments))
    end
  end

  test "subquery/2 builds an Ecto.SubQuery from QueryBuilder ops" do
    ids_subquery =
      QueryBuilder.subquery(User,
        where: [{:id, :in, [100, 101]}],
        select: :id
      )

    users =
      User
      |> QueryBuilder.where({:id, :in, ids_subquery})
      |> Repo.all()

    assert users |> Enum.map(& &1.id) |> Enum.sort() == [100, 101]
  end

  test "where supports :not_in with a subquery" do
    ids =
      from(u in User,
        where: u.id in [100, 101],
        select: u.id
      )

    users =
      User
      |> QueryBuilder.where({:id, :not_in, ids})
      |> Repo.all()

    refute Enum.any?(users, &(&1.id in [100, 101]))
  end

  test "where supports :not_in with a QueryBuilder subquery (QB-style select)" do
    ids =
      User
      |> QueryBuilder.where({:id, :in, [100, 101]})
      |> QueryBuilder.select(:id)

    users =
      User
      |> QueryBuilder.where({:id, :not_in, ids})
      |> Repo.all()

    refute Enum.any?(users, &(&1.id in [100, 101]))
  end

  test "where raises an error for :in subqueries that don't return a single field" do
    ids =
      from(u in User,
        where: u.id in [100, 101]
      )

    assert_raise Ecto.QueryError, ~r/subquery must return a single field/, fn ->
      User
      |> QueryBuilder.where({:id, :in, ids})
      |> Repo.all()
    end
  end

  test "where raises an error for :in QueryBuilder subqueries that don't return a single field" do
    ids =
      User
      |> QueryBuilder.where({:id, :in, [100, 101]})

    assert_raise Ecto.QueryError, ~r/subquery must return a single field/, fn ->
      User
      |> QueryBuilder.where({:id, :in, ids})
      |> Repo.all()
    end
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

  test "left_join/4 fails fast for nested assoc paths (use left_join_leaf/4 or left_join_path/4)" do
    assert_raise ArgumentError, ~r/left_join_leaf|left_join_path/, fn ->
      User
      |> QueryBuilder.left_join(authored_articles: :comments)
      |> Repo.all()
    end
  end

  test "left_join_leaf/4 uses inner joins for intermediate hops (drops roots with no parent assoc)" do
    refute User
           |> QueryBuilder.left_join_leaf(authored_articles: :comments)
           |> QueryBuilder.where(name: "Eric")
           |> Repo.one()
  end

  test "left_join_path/4 uses left joins for intermediate hops (keeps roots with no parent assoc)" do
    assert User
           |> QueryBuilder.left_join_path(authored_articles: :comments)
           |> QueryBuilder.where(name: "Eric")
           |> Repo.one()
  end

  test "assoc_fields with no marker defaults to LEFT joins (does not drop optional belongs_to roots)" do
    no_role_user =
      Repo.insert!(%User{
        name: "NoRoleJoined",
        nickname: "NoRoleJoined",
        email: "norole-joined@example.com",
        deleted: false
      })

    role_binding = User._binding(:role)

    ecto_query =
      User
      |> QueryBuilder.order_by(:role, asc: :name@role)
      |> Ecto.Queryable.to_query()

    assert Enum.any?(ecto_query.joins, fn
             %Ecto.Query.JoinExpr{as: as, qual: :left} when as == role_binding -> true
             _ -> false
           end)

    users =
      User
      |> QueryBuilder.order_by(:role, asc: :name@role)
      |> Repo.all()

    assert Enum.any?(users, &(&1.id == no_role_user.id))
  end

  test "inner_join/2 emits INNER joins (drops roots when assoc is missing)" do
    no_role_user =
      Repo.insert!(%User{
        name: "NoRoleInnerJoined",
        nickname: "NoRoleInnerJoined",
        email: "norole-inner-joined@example.com",
        deleted: false
      })

    role_binding = User._binding(:role)

    ecto_query =
      User
      |> QueryBuilder.inner_join(:role)
      |> Ecto.Queryable.to_query()

    assert Enum.any?(ecto_query.joins, fn
             %Ecto.Query.JoinExpr{as: as, qual: :inner} when as == role_binding -> true
             _ -> false
           end)

    users =
      User
      |> QueryBuilder.inner_join(:role)
      |> Repo.all()

    refute Enum.any?(users, &(&1.id == no_role_user.id))
  end

  test "where_any/3 does not drop roots when OR mixes root and assoc predicates" do
    users =
      User
      |> QueryBuilder.where_any(:authored_articles, [
        [name: "Eric"],
        [title@authored_articles: "ELIXIR V1.9 RELEASED"]
      ])
      |> Repo.all()
      |> Enum.map(& &1.name)
      |> Enum.sort()

    assert users == ["Alice", "Eric"]
  end

  test "assoc? raises if the assoc is already INNER-joined upstream" do
    query =
      User._query()
      |> User._join(:inner, User, :role, [])

    assert_raise ArgumentError, ~r/joined as :inner.*requires :left/i, fn ->
      query
      |> QueryBuilder.order_by(:role?, asc: :name@role)
      |> Ecto.Queryable.to_query()
    end
  end

  test "assoc! raises if the assoc is already LEFT-joined upstream" do
    query =
      User._query()
      |> User._join(:left, User, :role, [])

    assert_raise ArgumentError, ~r/joined as :left.*requires :inner/i, fn ->
      query
      |> QueryBuilder.where(:role!, name@role: "admin")
      |> Ecto.Queryable.to_query()
    end
  end

  test "cannot use ! under ? on the same assoc path" do
    assert_raise ArgumentError, ~r/optional association path/i, fn ->
      User
      |> QueryBuilder.where([authored_articles?: :comments!], title@comments: "x")
      |> Ecto.Queryable.to_query()
    end
  end

  test "conflicting join guarantees for the same assoc raise" do
    assert_raise ArgumentError, ~r/conflicting join requirements/i, fn ->
      User
      |> QueryBuilder.where(:role?, name@role: "admin")
      |> QueryBuilder.order_by(:role!, asc: :name@role)
      |> Ecto.Queryable.to_query()
    end
  end

  test "pre-joined composition: reuses an existing binding only when QueryBuilder does not need to apply join filters" do
    query =
      User._query()
      |> User._join(:inner, User, :role, [])

    # No join filters required -> safe reuse.
    assert query
           |> QueryBuilder.where(:role, name@role: "author")
           |> Repo.all() != []

    # Join filters required -> fail-fast (can't rewrite the existing join's ON).
    assert_raise ArgumentError, ~r/cannot safely apply those filters/i, fn ->
      query
      |> QueryBuilder.left_join(:role, name@role: "author")
      |> Repo.all()
    end
  end

  test "pre-joined composition raises when a QueryBuilder left join is requested but the existing join qualifier is inner" do
    query =
      User._query()
      |> User._join(:inner, User, :role, [])

    assert_raise ArgumentError, ~r/joined as :inner.*requires :left/i, fn ->
      query
      |> QueryBuilder.left_join(:role)
      |> Repo.all()
    end
  end

  test "pre-joined composition raises when an existing named binding is not the expected association join" do
    role_binding = User._binding(:role)

    query =
      User._query()
      |> join(:inner, [{^User, u}], p in QueryBuilder.Permission,
        as: ^role_binding,
        on: p.role_id == u.role_id
      )

    assert_raise ArgumentError, ~r/expected.*association join|assoc\(/i, fn ->
      query
      |> QueryBuilder.where(:role, name@role: "author")
      |> Repo.all()
    end
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
      |> QueryBuilder.preload_through_join(:authored_articles)
      |> QueryBuilder.preload_separate(preload)

    assert %{changed: :equal} = MapDiff.diff(Repo.all(query), Repo.all(built_query))

    built_query =
      User
      |> QueryBuilder.preload_separate(preload)
      |> QueryBuilder.preload_through_join(:authored_articles)
      |> QueryBuilder.where(:authored_articles, title@authored_articles: "ELIXIR V1.9 RELEASED")

    assert %{changed: :equal} = MapDiff.diff(Repo.all(query), Repo.all(built_query))
  end

  test "preload_separate loads all association rows even when the assoc is joined for filtering" do
    alice = Repo.get!(User, 100)
    article = Repo.get_by!(Article, title: "ELIXIR V1.9 RELEASED")
    _ = insert(:comment, %{article: article, user: alice, title: "Not great!"})

    users =
      User
      |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
      |> QueryBuilder.preload_separate(authored_articles: :comments)
      |> Repo.all()

    alice = users |> Enum.uniq_by(& &1.id) |> Enum.find(&(&1.id == 100))
    article = Enum.find(alice.authored_articles, &(&1.title == "ELIXIR V1.9 RELEASED"))

    assert Enum.any?(article.comments, &(&1.title == "Not great!"))
  end

  test "preload_through_join loads only joined/filtered association rows" do
    alice = Repo.get!(User, 100)
    article = Repo.get_by!(Article, title: "ELIXIR V1.9 RELEASED")
    _ = insert(:comment, %{article: article, user: alice, title: "Not great!"})

    users =
      User
      |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
      |> QueryBuilder.preload_through_join(authored_articles: :comments)
      |> Repo.all()

    alice = users |> Enum.uniq_by(& &1.id) |> Enum.find(&(&1.id == 100))
    article = Enum.find(alice.authored_articles, &(&1.title == "ELIXIR V1.9 RELEASED"))

    refute Enum.any?(article.comments, &(&1.title == "Not great!"))
  end

  test "preload_separate_scoped applies `where:` to the association preload query" do
    user =
      User
      |> QueryBuilder.where(id: 100)
      |> QueryBuilder.preload_separate_scoped(:authored_articles,
        where: [title: "ELIXIR V1.9 RELEASED"]
      )
      |> Repo.one!()

    assert ["ELIXIR V1.9 RELEASED"] == Enum.map(user.authored_articles, & &1.title)
  end

  test "preload_separate_scoped applies `order_by:` to the association preload query" do
    user =
      User
      |> QueryBuilder.where(id: 100)
      |> QueryBuilder.preload_separate_scoped(:authored_articles, order_by: [desc: :title])
      |> Repo.one!()

    assert ["MINT, A NEW HTTP CLIENT FOR ELIXIR", "ELIXIR V1.9 RELEASED"] ==
             Enum.map(user.authored_articles, & &1.title)
  end

  test "preload_separate_scoped rejects assoc tokens and custom filters" do
    assert_raise ArgumentError, ~r/does not allow assoc tokens/, fn ->
      User
      |> QueryBuilder.preload_separate_scoped(:authored_articles,
        where: [title@comments: "It's great!"]
      )
    end

    assert_raise ArgumentError, ~r/does not accept custom filter functions/, fn ->
      User
      |> QueryBuilder.preload_separate_scoped(:authored_articles,
        where: [fn _resolve -> true end]
      )
    end

    assert_raise ArgumentError, ~r/does not allow assoc tokens/, fn ->
      User
      |> QueryBuilder.preload_separate_scoped(:authored_articles,
        where: [{:title, :eq, :title@comments@self}]
      )
    end
  end

  test "preload_separate_scoped conflicts with preload_through_join on the same association" do
    assert_raise ArgumentError, ~r/conflicting preload requirements/, fn ->
      User
      |> QueryBuilder.preload_through_join(:authored_articles)
      |> QueryBuilder.preload_separate_scoped(:authored_articles,
        where: [title: "ELIXIR V1.9 RELEASED"]
      )
      |> Ecto.Queryable.to_query()
    end
  end

  test "preload_separate_scoped raises if combined with nested preloads under the same association" do
    assert_raise ArgumentError,
                 ~r/cannot combine `preload_separate_scoped\/3` with nested preloads/,
                 fn ->
                   User
                   |> QueryBuilder.preload_separate_scoped(:authored_articles,
                     where: [title: "ELIXIR V1.9 RELEASED"]
                   )
                   |> QueryBuilder.preload_separate(authored_articles: :comments)
                   |> Ecto.Queryable.to_query()
                 end
  end

  test "preload supports mixed chains (join-preload a prefix, query-preload the rest)" do
    alice = Repo.get!(User, 100)

    article = Repo.get_by!(Article, title: "ELIXIR V1.9 RELEASED")
    _ = insert(:comment, %{article: article, user: alice, title: "Not great!"})

    not_great_only_article =
      insert(:article, %{title: "ONLY NOT GREAT", author: alice, publisher: alice})

    _ = insert(:comment, %{article: not_great_only_article, user: alice, title: "Not great!"})

    users =
      User
      |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
      |> QueryBuilder.preload_separate(authored_articles: :comments)
      |> QueryBuilder.preload_through_join(:authored_articles)
      |> Repo.all()

    alice = users |> Enum.uniq_by(& &1.id) |> Enum.find(&(&1.id == 100))

    refute Enum.any?(alice.authored_articles, &(&1.title == "ONLY NOT GREAT"))

    article = Enum.find(alice.authored_articles, &(&1.title == "ELIXIR V1.9 RELEASED"))
    assert Enum.any?(article.comments, &(&1.title == "Not great!"))
  end

  test "preload_through_join raises if the association isn't joined" do
    assert_raise ArgumentError, ~r/not joined/, fn ->
      User
      |> QueryBuilder.preload_through_join(:role)
      |> Repo.all()
    end
  end

  test "preload_through_join raises when a nested association in the path is not joined" do
    assert_raise ArgumentError, ~r/Article.*comments/, fn ->
      User
      |> QueryBuilder.where(:authored_articles, title@authored_articles: "ELIXIR V1.9 RELEASED")
      |> QueryBuilder.preload_through_join(authored_articles: :comments)
      |> Repo.all()
    end
  end

  test "preload_separate does not de-duplicate root rows under has_many joins" do
    assert_raise Ecto.MultipleResultsError, fn ->
      User
      |> QueryBuilder.where(name: "Alice")
      |> QueryBuilder.order_by(:authored_articles, asc: :title@authored_articles)
      |> QueryBuilder.preload_separate(:authored_articles)
      |> Repo.one!()
    end
  end

  test "preload does not drop root rows when preloading an optional belongs_to (nullable FK)" do
    no_role_user =
      Repo.insert!(%User{
        name: "NoRole",
        nickname: "NoRole",
        email: "norole@example.com",
        deleted: false
      })

    assert is_nil(no_role_user.role_id)

    ecto_query =
      User
      |> QueryBuilder.preload(:role)
      |> Ecto.Queryable.to_query()

    assert [] == ecto_query.joins

    users =
      User
      |> QueryBuilder.preload(:role)
      |> Repo.all()

    no_role_user = Enum.find(users, &(&1.name == "NoRole"))
    assert is_nil(no_role_user.role_id)
    assert is_nil(no_role_user.role)
    assert Ecto.assoc_loaded?(no_role_user.role)
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

  test "existing Ecto.Query inputs get the expected root named binding added (non-destructive)" do
    base_query =
      from(u in User,
        where: u.id == 100
      )

    results =
      base_query
      |> QueryBuilder.where(name: "Alice")
      |> Repo.all()

    assert length(results) == 1
    assert hd(results).id == 100
  end

  test "raises when the expected root binding name is already used by a join binding" do
    query =
      from(u in User,
        join: r in assoc(u, :role),
        as: ^User,
        where: r.name == "author"
      )

    assert_raise ArgumentError, ~r/expected root query.*already used/i, fn ->
      QueryBuilder.where(query, name: "Alice")
    end
  end

  test "raises when the root query already has a different named binding" do
    query =
      from(u in User,
        as: :user,
        where: u.id == 100
      )

    assert_raise ArgumentError, ~r/from\(query, as: \^.*User\)/, fn ->
      QueryBuilder.where(query, name: "Alice")
    end
  end

  test "raises when the root has a different named binding and the expected binding name is used by a join" do
    query =
      from(u in User,
        as: :user,
        join: r in assoc(u, :role),
        as: ^User,
        where: r.name == "author"
      )

    assert_raise ArgumentError, ~r/non-root named binding|cannot add it to the root/i, fn ->
      QueryBuilder.where(query, name: "Alice")
    end
  end

  test "raises ArgumentError with a helpful message when passed a non-queryable input" do
    assert_raise ArgumentError, ~r/expected an Ecto\.Queryable/i, fn ->
      QueryBuilder.where(:not_a_queryable, name: "Alice")
    end
  end

  test "accepts an existing Ecto query whose root is already named with the expected binding" do
    query =
      User._query()
      |> Ecto.Query.where([u], u.id == 100)

    results =
      query
      |> QueryBuilder.where(name: "Alice")
      |> Repo.all()

    assert length(results) == 1
    assert hd(results).id == 100
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

  test "unsafe SQL-row pagination avoids preloading the sentinel row when preloads are present" do
    character_length = fn field, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      Ecto.Query.dynamic([{^binding, x}], fragment("character_length(?)", field(x, ^field)))
    end

    query =
      User
      |> QueryBuilder.preload(:authored_articles)
      |> QueryBuilder.order_by(asc: &character_length.(:nickname, &1))

    {%{paginated_entries: [first]}, queries} =
      with_repo_queries(fn ->
        QueryBuilder.paginate(query, Repo,
          page_size: 1,
          cursor: nil,
          direction: :after,
          unsafe_sql_row_pagination?: true
        )
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

  test "unsafe SQL-row pagination preserves offset semantics when preloads are present" do
    character_length = fn field, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      Ecto.Query.dynamic([{^binding, x}], fragment("character_length(?)", field(x, ^field)))
    end

    base_query =
      User
      |> QueryBuilder.offset(1)
      |> QueryBuilder.order_by(asc: &character_length.(:nickname, &1))

    %{paginated_entries: [without_preload]} =
      QueryBuilder.paginate(base_query, Repo,
        page_size: 1,
        cursor: nil,
        direction: :after,
        unsafe_sql_row_pagination?: true
      )

    %{paginated_entries: [with_preload]} =
      base_query
      |> QueryBuilder.preload(:authored_articles)
      |> QueryBuilder.paginate(Repo,
        page_size: 1,
        cursor: nil,
        direction: :after,
        unsafe_sql_row_pagination?: true
      )

    assert with_preload.id == without_preload.id
    assert Ecto.assoc_loaded?(with_preload.authored_articles)
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

  test "paginate validates page_size and max_page_size (fail-fast)" do
    query = QueryBuilder.order_by(User, asc: :nickname)

    assert_raise ArgumentError, ~r/page_size/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 0)
    end

    assert_raise ArgumentError, ~r/page_size/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: -1)
    end

    assert_raise ArgumentError, ~r/page_size/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: "2")
    end

    assert_raise ArgumentError, ~r/max_page_size/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 2, max_page_size: 0)
    end

    assert_raise ArgumentError, ~r/max_page_size/, fn ->
      QueryBuilder.paginate(query, Repo, page_size: 2, max_page_size: "2")
    end
  end

  test "paginate raises on custom select expressions even in unsafe_sql_row_pagination?: true mode" do
    character_length = fn field, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      Ecto.Query.dynamic([{^binding, x}], fragment("character_length(?)", field(x, ^field)))
    end

    query =
      User
      |> QueryBuilder.order_by(asc: &character_length.(:nickname, &1))
      |> QueryBuilder.select(:name)

    assert_raise ArgumentError, ~r/custom select|root schema struct/, fn ->
      QueryBuilder.paginate(query, Repo,
        page_size: 2,
        cursor: nil,
        direction: :after,
        unsafe_sql_row_pagination?: true
      )
    end
  end

  describe "paginate primary key tie-breaker" do
    test "paginate uses the root schema primary key as the tie-breaker (non-:id PK)" do
      _ = Repo.insert!(%CustomPkUser{user_id: 1, name: "Alice"})
      _ = Repo.insert!(%CustomPkUser{user_id: 2, name: "Alice"})
      _ = Repo.insert!(%CustomPkUser{user_id: 3, name: "Bob"})

      query =
        CustomPkUser
        |> QueryBuilder.order_by(asc: :name)

      %{
        paginated_entries: [first],
        pagination: %{cursor_for_entries_after: cursor1}
      } = QueryBuilder.paginate(query, Repo, page_size: 1, cursor: nil, direction: :after)

      assert first.user_id == 1

      %{
        paginated_entries: [second],
        pagination: %{cursor_for_entries_after: cursor2}
      } = QueryBuilder.paginate(query, Repo, page_size: 1, cursor: cursor1, direction: :after)

      assert second.user_id == 2

      %{paginated_entries: [third]} =
        QueryBuilder.paginate(query, Repo, page_size: 1, cursor: cursor2, direction: :after)

      assert third.user_id == 3
    end

    test "paginate supports composite primary keys (ties broken by all PK fields) in ids-first mode" do
      _ = Repo.insert!(%CompositeUser{tenant_id: 1, user_id: 1, name: "Alice"})
      _ = Repo.insert!(%CompositeUser{tenant_id: 1, user_id: 2, name: "Alice"})
      _ = Repo.insert!(%CompositeUser{tenant_id: 2, user_id: 1, name: "Alice"})

      # Force ids-first pagination (single-query cursor pagination is only possible when
      # all joins are provably to-one association joins).
      base_query =
        from(u in CompositeUser,
          left_join: other in CompositeUser,
          on: other.user_id == u.user_id and other.tenant_id != u.tenant_id
        )

      query =
        base_query
        |> QueryBuilder.order_by(asc: :name)

      %{
        paginated_entries: [first],
        pagination: %{cursor_for_entries_after: cursor1}
      } = QueryBuilder.paginate(query, Repo, page_size: 1, cursor: nil, direction: :after)

      assert {first.tenant_id, first.user_id} == {1, 1}

      %{
        paginated_entries: [second],
        pagination: %{cursor_for_entries_after: cursor2}
      } = QueryBuilder.paginate(query, Repo, page_size: 1, cursor: cursor1, direction: :after)

      assert {second.tenant_id, second.user_id} == {1, 2}

      %{paginated_entries: [third]} =
        QueryBuilder.paginate(query, Repo, page_size: 1, cursor: cursor2, direction: :after)

      assert {third.tenant_id, third.user_id} == {2, 1}
    end
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

    assert_raise Ecto.QueryError, fn ->
      User
      |> QueryBuilder.where({:name, :ne, "Bob"})
      |> QueryBuilder.limit("2.0")
      |> Repo.all()
    end

    assert_raise Ecto.QueryError, fn ->
      User
      |> QueryBuilder.where({:name, :ne, "Bob"})
      |> QueryBuilder.limit("2abc")
      |> Repo.all()
    end
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

    assert_raise Ecto.QueryError, fn ->
      User
      |> QueryBuilder.offset("2.0")
      |> Repo.all()
    end

    assert_raise Ecto.QueryError, fn ->
      User
      |> QueryBuilder.offset("2abc")
      |> Repo.all()
    end
  end

  test "from_opts" do
    alice =
      User
      |> QueryBuilder.from_opts(
        where: [{:email, :equal_to, "alice@example.com"}],
        where: [name: "Alice", nickname: "Alice"],
        where: QueryBuilder.args([role: :permissions], name@permissions: "write"),
        order_by: QueryBuilder.args(:authored_articles, asc: :title@authored_articles),
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

  test "from_opts fails fast on invalid opts shapes (instead of FunctionClauseError)" do
    assert_raise ArgumentError, ~r/from_opts\/2.*keyword list/i, fn ->
      QueryBuilder.from_opts(User, %{where: [id: 100]})
    end

    assert_raise ArgumentError, ~r/from_opts\/2.*keyword list/i, fn ->
      QueryBuilder.from_opts(User, [:not_a_pair])
    end
  end

  test "from_opts fails fast on invalid where tuple shapes (instead of crashing later)" do
    assert_raise ArgumentError, ~r/from_opts\/2.*where.*tuple/i, fn ->
      QueryBuilder.from_opts(User, where: {:id})
    end
  end

  test "from_opts treats where filter tuples as data (does not expand tuples into args)" do
    ids =
      User
      |> QueryBuilder.from_opts(where: {:id, :in, [100, 101]}, order_by: [asc: :id])
      |> Repo.all()
      |> Enum.map(& &1.id)

    assert ids == [100, 101]
  end

  test "from_opts treats select tuples as data (does not expand tuples into args)" do
    assert User
           |> QueryBuilder.from_opts(where: [id: 100], select: {:id, :name})
           |> Repo.one() == {100, "Alice"}
  end

  test "from_opts raises on tuple values for multi-arg operations (use QueryBuilder.args)" do
    assert_raise ArgumentError, ~r/QueryBuilder\.args/i, fn ->
      User
      |> QueryBuilder.from_opts(order_by: {:authored_articles, asc: :title@authored_articles})
      |> Repo.all()
    end
  end

  test "from_opts raises on where assoc_fields tuple packs (use QueryBuilder.args)" do
    assert_raise ArgumentError, ~r/where: QueryBuilder\.args|QueryBuilder\.args/i, fn ->
      User
      |> QueryBuilder.from_opts(where: {[role: :permissions], name@permissions: "write"})
      |> Repo.all()
    end
  end

  test "from_opts raises on where assoc_fields tuple packs with assoc atom (use QueryBuilder.args)" do
    assert_raise ArgumentError, ~r/QueryBuilder\.args/i, fn ->
      User
      |> QueryBuilder.from_opts(where: {:role, [name@role: "admin"]})
      |> Repo.all()
    end
  end

  test "from_opts rejects non-builder operations like paginate/3 with an actionable error" do
    assert_raise ArgumentError, ~r/paginate/, fn ->
      QueryBuilder.from_opts(User, paginate: {Repo, [page_size: 1]})
    end
  end

  test "where/2 and order_by/2 fail fast on nil inputs (instead of crashing later)" do
    assert_raise ArgumentError, ~r/where\/2.*nil|filters.*nil/i, fn ->
      User
      |> QueryBuilder.where(nil)
      |> Repo.all()
    end

    assert_raise ArgumentError, ~r/order_by\/2.*nil|order_by.*nil/i, fn ->
      User
      |> QueryBuilder.order_by(nil)
      |> Repo.all()
    end
  end

  test "from_opts fails fast on nil operation values (instead of producing arity errors or crashes)" do
    assert_raise ArgumentError, ~r/from_opts\/2.*nil/i, fn ->
      QueryBuilder.from_opts(User, where: nil)
    end

    assert_raise ArgumentError, ~r/from_opts\/2.*nil/i, fn ->
      QueryBuilder.from_opts(User, order_by: nil)
    end
  end

  describe "select/select_merge" do
    test "select/2 selects a single field" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(:name)
             |> Repo.one() == "Alice"
    end

    test "select/2 selects a list of fields into a map keyed by the tokens" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select([:id, :name])
             |> Repo.one() == %{id: 100, name: "Alice"}
    end

    test "select/3 selects association fields via tokens" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(:role, %{user_id: :id, role_name: :name@role})
             |> Repo.one() == %{user_id: 100, role_name: "author"}
    end

    test "select/3 supports list selection with association tokens" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(:role, [:id, :name@role])
             |> Repo.one() == %{:name@role => "author", id: 100}
    end

    test "select/2 supports tuple selection" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select({:id, :name})
             |> Repo.one() == {100, "Alice"}
    end

    test "select/3 supports tuple selection with association tokens" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(:role, {:id, :name@role})
             |> Repo.one() == {100, "author"}
    end

    test "select/2 supports tuple selection with literal values via {:literal, value}" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select({:id, {:literal, "x"}})
             |> Repo.one() == {100, "x"}
    end

    test "select/2 accepts a keyword list (treated like a map)" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(user_id: :id, user_name: :name)
             |> Repo.one() == %{user_id: 100, user_name: "Alice"}
    end

    test "select/2 supports literal values via {:literal, value}" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(%{constant: {:literal, "x"}, name: :name})
             |> Repo.one() == %{constant: "x", name: "Alice"}
    end

    test "select/2 supports a custom select expression function escape hatch" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(fn resolve ->
               {field, binding} = resolve.(:name)
               dynamic([{^binding, u}], fragment("lower(?)", field(u, ^field)))
             end)
             |> Repo.one() == "alice"
    end

    test "select_merge can be called multiple times and accumulates (Ecto conformance)" do
      ecto_result =
        from(u in User,
          where: u.id == 100,
          select: %{user_id: u.id}
        )
        |> select_merge([u], %{user_name: u.name})
        |> select_merge([u], %{user_email: u.email})
        |> Repo.one()

      qb_result =
        User
        |> QueryBuilder.where(id: 100)
        |> QueryBuilder.select(%{user_id: :id})
        |> QueryBuilder.select_merge(%{user_name: :name})
        |> QueryBuilder.select_merge(%{user_email: :email})
        |> Repo.one()

      assert qb_result == ecto_result
    end

    test "Ecto and QueryBuilder both reject calling select twice" do
      assert_raise Ecto.Query.CompileError,
                   ~r/only one select expression is allowed in query/,
                   fn ->
                     Code.eval_string("""
                     import Ecto.Query
                     alias QueryBuilder.User

                     from(u in User)
                     |> select([u], %{id: u.id})
                     |> select([u], %{name: u.name})
                     """)
                   end

      assert_raise ArgumentError, ~r/only one select expression is allowed/, fn ->
        User
        |> QueryBuilder.select(%{id: :id})
        |> QueryBuilder.select(%{name: :name})
      end
    end

    test "Ecto and QueryBuilder both reject calling select after select_merge" do
      assert_raise Ecto.Query.CompileError,
                   ~r/only one select expression is allowed in query/,
                   fn ->
                     Code.eval_string("""
                     import Ecto.Query
                     alias QueryBuilder.User

                     from(u in User)
                     |> select_merge([u], %{id: u.id})
                     |> select([u], %{name: u.name})
                     |> select_merge([u], %{email: u.email})
                     |> select([u], %{nickname: u.nickname})
                     """)
                   end

      assert_raise ArgumentError, ~r/only one select expression is allowed/, fn ->
        User
        |> QueryBuilder.select_merge(%{id: :id})
        |> QueryBuilder.select(%{name: :name})
      end
    end

    test "select_merge/2 merges into an existing map select" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(%{user_id: :id})
             |> QueryBuilder.select_merge(%{user_name: :name})
             |> Repo.one() == %{user_id: 100, user_name: "Alice"}
    end

    test "select_merge/2 accepts a keyword list (treated like a map)" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(%{user_id: :id})
             |> QueryBuilder.select_merge(user_name: :name)
             |> Repo.one() == %{user_id: 100, user_name: "Alice"}
    end

    test "from_opts supports select with a keyword list" do
      assert User
             |> QueryBuilder.from_opts(
               where: [id: 100],
               select: [id: :id, name: :name]
             )
             |> Repo.one() == %{id: 100, name: "Alice"}
    end

    test "select_merge/3 supports association field tokens via explicit keys" do
      assert User
             |> QueryBuilder.where(id: 100)
             |> QueryBuilder.select(%{user_id: :id})
             |> QueryBuilder.select_merge(:role, %{role_name: :name@role})
             |> Repo.one() == %{user_id: 100, role_name: "author"}
    end

    test "select_merge raises when given a field@assoc token without an explicit key" do
      assert_raise ArgumentError, ~r/explicit key/, fn ->
        User
        |> QueryBuilder.select_merge(:name@role)
        |> Repo.all()
      end
    end
  end

  test "extension" do
    # Call custom query functionality directly
    alice =
      User
      |> CustomQueryBuilder.where_initcap(:name, "alice")
      |> Repo.all()

    assert 1 == length(alice)

    assert_raise ArgumentError, ~r/args/i, fn ->
      User
      |> CustomQueryBuilder.from_opts(where_initcap: {:name, "alice"})
      |> Repo.all()
    end

    # Test from_opts
    alice =
      User
      |> CustomQueryBuilder.from_opts(
        where_initcap: CustomQueryBuilder.args(:name, "alice"),
        where: CustomQueryBuilder.args([role: :permissions], name@permissions: "write"),
        order_by: CustomQueryBuilder.args(:authored_articles, asc: :title@authored_articles),
        preload: :authored_articles
      )
      |> Repo.one!()

    assert hd(alice.authored_articles).title == "ELIXIR V1.9 RELEASED"
  end

  test "extension from_opts fails fast on invalid opts shapes (instead of FunctionClauseError)" do
    assert_raise ArgumentError, ~r/from_opts\/2.*keyword list/i, fn ->
      CustomQueryBuilder.from_opts(User, %{where: [id: 100]})
    end

    assert_raise ArgumentError, ~r/from_opts\/2.*keyword list/i, fn ->
      CustomQueryBuilder.from_opts(User, [:not_a_pair])
    end
  end

  test "extension from_opts fails fast on invalid where tuple shapes" do
    assert_raise ArgumentError, ~r/from_opts\/2.*where.*tuple/i, fn ->
      CustomQueryBuilder.from_opts(User, where: {:id})
    end
  end

  describe "regressions / leftover cleanup" do
    test "limit/2 ensures the root named binding so chaining where works" do
      results =
        User
        |> QueryBuilder.limit(1)
        |> QueryBuilder.where(name: "Alice")
        |> Repo.all()

      assert length(results) == 1
    end

    test "offset/2 ensures the root named binding so chaining where works" do
      results =
        User
        |> QueryBuilder.offset(0)
        |> QueryBuilder.where(name: "Alice")
        |> Repo.all()

      assert length(results) == 1
    end

    test "where/4 treats `or: []` as a no-op instead of crashing" do
      results =
        User
        |> QueryBuilder.where([], [], or: [])
        |> Repo.all()

      assert length(results) == 9
    end

    test "preload does not get dropped when the association has nested assocs used only for filtering" do
      users =
        User
        |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
        |> QueryBuilder.preload(:authored_articles)
        |> Repo.all()

      assert Enum.any?(users, &Ecto.assoc_loaded?(&1.authored_articles))
    end

    test "preloading multiple associations to the same schema works (no collisions)" do
      calvin =
        User
        |> QueryBuilder.where(name: "Calvin")
        |> QueryBuilder.left_join(:authored_articles)
        |> QueryBuilder.left_join(:published_articles)
        |> QueryBuilder.preload_through_join([:authored_articles, :published_articles])
        |> Repo.one!()

      assert Ecto.assoc_loaded?(calvin.authored_articles)
      assert Ecto.assoc_loaded?(calvin.published_articles)
    end

    test "reuses already-joined bindings instead of raising" do
      query =
        User._query()
        |> User._join(:inner, User, :role, [])

      results =
        query
        |> QueryBuilder.where(:role, name@role: "author")
        |> Repo.all()

      assert results != []
    end

    test "paginate works when a cursor field value is nil" do
      query =
        User
        |> QueryBuilder.order_by(desc: :email, asc: :id)

      %{
        paginated_entries: [first],
        pagination: %{cursor_for_entries_after: cursor}
      } = QueryBuilder.paginate(query, Repo, page_size: 1)

      assert is_nil(first.email)

      %{paginated_entries: entries2} =
        QueryBuilder.paginate(query, Repo, page_size: 1, cursor: cursor, direction: :after)

      assert entries2 != []
    end

    test "paginate supports `*_nulls_*` order directions when paging before" do
      query = QueryBuilder.order_by(User, asc_nulls_last: :email)

      %{paginated_entries: entries} =
        QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :before)

      assert length(entries) == 2
    end

    test "paginate returns page_size unique roots even when to-many joins multiply SQL rows" do
      query =
        User
        |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
        |> QueryBuilder.order_by(asc: :id)

      %{paginated_entries: paginated_entries} =
        QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)

      assert length(paginated_entries) == 2
      assert length(Enum.uniq_by(paginated_entries, & &1.id)) == 2
    end

    test "unknown token binding errors are returned as ArgumentError with a helpful message" do
      assert_raise ArgumentError, ~r/@role|:role/, fn ->
        User
        |> QueryBuilder.where(name@role: "author")
        |> Repo.all()
      end
    end

    test "invalid association fields raise ArgumentError (not a generic string exception)" do
      assert_raise ArgumentError, ~r/permissions/, fn ->
        User
        |> QueryBuilder.where(:permissions, name@permissions: "read")
        |> Repo.all()
      end
    end

    test "`:in` validates that values are a list or subquery and raises Ecto.QueryError" do
      assert_raise Ecto.QueryError, fn ->
        User
        |> QueryBuilder.where({:name, :in, "Alice"})
        |> Repo.all()
      end
    end

    test "limit validates non-integer values and raises Ecto.QueryError" do
      assert_raise Ecto.QueryError, fn ->
        User
        |> QueryBuilder.limit(1.0)
        |> Repo.all()
      end
    end

    test "preloading joined associations supports chains deeper than 6" do
      assoc_chain =
        Enum.reduce(:lists.seq(7, 1, -1), nil, fn i, nested ->
          %{
            assoc_binding: String.to_atom("binding_#{i}"),
            assoc_field: String.to_atom("field_#{i}"),
            has_joined: true,
            preload: true,
            nested_assocs: if(nested, do: [nested], else: [])
          }
        end)

      assoc_list = [assoc_chain]

      _ = QueryBuilder.Query.Preload.preload(User._query(), assoc_list)

      assert true
    end

    test "from_opts/2 accepts nil (no-op)" do
      assert QueryBuilder.from_opts(User, nil) == User
    end

    test "from_opts/2 validates operation names (whitelist) with a clear error" do
      assert_raise ArgumentError, ~r/unknown operation/, fn ->
        QueryBuilder.from_opts(User, unknown_operation: [])
      end
    end
  end

  describe "real-world regression patterns" do
    test "preload does not drop roots for nullable belongs_to associations" do
      user_without_role =
        insert(:user, %{
          name: "NoRoleUser",
          nickname: "NoRoleUser",
          email: "norole@example.com",
          role: nil,
          role_id: nil
        })

      loaded =
        User
        |> QueryBuilder.where(id: user_without_role.id)
        |> QueryBuilder.preload(:role)
        |> Repo.one!()

      assert loaded.id == user_without_role.id
      assert is_nil(loaded.role)
      assert Ecto.assoc_loaded?(loaded.role)
    end

    test "preload does not drop roots for nullable has_one associations" do
      user_without_setting =
        insert(:user, %{
          name: "NoSettingUser",
          nickname: "NoSettingUser",
          email: "nosetting@example.com"
        })

      loaded =
        User
        |> QueryBuilder.where(id: user_without_setting.id)
        |> QueryBuilder.preload(:setting)
        |> Repo.one!()

      assert loaded.id == user_without_setting.id
      assert is_nil(loaded.setting)
      assert Ecto.assoc_loaded?(loaded.setting)
    end

    test "preloading one-of-many nullable FKs does not drop roots (polymorphic-ish header tables)" do
      author = Repo.get!(User, 100)
      commenter = Repo.get!(User, 101)

      article = insert(:article, %{author: author, publisher: author})
      comment = insert(:comment, %{article: article, user: commenter, title: "x"})

      article_event = Repo.insert!(%Event{kind: "article", article_id: article.id})
      comment_event = Repo.insert!(%Event{kind: "comment", comment_id: comment.id})

      events =
        Event
        |> QueryBuilder.order_by(asc: :id)
        |> QueryBuilder.preload([:article, :comment])
        |> Repo.all()

      assert Enum.map(events, & &1.id) == [article_event.id, comment_event.id]

      [first, second] = events

      assert first.kind == "article"
      assert Ecto.assoc_loaded?(first.article)
      assert first.article.id == article.id
      assert is_nil(first.comment) and Ecto.assoc_loaded?(first.comment)

      assert second.kind == "comment"
      assert Ecto.assoc_loaded?(second.comment)
      assert second.comment.id == comment.id
      assert is_nil(second.article) and Ecto.assoc_loaded?(second.article)
    end

    test "cursor pagination returns stable pages under joins (no overlaps, no missing roots)" do
      query =
        User
        |> QueryBuilder.where([authored_articles: :comments], title@comments: "It's great!")
        |> QueryBuilder.order_by(asc: :nickname)

      %{
        paginated_entries: page1,
        pagination: %{cursor_for_entries_after: cursor1}
      } = QueryBuilder.paginate(query, Repo, page_size: 2, cursor: nil, direction: :after)

      %{
        paginated_entries: page2,
        pagination: %{cursor_for_entries_after: cursor2}
      } = QueryBuilder.paginate(query, Repo, page_size: 2, cursor: cursor1, direction: :after)

      %{paginated_entries: page3} =
        QueryBuilder.paginate(query, Repo, page_size: 2, cursor: cursor2, direction: :after)

      assert Enum.map(page1, & &1.id) == [100, 101]
      assert Enum.map(page2, & &1.id) == [103]
      assert page3 == []

      all_ids = Enum.map(page1 ++ page2, & &1.id)
      assert all_ids == [100, 101, 103]
      assert Enum.uniq(all_ids) == all_ids
    end
  end

  test "from_list raises and points to from_opts" do
    assert_raise ArgumentError, ~r/from_opts\/2/, fn ->
      QueryBuilder.from_list(User, where: [name: "Alice"])
    end
  end
end
