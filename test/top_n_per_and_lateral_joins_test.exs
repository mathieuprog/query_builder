defmodule QueryBuilder.TopNPerAndLateralJoinsTest do
  use ExUnit.Case, async: true

  import Ecto.Query, only: [from: 2]
  import QueryBuilder.Factory

  alias QueryBuilder.{Article, CompositeUser, CustomPkUser, Repo, User}

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
  end

  describe "top_n_per/2 and first_per/2" do
    test "keeps N rows per partition" do
      author1 = insert(:user, %{name: "Author1"})
      author2 = insert(:user, %{name: "Author2"})

      a1 = insert(:article, author: author1, publisher: author1)
      a2 = insert(:article, author: author1, publisher: author1)
      a3 = insert(:article, author: author1, publisher: author1)

      b1 = insert(:article, author: author2, publisher: author2)
      b2 = insert(:article, author: author2, publisher: author2)

      results =
        Article
        |> QueryBuilder.top_n_per(partition_by: [:author_id], order_by: [desc: :id], n: 2)
        |> Repo.all()

      by_author = Enum.group_by(results, & &1.author_id)

      expected_author1_ids =
        [a1.id, a2.id, a3.id]
        |> Enum.sort(:desc)
        |> Enum.take(2)

      expected_author2_ids =
        [b1.id, b2.id]
        |> Enum.sort(:desc)
        |> Enum.take(2)

      assert by_author[author1.id] |> Enum.map(& &1.id) |> Enum.sort(:desc) ==
               expected_author1_ids

      assert by_author[author2.id] |> Enum.map(& &1.id) |> Enum.sort(:desc) ==
               expected_author2_ids
    end

    test "first_per is top_n_per with n: 1" do
      author1 = insert(:user, %{name: "Author1"})
      author2 = insert(:user, %{name: "Author2"})

      a1 = insert(:article, author: author1, publisher: author1)
      a2 = insert(:article, author: author1, publisher: author1)

      b1 = insert(:article, author: author2, publisher: author2)
      b2 = insert(:article, author: author2, publisher: author2)

      results =
        Article
        |> QueryBuilder.first_per(partition_by: [:author_id], order_by: [desc: :id])
        |> Repo.all()

      by_author = Enum.group_by(results, & &1.author_id)
      assert Enum.map(by_author[author1.id], & &1.id) == [max(a1.id, a2.id)]
      assert Enum.map(by_author[author2.id], & &1.id) == [max(b1.id, b2.id)]
    end

    test "first_per rejects n != 1" do
      assert_raise ArgumentError, ~r/first_per\/2 is `top_n_per\/2` with `n: 1`/i, fn ->
        Article
        |> QueryBuilder.first_per(partition_by: [:author_id], order_by: [desc: :id], n: 2)
        |> Repo.all()
      end
    end

    test "uses DISTINCT ON by default for n: 1 on Postgres" do
      query =
        Article
        |> QueryBuilder.top_n_per(
          partition_by: [:author_id],
          order_by: [desc: :id],
          n: 1
        )

      {sql, _params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

      assert sql =~ "DISTINCT ON"
      refute Regex.match?(~r/row_number/i, sql)
    end

    test "rejects prefer_distinct_on? (Postgres-only)" do
      assert_raise ArgumentError, ~r/unknown options:.*prefer_distinct_on\?/i, fn ->
        query =
          Article
          |> QueryBuilder.top_n_per(
            partition_by: [:author_id],
            order_by: [desc: :id],
            n: 1,
            prefer_distinct_on?: false
          )

        Ecto.Adapters.SQL.to_sql(:all, Repo, query)
      end
    end

    test "disable_distinct_on?: true forces the window-function plan for n: 1" do
      query =
        Article
        |> QueryBuilder.top_n_per(
          partition_by: [:author_id],
          order_by: [desc: :id],
          n: 1,
          disable_distinct_on?: true
        )

      {sql, _params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

      refute sql =~ "DISTINCT ON"
      assert Regex.match?(~r/row_number/i, sql)
    end

    test "disable_distinct_on? must be a boolean" do
      assert_raise ArgumentError, ~r/disable_distinct_on\?.*boolean/i, fn ->
        query =
          Article
          |> QueryBuilder.top_n_per(
            partition_by: [:author_id],
            order_by: [desc: :id],
            n: 1,
            disable_distinct_on?: :nope
          )

        Ecto.Adapters.SQL.to_sql(:all, Repo, query)
      end
    end

    test "can prefer DISTINCT ON when distinct is explicitly false" do
      base = from(a in Article, distinct: false)

      query =
        base
        |> QueryBuilder.top_n_per(
          partition_by: [:author_id],
          order_by: [desc: :id],
          n: 1
        )

      {sql, _params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

      assert sql =~ "DISTINCT ON"
      refute Regex.match?(~r/row_number/i, sql)
    end

    test "does not use DISTINCT ON for n > 1" do
      query =
        Article
        |> QueryBuilder.top_n_per(
          partition_by: [:author_id],
          order_by: [desc: :id],
          n: 2
        )

      {sql, _params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

      refute sql =~ "DISTINCT ON"
      assert Regex.match?(~r/row_number/i, sql)
    end

    test "requires a primary-key tie-breaker in order_by (custom PK schemas too)" do
      assert_raise ArgumentError, ~r/include the root primary key fields as a tie-breaker/i, fn ->
        CustomPkUser
        |> QueryBuilder.top_n_per(
          partition_by: [:name],
          order_by: [desc: :inserted_at],
          n: 1
        )
        |> Repo.all()
      end
    end

    test "requires every composite primary key field as a tie-breaker" do
      assert_raise ArgumentError, ~r/missing: \[:tenant_id\]/i, fn ->
        CompositeUser
        |> QueryBuilder.top_n_per(
          partition_by: [:tenant_id],
          order_by: [desc: :user_id],
          n: 1
        )
        |> Repo.all()
      end
    end

    test "works for composite primary key schemas when order_by includes all PK fields" do
      _ = Repo.insert!(%CompositeUser{tenant_id: 1, user_id: 1, name: "t1-u1"})
      _ = Repo.insert!(%CompositeUser{tenant_id: 1, user_id: 2, name: "t1-u2"})
      _ = Repo.insert!(%CompositeUser{tenant_id: 2, user_id: 1, name: "t2-u1"})
      _ = Repo.insert!(%CompositeUser{tenant_id: 2, user_id: 3, name: "t2-u3"})

      results =
        CompositeUser
        |> QueryBuilder.top_n_per(
          partition_by: [:tenant_id],
          order_by: [desc: :user_id, desc: :tenant_id],
          n: 1
        )
        |> Repo.all()

      assert Enum.map(results, fn row -> {row.tenant_id, row.user_id} end) |> Enum.sort() ==
               [{1, 2}, {2, 3}]
    end

    test "must be applied before limit/offset" do
      assert_raise ArgumentError, ~r/must be applied before limit\/offset/i, fn ->
        Article
        |> QueryBuilder.limit(1)
        |> QueryBuilder.top_n_per(partition_by: [:author_id], order_by: [desc: :id], n: 1)
        |> Repo.all()
      end
    end

    test "must be applied before order_by" do
      assert_raise ArgumentError, ~r/must be applied before order_by/i, fn ->
        Article
        |> QueryBuilder.order_by(desc: :id)
        |> QueryBuilder.top_n_per(partition_by: [:author_id], order_by: [desc: :id], n: 1)
        |> Repo.all()
      end
    end

    test "must be applied before custom select/select_merge (query-block boundary)" do
      assert_raise ArgumentError, ~r/does not support custom select expressions/i, fn ->
        Article
        |> QueryBuilder.select(:id)
        |> QueryBuilder.top_n_per(partition_by: [:author_id], order_by: [desc: :id], n: 1)
        |> Repo.all()
      end
    end

    test "rejects to-many joins without group_by/distinct (duplicate roots)" do
      assert_raise ArgumentError, ~r/to-many joins/i, fn ->
        User
        |> QueryBuilder.inner_join(:authored_articles)
        |> QueryBuilder.top_n_per(partition_by: [:role_id], order_by: [desc: :id], n: 1)
        |> Repo.all()
      end
    end

    test "rejects to-many joins when distinct is explicitly false" do
      assert_raise ArgumentError, ~r/to-many joins/i, fn ->
        User
        |> QueryBuilder.inner_join(:authored_articles)
        |> QueryBuilder.distinct(false)
        |> QueryBuilder.top_n_per(partition_by: [:role_id], order_by: [desc: :id], n: 1)
        |> Repo.all()
      end
    end

    test "supports ordering by aggregates on grouped to-many joins" do
      author = insert(:user, %{name: "Author"})

      a1 = insert(:article, author: author, publisher: author)
      a2 = insert(:article, author: author, publisher: author)
      a3 = insert(:article, author: author, publisher: author)

      _ = insert_list(1, :comment, article: a1, user: author)
      _ = insert_list(3, :comment, article: a2, user: author)
      _ = insert_list(2, :comment, article: a3, user: author)

      results =
        Article
        |> QueryBuilder.where(author_id: author.id)
        |> QueryBuilder.left_join(:comments)
        |> QueryBuilder.group_by(:id)
        |> QueryBuilder.top_n_per(
          partition_by: [:author_id],
          order_by: [desc: QueryBuilder.count(:id@comments), desc: :id],
          n: 2
        )
        |> Repo.all()

      assert Enum.map(results, & &1.id) |> Enum.sort() == Enum.sort([a2.id, a3.id])
    end

    test "rejects aggregate expressions in partition_by" do
      assert_raise ArgumentError,
                   ~r/does not support aggregate expressions in `partition_by`/i,
                   fn ->
                     Article
                     |> QueryBuilder.top_n_per(
                       partition_by: [QueryBuilder.count(:id)],
                       order_by: [desc: :id],
                       n: 1
                     )
                     |> Repo.all()
                   end
    end

    test "fails fast for reserved window name collisions (window path only)" do
      base =
        from(a in Article,
          as: ^Article._binding(),
          windows: [qb__top_n_per: [partition_by: a.author_id, order_by: [desc: a.id]]]
        )

      assert_raise ArgumentError, ~r/window named :qb__top_n_per/i, fn ->
        base
        |> QueryBuilder.top_n_per(
          partition_by: [:author_id],
          order_by: [desc: :id],
          n: 2
        )
        |> Ecto.Queryable.to_query()
      end
    end

    test "fails fast for reserved binding name collisions" do
      base =
        from(a in Article,
          as: ^Article._binding(),
          join: u in User,
          on: u.id == a.author_id,
          as: :qb__top_n_per_ranked
        )

      assert_raise ArgumentError, ~r/named binding :qb__top_n_per_ranked/i, fn ->
        base
        |> QueryBuilder.top_n_per(partition_by: [:author_id], order_by: [desc: :id], n: 2)
        |> Ecto.Queryable.to_query()
      end
    end

    test "does not duplicate join trees/predicates in SQL (DISTINCT ON path)" do
      query =
        Article
        |> QueryBuilder.where(:publisher, name@publisher: "Acme")
        |> QueryBuilder.top_n_per(partition_by: [:author_id], order_by: [desc: :id], n: 1)

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

      assert sql =~ "DISTINCT ON"
      refute Regex.match?(~r/JOIN\s*\(/i, sql)
      assert length(Regex.scan(~r/JOIN \"users\"/, sql)) == 1
      assert Enum.count(params, &(&1 == "Acme")) == 1
    end

    test "does not duplicate join trees/predicates in SQL (window path)" do
      query =
        Article
        |> QueryBuilder.where(:publisher, name@publisher: "Acme")
        |> QueryBuilder.top_n_per(partition_by: [:author_id], order_by: [desc: :id], n: 2)

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

      assert Regex.match?(~r/row_number/i, sql)
      assert length(Regex.scan(~r/JOIN \"users\"/, sql)) == 1
      assert Enum.count(params, &(&1 == "Acme")) == 1
    end
  end

  describe "left_join_latest/3" do
    test "left-joins the latest has_many row per parent and returns {parent, child}" do
      u1 = insert(:user, %{name: "LatestJoinUser1"})
      u2 = insert(:user, %{name: "LatestJoinUser2"})
      u3 = insert(:user, %{name: "LatestJoinUser3"})

      a1 = insert(:article, author: u1, publisher: u1)
      a2 = insert(:article, author: u1, publisher: u1)

      b1 = insert(:article, author: u2, publisher: u2)
      b2 = insert(:article, author: u2, publisher: u2)

      rows =
        User
        |> QueryBuilder.where({:id, :in, [u1.id, u2.id, u3.id]})
        |> QueryBuilder.order_by(asc: :id)
        |> QueryBuilder.left_join_latest(:authored_articles, order_by: [desc: :id])
        |> Repo.all()

      by_user_id = Map.new(rows, fn {u, a} -> {u.id, a && a.id} end)

      assert by_user_id[u1.id] == max(a1.id, a2.id)
      assert by_user_id[u2.id] == max(b1.id, b2.id)
      assert by_user_id[u3.id] == nil
    end

    test "supports scoped where filters on the assoc schema" do
      user = insert(:user, %{name: "LatestJoinScoped"})

      match1 = insert(:article, author: user, publisher: user, title: "match")
      match2 = insert(:article, author: user, publisher: user, title: "match")
      _other = insert(:article, author: user, publisher: user, title: "other")

      {_u, latest_match} =
        User
        |> QueryBuilder.where(id: user.id)
        |> QueryBuilder.left_join_latest(:authored_articles,
          where: [title: "match"],
          order_by: [desc: :id]
        )
        |> Repo.one!()

      assert latest_match.id == match2.id
      assert latest_match.id != match1.id
    end

    test "fails fast for non-has_many associations (belongs_to / has_one)" do
      assert_raise ArgumentError, ~r/only supports has_many/i, fn ->
        User
        |> QueryBuilder.left_join_latest(:role, order_by: [desc: :id])
        |> Repo.all()
      end

      assert_raise ArgumentError, ~r/only supports has_many/i, fn ->
        User
        |> QueryBuilder.left_join_latest(:setting, order_by: [desc: :id])
        |> Repo.all()
      end
    end

    test "requires the assoc primary key fields as a tie-breaker in order_by" do
      assert_raise ArgumentError,
                   ~r/include the association primary key fields as a tie-breaker/i,
                   fn ->
                     User
                     |> QueryBuilder.left_join_latest(:authored_articles,
                       order_by: [desc: :inserted_at]
                     )
                     |> Repo.all()
                   end
    end

    test "supports @ tokens inside the assoc subquery when child_assoc_fields is provided" do
      author = insert(:user, %{name: "LatestJoinAssocTokens"})
      pub1 = insert(:user, %{name: "Acme"})
      pub2 = insert(:user, %{name: "Other"})

      keep = insert(:article, author: author, publisher: pub1)
      _drop = insert(:article, author: author, publisher: pub2)

      {_u, latest} =
        User
        |> QueryBuilder.where(id: author.id)
        |> QueryBuilder.left_join_latest(:authored_articles,
          child_assoc_fields: :publisher,
          where: [name@publisher: "Acme"],
          order_by: [desc: :id]
        )
        |> Repo.one!()

      assert latest.id == keep.id
    end

    test "fails fast when @ tokens are used without child_assoc_fields" do
      author = insert(:user, %{name: "LatestJoinAssocTokensMissing"})
      pub = insert(:user, %{name: "Acme"})
      _ = insert(:article, author: author, publisher: pub)

      assert_raise ArgumentError, ~r/include it in the assoc_fields argument/i, fn ->
        User
        |> QueryBuilder.where(id: author.id)
        |> QueryBuilder.left_join_latest(:authored_articles,
          where: [name@publisher: "Acme"],
          order_by: [desc: :id]
        )
        |> Repo.all()
      end
    end

    test "cannot be used with paginate/3 (custom select {root, assoc})" do
      assert_raise ArgumentError,
                   ~r/paginate_cursor\/3 and paginate_offset\/3 do not support custom select expressions/i,
                   fn ->
                     User
                     |> QueryBuilder.left_join_latest(:authored_articles, order_by: [desc: :id])
                     |> QueryBuilder.paginate(Repo, page_size: 10)
                   end
    end

    test "cannot be combined with select/select_merge (only one select expression)" do
      assert_raise ArgumentError, ~r/only one select expression/i, fn ->
        User
        |> QueryBuilder.select(:id)
        |> QueryBuilder.left_join_latest(:authored_articles, order_by: [desc: :id])
        |> Repo.all()
      end

      assert_raise ArgumentError, ~r/only one select expression/i, fn ->
        User
        |> QueryBuilder.left_join_latest(:authored_articles, order_by: [desc: :id])
        |> QueryBuilder.select(:id)
        |> Repo.all()
      end

      assert_raise ArgumentError, ~r/select_merge.*left_join_latest/i, fn ->
        User
        |> QueryBuilder.left_join_latest(:authored_articles, order_by: [desc: :id])
        |> QueryBuilder.select_merge(%{name: :name})
        |> Repo.all()
      end
    end

    test "cannot be applied more than once (only one select expression)" do
      assert_raise ArgumentError, ~r/call `left_join_latest\/3` at most once/i, fn ->
        User
        |> QueryBuilder.left_join_latest(:authored_articles, order_by: [desc: :id])
        |> QueryBuilder.left_join_latest(:published_articles, order_by: [desc: :id])
        |> Repo.all()
      end
    end

    test "uses LATERAL on Postgres" do
      query =
        User
        |> QueryBuilder.left_join_latest(:authored_articles,
          order_by: [desc: :id]
        )

      {sql, _params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)
      assert Regex.match?(~r/\bLATERAL\b/i, sql)
      assert Regex.match?(~r/\bLIMIT\b/i, sql)
      refute sql =~ "DISTINCT ON"
      refute Regex.match?(~r/row_number/i, sql)
    end

    test "rejects prefer_lateral?/prefer_distinct_on? (Postgres-only)" do
      assert_raise ArgumentError, ~r/unknown options:.*prefer_lateral\?/i, fn ->
        query =
          User
          |> QueryBuilder.left_join_latest(:authored_articles,
            order_by: [desc: :id],
            prefer_lateral?: false
          )

        Ecto.Adapters.SQL.to_sql(:all, Repo, query)
      end

      assert_raise ArgumentError, ~r/unknown options:.*prefer_distinct_on\?/i, fn ->
        query =
          User
          |> QueryBuilder.left_join_latest(:authored_articles,
            order_by: [desc: :id],
            prefer_distinct_on?: false
          )

        Ecto.Adapters.SQL.to_sql(:all, Repo, query)
      end
    end

    test "fails fast if the generated latest binding name is already present in the query" do
      latest_binding = User._latest_binding(:authored_articles)

      base =
        from(u in User,
          as: ^User._binding(),
          join: a in Article,
          on: a.author_id == u.id,
          as: ^latest_binding
        )

      assert_raise ArgumentError, ~r/binding name is already present/i, fn ->
        base
        |> QueryBuilder.left_join_latest(:authored_articles, order_by: [desc: :id])
        |> Ecto.Queryable.to_query()
      end
    end

    test "does not duplicate child joins/predicates in SQL (LATERAL path)" do
      query =
        User
        |> QueryBuilder.left_join_latest(:authored_articles,
          child_assoc_fields: [publisher: :role],
          where: [name@publisher@role: "admin"],
          order_by: [desc: :id]
        )

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

      assert length(Regex.scan(~r/JOIN \"roles\"/, sql)) == 1
      assert Enum.count(params, &(&1 == "admin")) == 1
    end
  end

  describe "left_join_top_n/3" do
    test "left-joins the top N has_many rows per parent and returns {parent, child} (multiple rows per parent)" do
      u1 = insert(:user, %{name: "TopNJoinUser1"})
      u2 = insert(:user, %{name: "TopNJoinUser2"})
      u3 = insert(:user, %{name: "TopNJoinUser3"})

      a1 = insert(:article, author: u1, publisher: u1)
      a2 = insert(:article, author: u1, publisher: u1)
      a3 = insert(:article, author: u1, publisher: u1)

      b1 = insert(:article, author: u2, publisher: u2)

      rows =
        User
        |> QueryBuilder.where({:id, :in, [u1.id, u2.id, u3.id]})
        |> QueryBuilder.order_by(asc: :id)
        |> QueryBuilder.left_join_top_n(:authored_articles, n: 2, order_by: [desc: :id])
        |> Repo.all()

      by_user_id = Enum.group_by(rows, fn {u, _a} -> u.id end)

      expected_u1_ids =
        [a1.id, a2.id, a3.id]
        |> Enum.sort(:desc)
        |> Enum.take(2)

      assert length(by_user_id[u1.id]) == 2

      assert by_user_id[u1.id]
             |> Enum.map(fn {_u, a} -> a && a.id end)
             |> Enum.reject(&is_nil/1)
             |> Enum.sort(:desc) == expected_u1_ids

      assert length(by_user_id[u2.id]) == 1

      assert by_user_id[u2.id]
             |> Enum.map(fn {_u, a} -> a && a.id end)
             |> Enum.reject(&is_nil/1) == [b1.id]

      u3_id = u3.id
      assert [{%User{id: ^u3_id}, nil}] = by_user_id[u3.id]
    end

    test "supports scoped where filters on the assoc schema" do
      user = insert(:user, %{name: "TopNJoinScoped"})

      _match1 = insert(:article, author: user, publisher: user, title: "match")
      match2 = insert(:article, author: user, publisher: user, title: "match")
      match3 = insert(:article, author: user, publisher: user, title: "match")
      _other = insert(:article, author: user, publisher: user, title: "other")

      rows =
        User
        |> QueryBuilder.where(id: user.id)
        |> QueryBuilder.left_join_top_n(:authored_articles,
          n: 2,
          where: [title: "match"],
          order_by: [desc: :id]
        )
        |> Repo.all()

      assert length(rows) == 2
      expected_ids = [match3.id, match2.id] |> Enum.sort(:desc)

      assert rows
             |> Enum.map(fn {_u, a} -> a.id end)
             |> Enum.sort(:desc) == expected_ids
    end

    test "returns {parent, nil} when assoc rows exist but none match the where filter" do
      user = insert(:user, %{name: "TopNJoinNoMatch"})
      _ = insert(:article, author: user, publisher: user, title: "other")

      rows =
        User
        |> QueryBuilder.where(id: user.id)
        |> QueryBuilder.left_join_top_n(:authored_articles,
          n: 3,
          where: [title: "match"],
          order_by: [desc: :id]
        )
        |> Repo.all()

      user_id = user.id
      assert [{%User{id: ^user_id}, nil}] = rows
    end

    test "fails fast for non-has_many associations (belongs_to / has_one)" do
      assert_raise ArgumentError, ~r/only supports has_many/i, fn ->
        User
        |> QueryBuilder.left_join_top_n(:role, n: 1, order_by: [desc: :id])
        |> Repo.all()
      end

      assert_raise ArgumentError, ~r/only supports has_many/i, fn ->
        User
        |> QueryBuilder.left_join_top_n(:setting, n: 1, order_by: [desc: :id])
        |> Repo.all()
      end
    end

    test "requires the assoc primary key fields as a tie-breaker in order_by" do
      assert_raise ArgumentError,
                   ~r/include the association primary key fields as a tie-breaker/i,
                   fn ->
                     User
                     |> QueryBuilder.left_join_top_n(:authored_articles,
                       n: 2,
                       order_by: [desc: :inserted_at]
                     )
                     |> Repo.all()
                   end
    end

    test "supports @ tokens inside the assoc subquery when child_assoc_fields is provided" do
      author = insert(:user, %{name: "TopNJoinAssocTokens"})
      pub1 = insert(:user, %{name: "Acme"})
      pub2 = insert(:user, %{name: "Other"})

      keep = insert(:article, author: author, publisher: pub1)
      _drop = insert(:article, author: author, publisher: pub2)

      rows =
        User
        |> QueryBuilder.where(id: author.id)
        |> QueryBuilder.left_join_top_n(:authored_articles,
          n: 2,
          child_assoc_fields: :publisher,
          where: [name@publisher: "Acme"],
          order_by: [desc: :id]
        )
        |> Repo.all()

      assert Enum.map(rows, fn {_u, a} -> a.id end) == [keep.id]
    end

    test "fails fast when @ tokens are used without child_assoc_fields" do
      author = insert(:user, %{name: "TopNJoinAssocTokensMissing"})
      pub = insert(:user, %{name: "Acme"})
      _ = insert(:article, author: author, publisher: pub)

      assert_raise ArgumentError, ~r/include it in the assoc_fields argument/i, fn ->
        User
        |> QueryBuilder.where(id: author.id)
        |> QueryBuilder.left_join_top_n(:authored_articles,
          n: 2,
          where: [name@publisher: "Acme"],
          order_by: [desc: :id]
        )
        |> Repo.all()
      end
    end

    test "requires n and order_by options" do
      assert_raise ArgumentError, ~r/requires `n:` and `order_by:` options/i, fn ->
        User
        |> QueryBuilder.left_join_top_n(:authored_articles, order_by: [desc: :id])
        |> Ecto.Queryable.to_query()
      end

      assert_raise ArgumentError, ~r/requires `n:` and `order_by:` options/i, fn ->
        User
        |> QueryBuilder.left_join_top_n(:authored_articles, n: 2)
        |> Ecto.Queryable.to_query()
      end
    end

    test "rejects unknown options (Postgres-only)" do
      assert_raise ArgumentError, ~r/unknown options:.*prefer_lateral\?/i, fn ->
        query =
          User
          |> QueryBuilder.left_join_top_n(:authored_articles,
            n: 2,
            order_by: [desc: :id],
            prefer_lateral?: false
          )

        Ecto.Adapters.SQL.to_sql(:all, Repo, query)
      end
    end

    test "cannot be used with paginate/3 (custom select {root, assoc})" do
      assert_raise ArgumentError,
                   ~r/paginate_cursor\/3 and paginate_offset\/3 do not support custom select expressions/i,
                   fn ->
                     User
                     |> QueryBuilder.left_join_top_n(:authored_articles,
                       n: 2,
                       order_by: [desc: :id]
                     )
                     |> QueryBuilder.paginate(Repo, page_size: 10)
                   end
    end

    test "cannot be combined with select/select_merge (only one select expression)" do
      assert_raise ArgumentError, ~r/only one select expression/i, fn ->
        User
        |> QueryBuilder.select(:id)
        |> QueryBuilder.left_join_top_n(:authored_articles, n: 2, order_by: [desc: :id])
        |> Repo.all()
      end

      assert_raise ArgumentError, ~r/only one select expression/i, fn ->
        User
        |> QueryBuilder.left_join_top_n(:authored_articles, n: 2, order_by: [desc: :id])
        |> QueryBuilder.select(:id)
        |> Repo.all()
      end

      assert_raise ArgumentError, ~r/select_merge.*left_join_top_n/i, fn ->
        User
        |> QueryBuilder.left_join_top_n(:authored_articles, n: 2, order_by: [desc: :id])
        |> QueryBuilder.select_merge(%{name: :name})
        |> Repo.all()
      end
    end

    test "cannot be applied more than once (only one select expression)" do
      assert_raise ArgumentError, ~r/call `left_join_top_n\/3` at most once/i, fn ->
        User
        |> QueryBuilder.left_join_top_n(:authored_articles, n: 2, order_by: [desc: :id])
        |> QueryBuilder.left_join_top_n(:published_articles, n: 2, order_by: [desc: :id])
        |> Repo.all()
      end
    end

    test "uses LATERAL on Postgres and includes LIMIT n" do
      query =
        User
        |> QueryBuilder.left_join_top_n(:authored_articles,
          n: 3,
          order_by: [desc: :id]
        )

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)
      assert Regex.match?(~r/\bLATERAL\b/i, sql)
      assert Regex.match?(~r/\bLIMIT\b/i, sql)
      assert Enum.count(params, &(&1 == 3)) == 1
      refute sql =~ "DISTINCT ON"
      refute Regex.match?(~r/row_number/i, sql)
    end

    test "fails fast if the generated top_n binding name is already present in the query" do
      top_n_binding = User._top_n_binding(:authored_articles)

      base =
        from(u in User,
          as: ^User._binding(),
          join: a in Article,
          on: a.author_id == u.id,
          as: ^top_n_binding
        )

      assert_raise ArgumentError, ~r/binding name is already present/i, fn ->
        base
        |> QueryBuilder.left_join_top_n(:authored_articles, n: 2, order_by: [desc: :id])
        |> Ecto.Queryable.to_query()
      end
    end

    test "does not duplicate child joins/predicates in SQL (LATERAL path)" do
      query =
        User
        |> QueryBuilder.left_join_top_n(:authored_articles,
          n: 5,
          child_assoc_fields: [publisher: :role],
          where: [name@publisher@role: "admin"],
          order_by: [desc: :id]
        )

      {sql, params} = Ecto.Adapters.SQL.to_sql(:all, Repo, query)

      assert length(Regex.scan(~r/JOIN \"roles\"/, sql)) == 1
      assert Enum.count(params, &(&1 == "admin")) == 1
    end
  end
end
