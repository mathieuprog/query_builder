defmodule QueryBuilder.FromOptsIncludesTest do
  use ExUnit.Case, async: true

  alias QueryBuilder.User
  alias QueryBuilder.AssocList.PreloadSpec

  describe "from_opts/3 includes: allowlist + opts include:" do
    test "applies allowlisted includes (boundary mode) and keeps normal ops working" do
      query =
        QueryBuilder.from_opts(
          User,
          [where: [name: "Alice"], include: [:role], order_by: [asc: :id]],
          includes: [role: :role]
        )

      assert match?(%QueryBuilder.Query{}, query)

      assert Enum.any?(query.operations, fn
               {:preload, :role, [%PreloadSpec{strategy: :separate}]} -> true
               _ -> false
             end)
    end

    test "accepts include keys as strings (no atom creation required)" do
      query =
        QueryBuilder.from_opts(
          User,
          [include: ["role"]],
          includes: [role: :role]
        )

      assert Enum.any?(query.operations, fn
               {:preload, :role, [%PreloadSpec{strategy: :separate}]} -> true
               _ -> false
             end)
    end

    test "supports multiple include entries (merged)" do
      query =
        QueryBuilder.from_opts(
          User,
          [include: [:role], where: [id: 100], include: ["role"], limit: 10],
          includes: [role: :role]
        )

      assert Enum.count(query.operations, fn
               {:preload, :role, [%PreloadSpec{strategy: :separate}]} -> true
               _ -> false
             end) == 1
    end

    test "supports declarative scoped separate preload specs" do
      query =
        QueryBuilder.from_opts(
          User,
          [include: [:published_authored_articles]],
          includes: [
            published_authored_articles:
              {:preload_separate_scoped, :authored_articles,
               [where: [published: true], order_by: [desc: :inserted_at]]}
          ]
        )

      assert Enum.any?(query.operations, fn
               {:preload, :authored_articles,
                [%PreloadSpec{strategy: :separate, query_opts: query_opts}]}
               when is_list(query_opts) ->
                 Keyword.get(query_opts, :where) == [published: true] and
                   Keyword.get(query_opts, :order_by) == [desc: :inserted_at]

               _ ->
                 false
             end)
    end

    test "raises when include: is present but no includes: allowlist is provided" do
      assert_raise ArgumentError, ~r/include:.*includes:.*allowlist/i, fn ->
        QueryBuilder.from_opts(User, include: [:role])
      end
    end

    test "raises on unknown include key (shows allowlist)" do
      assert_raise ArgumentError, ~r/unknown include key.*allowed includes/i, fn ->
        QueryBuilder.from_opts(User, [include: [:nope]], includes: [role: :role])
      end
    end

    test "rejects function include handlers in includes:" do
      assert_raise ArgumentError, ~r/does not accept function include handlers/i, fn ->
        QueryBuilder.from_opts(User, [], includes: [role: fn q -> q end])
      end
    end
  end
end
