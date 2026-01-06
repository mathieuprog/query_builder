defmodule QueryBuilder.FromOptsBoundaryTest do
  use ExUnit.Case, async: true

  import Ecto.Query, only: [from: 2]

  alias QueryBuilder.User

  describe "from_opts/2 (boundary mode by default)" do
    test "allows a join-independent subset of operations" do
      query =
        QueryBuilder.from_opts(User,
          where: [name: "Alice", deleted: false],
          where_any: [[deleted: false], [deleted: true]],
          order_by: [asc: :name],
          limit: 10,
          offset: 0
        )

      assert match?(%QueryBuilder.Query{}, query)
    end

    test "rejects negative limit/offset values" do
      assert_raise ArgumentError, ~r/boundary mode.*limit.*non-negative/i, fn ->
        QueryBuilder.from_opts(User, limit: -1)
      end

      assert_raise ArgumentError, ~r/boundary mode.*offset.*non-negative/i, fn ->
        QueryBuilder.from_opts(User, offset: -1)
      end

      assert_raise ArgumentError, ~r/boundary mode.*limit.*non-negative/i, fn ->
        QueryBuilder.from_opts(User, limit: "-1")
      end

      assert_raise ArgumentError, ~r/boundary mode.*offset.*non-negative/i, fn ->
        QueryBuilder.from_opts(User, offset: "-1")
      end
    end

    test "rejects joins and shape-changing clauses" do
      assert_raise ArgumentError,
                   ~r/mode: :boundary.*inner_join|inner_join.*mode: :boundary/i,
                   fn ->
                     QueryBuilder.from_opts(User, inner_join: :role)
                   end

      assert_raise ArgumentError, ~r/mode: :boundary.*select|select.*mode: :boundary/i, fn ->
        QueryBuilder.from_opts(User, select: :id)
      end
    end

    test "rejects preloads at the boundary" do
      assert_raise ArgumentError, ~r/mode: :boundary.*preload|preload.*mode: :boundary/i, fn ->
        QueryBuilder.from_opts(User, preload_separate: :role)
      end
    end

    test "rejects assoc traversal (field@assoc tokens), even if the base query joined" do
      assert_raise ArgumentError,
                   ~r/boundary mode.*assoc tokens|assoc tokens.*boundary mode/i,
                   fn ->
                     QueryBuilder.from_opts(User, where: [name@role: "admin"])
                   end

      assert_raise ArgumentError,
                   ~r/boundary mode.*assoc tokens|assoc tokens.*boundary mode/i,
                   fn ->
                     QueryBuilder.from_opts(User, order_by: [asc: :name@role])
                   end

      assert_raise ArgumentError,
                   ~r/boundary mode.*assoc tokens|assoc tokens.*boundary mode/i,
                   fn ->
                     User
                     |> QueryBuilder.inner_join(:role)
                     |> QueryBuilder.from_opts(where_any: [[name: "Alice"], [name@role: "admin"]])
                   end
    end

    test "rejects QueryBuilder.args/* wrappers (multi-arg / assoc-aware calls)" do
      assert_raise ArgumentError, ~r/QueryBuilder\.args.*boundary mode/i, fn ->
        QueryBuilder.from_opts(User, where: QueryBuilder.args(:role, name@role: "admin"))
      end
    end

    test "rejects escape hatches that depend on hidden query shape" do
      assert_raise ArgumentError, ~r/boundary mode.*function filters/i, fn ->
        QueryBuilder.from_opts(User, where: [fn _resolve -> true end])
      end

      assert_raise ArgumentError, ~r/boundary mode.*function order_by/i, fn ->
        QueryBuilder.from_opts(User, order_by: [asc: fn _resolve -> true end])
      end

      ids = from(u in User, select: u.id)

      assert_raise ArgumentError, ~r/boundary mode.*subqueries/i, fn ->
        QueryBuilder.from_opts(User, where: [{:id, :in, ids}])
      end
    end
  end

  defmodule UnsafeExtension do
    use QueryBuilder.Extension

    def secret(query, value) do
      send(self(), {:secret_called, value})
      query
    end
  end

  test "extension from_opts/2 boundary mode rejects extension operations by keyword" do
    from_opts = [secret: :pwned]

    assert_raise ArgumentError, ~r/mode: :boundary.*supported operations/i, fn ->
      UnsafeExtension.from_opts(User, from_opts)
    end

    refute_received {:secret_called, :pwned}
  end
end
