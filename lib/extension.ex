defmodule QueryBuilder.Extension do
  @moduledoc ~S"""
  Use this module to create an extension module to `QueryBuilder` for app specific query utilities.
  Use your query builder extension module wherever you would normally use `QueryBuilder`

  Example:
  ```
  defmodule MyApp.QueryBuilder do
    use QueryBuilder.Extension

    defmacro __using__(opts) do
      quote do
        require QueryBuilder
        QueryBuilder.__using__(unquote(opts))
      end
    end

    # Add app specific query functions
    #---------------------------------

    def where_initcap(query, field, value) do
      text_equals_condition = fn field, value, get_binding_fun ->
        {field, binding} = get_binding_fun.(field)
        Ecto.Query.dynamic([{^binding, x}], fragment("initcap(?)", ^value) == field(x, ^field))
      end

      query
      |> where(&text_equals_condition.(field, value, &1))
    end
  end

  defmodule MyApp.Accounts.User do
    use MyApp.QueryBuilder

    schema "users" do
      field :name, :string
      field :active, :boolean
    end
  end

  defmodule MyApp.Accounts do
    alias MyApp.QueryBuilder, as: QB

    def list_users(opts \\ []) do
      # Query list can include custom query functions as well:
      # [where_initcap: QB.args(:name, "john"), where: [active: true]]
      MyApp.Accounts.User
      |> QB.from_opts(opts, mode: :full)
      |> Repo.all()
    end
  end
  ```
  """

  defmacro __using__(_opts) do
    quote do
      # Expose all QueryBuilder functions: QueryBuilder.__info__(:functions)

      defdelegate left_join(query, assoc_fields, filters \\ [], or_filters \\ []),
        to: QueryBuilder

      defdelegate inner_join(query, assoc_fields), to: QueryBuilder

      defdelegate left_join_leaf(query, assoc_fields, filters \\ [], or_filters \\ []),
        to: QueryBuilder

      defdelegate left_join_path(query, assoc_fields, filters \\ [], or_filters \\ []),
        to: QueryBuilder

      defdelegate maybe_where(query, bool, filters), to: QueryBuilder

      defdelegate maybe_where(query, condition, assoc_fields, filters, or_filters \\ []),
        to: QueryBuilder

      defdelegate maybe_order_by(query, bool, value), to: QueryBuilder

      defdelegate maybe_order_by(query, condition, assoc_fields, value),
        to: QueryBuilder

      defdelegate args(args), to: QueryBuilder
      defdelegate args(arg1, arg2), to: QueryBuilder
      defdelegate args(arg1, arg2, arg3), to: QueryBuilder
      defdelegate args(arg1, arg2, arg3, arg4), to: QueryBuilder

      defdelegate new(ecto_query), to: QueryBuilder
      defdelegate subquery(queryable, opts \\ []), to: QueryBuilder
      defdelegate paginate(query, repo, opts \\ []), to: QueryBuilder
      defdelegate distinct(query, value), to: QueryBuilder
      defdelegate distinct(query, assoc_fields, value), to: QueryBuilder
      defdelegate group_by(query, expr), to: QueryBuilder
      defdelegate group_by(query, assoc_fields, expr), to: QueryBuilder
      defdelegate having(query, filters), to: QueryBuilder
      defdelegate having(query, assoc_fields, filters, or_filters \\ []), to: QueryBuilder
      defdelegate having_any(query, or_groups), to: QueryBuilder
      defdelegate having_any(query, assoc_fields, or_groups), to: QueryBuilder
      defdelegate order_by(query, value), to: QueryBuilder
      defdelegate order_by(query, assoc_fields, value), to: QueryBuilder
      defdelegate preload_separate(query, assoc_fields), to: QueryBuilder
      defdelegate preload_separate_scoped(query, assoc_field, opts \\ []), to: QueryBuilder
      defdelegate preload_through_join(query, assoc_fields), to: QueryBuilder
      defdelegate select(query, selection), to: QueryBuilder
      defdelegate select(query, assoc_fields, selection), to: QueryBuilder
      defdelegate select_merge(query, selection), to: QueryBuilder
      defdelegate select_merge(query, assoc_fields, selection), to: QueryBuilder
      defdelegate count(), to: QueryBuilder
      defdelegate count(token), to: QueryBuilder
      defdelegate count(token, modifier), to: QueryBuilder
      defdelegate count_distinct(token), to: QueryBuilder
      defdelegate avg(token), to: QueryBuilder
      defdelegate sum(token), to: QueryBuilder
      defdelegate min(token), to: QueryBuilder
      defdelegate max(token), to: QueryBuilder
      defdelegate where(query, filters), to: QueryBuilder
      defdelegate where(query, assoc_fields, filters, or_filters \\ []), to: QueryBuilder
      defdelegate where_any(query, or_groups), to: QueryBuilder
      defdelegate where_any(query, assoc_fields, or_groups), to: QueryBuilder
      defdelegate where_exists(query, assoc_fields, filters, or_filters \\ []), to: QueryBuilder

      defdelegate where_not_exists(query, assoc_fields, filters, or_filters \\ []),
        to: QueryBuilder

      defdelegate where_exists_subquery(query, assoc_fields, opts \\ []), to: QueryBuilder

      defdelegate where_not_exists_subquery(query, assoc_fields, opts \\ []),
        to: QueryBuilder

      defdelegate offset(query, value), to: QueryBuilder
      defdelegate limit(query, value), to: QueryBuilder

      @doc ~S"""
      Applies a keyword list of operations to a query.

      Like `QueryBuilder.from_opts/*`, this defaults to boundary mode. To allow the
      full `from_opts` surface (including custom extension operations), pass
      `mode: :full`.
      """
      def from_opts(query, opts) do
        from_opts(query, opts, mode: :boundary)
      end

      def from_opts(query, opts, from_opts_opts) do
        QueryBuilder.__from_opts__(query, opts, __MODULE__, from_opts_opts)
      end

      # Migration shim: v1 exposed from_list/2. Keep it to raise with a clear upgrade hint.
      def from_list(_query, _opts) do
        raise ArgumentError,
              "from_list/2 was renamed to from_opts/2; please update your call sites"
      end
    end
  end
end
