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
      # [where_initcap: {:name, "john"}, where: {:active, true}]
      MyApp.Accounts.User
      |> QB.from_list(opts)
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

      defdelegate maybe_where(query, bool, filters), to: QueryBuilder

      defdelegate maybe_where(query, condition, assoc_fields, filters, or_filters \\ []),
        to: QueryBuilder

        defdelegate maybe_order_by(query, bool, value), to: QueryBuilder

        defdelegate maybe_order_by(query, condition, assoc_fields, value),
          to: QueryBuilder

      defdelegate new(ecto_query), to: QueryBuilder
      defdelegate paginate(query, repo, opts \\ []), to: QueryBuilder
      defdelegate order_by(query, value), to: QueryBuilder
      defdelegate order_by(query, assoc_fields, value), to: QueryBuilder
      defdelegate preload(query, assoc_fields), to: QueryBuilder
      defdelegate where(query, filters), to: QueryBuilder
      defdelegate where(query, assoc_fields, filters, or_filters \\ []), to: QueryBuilder
      defdelegate offset(query, value), to: QueryBuilder
      defdelegate limit(query, value), to: QueryBuilder

      @doc ~S"""
      Allows to pass a list of operations through a keyword list.

      Example:
      ```
      QueryBuilder.from_list(query, [
        where: [name: "John", city: "Anytown"],
        preload: [articles: :comments]
      ])
      ```
      """
      def from_list(query, nil), do: query
      def from_list(query, []), do: query

      def from_list(query, [{operation, arguments} | tail]) do
        arguments =
          cond do
            is_tuple(arguments) -> Tuple.to_list(arguments)
            is_list(arguments) -> [arguments]
            true -> List.wrap(arguments)
          end

        arity = 1 + length(arguments)

        unless function_exported?(__MODULE__, operation, arity) do
          available =
            __MODULE__.__info__(:functions)
            |> Enum.map(&elem(&1, 0))
            |> Enum.uniq()
            |> Enum.sort()
            |> Enum.join(", ")

          raise ArgumentError,
                "unknown from_list operation #{inspect(operation)}/#{arity}; " <>
                  "expected a public function on #{inspect(__MODULE__)}. Available operations: #{available}"
        end

        apply(__MODULE__, operation, [query | arguments]) |> from_list(tail)
      end
    end
  end
end
