defmodule QueryBuilder.Extension do
  @moduledoc ~S"""
  Use this module to create an extension module to `QueryBuilder` for app specific query utilities.
  Use your query builder extension module wherever you would normally use `QueryBuilder`

  Example:
  ```
  defmodule MyApp.QueryBuilder do
    use QueryBuilder.Extension, from_opts_full_ops: [:where_initcap]
    import Ecto.Query

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
        dynamic([{^binding, x}], fragment("initcap(?)", ^value) == field(x, ^field))
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

  defmacro __using__(opts) do
    opts = validate_extension_opts!(opts)

    from_opts_full_ops = Keyword.get(opts, :from_opts_full_ops, [])
    boundary_ops_user_asserted = Keyword.get(opts, :boundary_ops_user_asserted, [])

    Code.ensure_compiled!(QueryBuilder)

    delegates =
      QueryBuilder.__info__(:functions)
      |> Enum.reject(fn {fun, _arity} ->
        fun in [:module_info, :from_opts] or String.starts_with?(Atom.to_string(fun), "__")
      end)
      |> Enum.map(fn {fun, arity} ->
        args = Macro.generate_arguments(arity, __MODULE__)

        quote do
          defdelegate unquote(fun)(unquote_splicing(args)), to: QueryBuilder
        end
      end)

    quote do
      # Expose all QueryBuilder functions: QueryBuilder.__info__(:functions)

      @doc false
      def __query_builder_extension_from_opts_config__() do
        %{
          from_opts_full_ops: unquote(from_opts_full_ops),
          boundary_ops_user_asserted: unquote(boundary_ops_user_asserted)
        }
      end

      unquote_splicing(delegates)

      @doc ~S"""
      Applies a keyword list of operations to a query.

      Like `QueryBuilder.from_opts/*`, this defaults to boundary mode.

      To use the full `from_opts` surface, pass `mode: :full`. Custom extension
      operations are rejected by default in full mode; allowlist them via
      `use QueryBuilder.Extension, from_opts_full_ops: [...]`.

      Note: operations allowlisted via `boundary_ops_user_asserted: [...]` are also
      accepted in full mode (full is a superset).
      """
      def from_opts(query, opts) do
        from_opts(query, opts, mode: :boundary)
      end

      def from_opts(query, opts, from_opts_opts) do
        QueryBuilder.__from_opts__(query, opts, __MODULE__, from_opts_opts)
      end
    end
  end

  defp validate_extension_opts!(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "use QueryBuilder.Extension expects options to be a keyword list, got: #{inspect(opts)}"
    end

    allowed_keys = [:from_opts_full_ops, :boundary_ops_user_asserted]

    case Keyword.keys(opts) -- allowed_keys do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "use QueryBuilder.Extension got unknown options #{inspect(unknown)}; " <>
                "supported options: #{inspect(allowed_keys)}"
    end

    validate_extension_ops_list!(opts, :from_opts_full_ops)
    validate_extension_ops_list!(opts, :boundary_ops_user_asserted)

    opts
  end

  defp validate_extension_opts!(opts) do
    raise ArgumentError,
          "use QueryBuilder.Extension expects options to be a keyword list, got: #{inspect(opts)}"
  end

  defp validate_extension_ops_list!(opts, key) do
    case Keyword.get(opts, key, []) do
      list when is_list(list) ->
        Enum.each(list, fn
          op when is_atom(op) ->
            :ok

          other ->
            raise ArgumentError,
                  "use QueryBuilder.Extension expects #{inspect(key)} to be a list of atoms, got: #{inspect(other)} in #{inspect(list)}"
        end)

      other ->
        raise ArgumentError,
              "use QueryBuilder.Extension expects #{inspect(key)} to be a list of atoms, got: #{inspect(other)}"
    end
  end
end
