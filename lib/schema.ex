defmodule QueryBuilder.Schema do
  @moduledoc false

  # QueryBuilder builds up a list of operations (where/order_by/preload/joins) and only
  # later turns them into an `Ecto.Query` with joins and `dynamic/2` expressions. In
  # order to generate expressions like `field(x, ^field)` against the *right* joined
  # table, it needs a stable way to refer to each join.
  #
  # Ecto’s named bindings (`as: ...` + `[{^binding, x}]`) are that stable handle: they
  # are order-independent and composable. Without named bindings, QueryBuilder would
  # have to track positional bind indexes (`[u, r, a, ...]`) and would become brittle
  # as joins are added/merged/reordered.
  #
  # Preloading “with bindings” also depends on named bindings
  # (`Ecto.Query.preload(query, [{^binding, x}], ...)`) to map joined rows back onto
  # associations.
  #
  # Binding names for schema associations are generated automatically at compile time
  # in `__before_compile__/1`.

  defmacro __using__(opts) do
    # Migration guard: v1 accepted `assoc_fields:` for manual association binding names. In v2,
    # association bindings are generated automatically, so we fail fast with an upgrade hint.
    if Keyword.has_key?(opts, :assoc_fields) do
      raise ArgumentError,
            "the `assoc_fields:` option was removed in QueryBuilder v2; " <>
              "association bindings are now generated automatically"
    end

    unless opts == [] do
      raise ArgumentError,
            "unknown options passed to `use QueryBuilder`: #{inspect(opts)} " <>
              "(supported options: none)"
    end

    [
      quote do
        require Ecto.Query

        @before_compile unquote(__MODULE__)

        # assign a named binding (the schema's module name) to the schema of the root query
        def _query(), do: Ecto.Query.from(x in __MODULE__, as: unquote(__CALLER__.module))

        def _binding(), do: __MODULE__
      end
    ]
  end

  defmacro __before_compile__(env) do
    assocs =
      env.module
      |> Module.get_attribute(:ecto_assocs)
      |> List.wrap()

    assoc_bindings =
      Enum.map(assocs, fn {name, _struct} ->
        binding =
          ("qb__" <> to_string(env.module) <> "__" <> to_string(name))
          |> String.to_atom()

        {name, binding}
      end)

    assoc_latest_bindings =
      Enum.map(assocs, fn {name, _struct} ->
        binding =
          ("qb__latest__" <> to_string(env.module) <> "__" <> to_string(name))
          |> String.to_atom()

        {name, binding}
      end)

    assoc_left_join_top_n_bindings =
      Enum.map(assocs, fn {name, _struct} ->
        binding =
          ("qb__top_n__" <> to_string(env.module) <> "__" <> to_string(name))
          |> String.to_atom()

        {name, binding}
      end)

    assoc_infos =
      Enum.map(assocs, fn {name, struct} ->
        {name, struct.queryable, struct.cardinality}
      end)

    binding_clauses =
      for {assoc_field, binding} <- assoc_bindings do
        quote do
          def _binding(unquote(assoc_field)), do: unquote(binding)
        end
      end

    latest_binding_clauses =
      for {assoc_field, binding} <- assoc_latest_bindings do
        quote do
          def _latest_binding(unquote(assoc_field)), do: unquote(binding)
        end
      end

    left_join_top_n_binding_clauses =
      for {assoc_field, binding} <- assoc_left_join_top_n_bindings do
        quote do
          def _top_n_binding(unquote(assoc_field)), do: unquote(binding)
        end
      end

    schema_clauses =
      for {assoc_field, assoc_schema, _cardinality} <- assoc_infos do
        quote do
          def _assoc_schema(unquote(assoc_field)), do: unquote(assoc_schema)
        end
      end

    cardinality_clauses =
      for {assoc_field, _assoc_schema, cardinality} <- assoc_infos do
        quote do
          def _assoc_cardinality(unquote(assoc_field)), do: unquote(cardinality)
        end
      end

    assoc_fields = Enum.map(assoc_bindings, &elem(&1, 0))

    join_clause =
      if assocs == [] do
        quote do
          def _join(_query, _type, _source_binding, assoc_field, _on) do
            raise ArgumentError,
                  "unknown association #{inspect(assoc_field)} for #{inspect(__MODULE__)}; " <>
                    "available associations: #{inspect(__schema__(:associations))}"
          end
        end
      else
        quote do
          def _join(query, type, source_binding, assoc_field, on) do
            assoc_binding = _binding(assoc_field)

            Ecto.Query.join(
              query,
              type,
              [{^source_binding, x}],
              y in assoc(x, ^assoc_field),
              as: ^assoc_binding,
              on: ^on
            )
          end
        end
      end

    quote do
      unquote_splicing(binding_clauses)

      def _binding(assoc_field) do
        raise ArgumentError,
              "unknown association #{inspect(assoc_field)} for #{inspect(__MODULE__)}; " <>
                "available associations: #{inspect(__schema__(:associations))}"
      end

      unquote_splicing(latest_binding_clauses)

      def _latest_binding(assoc_field) do
        raise ArgumentError,
              "unknown association #{inspect(assoc_field)} for #{inspect(__MODULE__)}; " <>
                "available associations: #{inspect(__schema__(:associations))}"
      end

      unquote_splicing(left_join_top_n_binding_clauses)

      def _top_n_binding(assoc_field) do
        raise ArgumentError,
              "unknown association #{inspect(assoc_field)} for #{inspect(__MODULE__)}; " <>
                "available associations: #{inspect(__schema__(:associations))}"
      end

      unquote_splicing(schema_clauses)

      def _assoc_schema(assoc_field) do
        raise ArgumentError,
              "unknown association #{inspect(assoc_field)} for #{inspect(__MODULE__)}; " <>
                "available associations: #{inspect(__schema__(:associations))}"
      end

      unquote_splicing(cardinality_clauses)

      def _assoc_cardinality(assoc_field) do
        raise ArgumentError,
              "unknown association #{inspect(assoc_field)} for #{inspect(__MODULE__)}; " <>
                "available associations: #{inspect(__schema__(:associations))}"
      end

      def _assoc_fields(), do: unquote(assoc_fields)

      unquote(join_clause)
    end
  end
end
