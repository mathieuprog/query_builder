defmodule QueryBuilder.Schema do
  @moduledoc false

  defmacro __using__(opts) do
    assoc_fields = Keyword.get(opts, :assoc_fields, [])

    [
      quote do
        require Ecto.Query

        def query(), do: Ecto.Query.from(x in __MODULE__, as: unquote(__CALLER__.module))

        def binding(), do: __MODULE__

        Module.register_attribute(__MODULE__, :assoc_fields, accumulate: true)
      end,
      for assoc_field <- assoc_fields do
        binding = String.to_atom("#{__CALLER__.module}__#{assoc_field}")

        quote do
          def join(query, type, source_binding, unquote(assoc_field)) do
            Ecto.Query.join(
              query,
              type,
              [{^source_binding, x}],
              y in assoc(x, ^unquote(assoc_field)),
              as: unquote(binding)
            )
          end
        end
      end,
      for assoc_field <- assoc_fields do
        binding = String.to_atom("#{__CALLER__.module}__#{assoc_field}")

        quote do
          def binding(unquote(assoc_field)),
            do: unquote(binding)
        end
      end,
      quote do
        def binding(_), do: nil
      end,
      quote do
        def join(query, type, source_binding, assoc_field) do
          Ecto.Query.join(query, type, [{^source_binding, x}], y in assoc(x, ^assoc_field),
            as: unquote(__CALLER__.module)
          )
        end
      end
    ]
  end
end
