defmodule QueryBuilder.Schema do
  @moduledoc false

  # The library code relies on named bindings to refer to associations. This module
  # allows to define a named binding for a schema, when it is used in a join.
  #
  # The module provides the caller with the following functions:
  # * `binding/1`: binding name for a join between this schema and the given
  # association field. The list of association fields must be given in `opts` when
  # `use`-ing this module for this function to return an atom.
  # * `binding/0`: if we don't have a specific binding name between the schema and
  # the association field (i.e. the list of association fields has not been given),
  # use the schema name as the binding name.
  # * `join/4`: creates a join query expression.
  #
  # Important to understand:
  # ------------------------
  # When a named binding needs to be assigned for a join, first the code calls
  # `binding/1` on the **source schema**, with the association field as argument.
  # If a name cannot be found, then `binding/0` is called on the
  # **associated schema**.
  # `binding/1` returns an atom in the format: `schema_name <> "__" <> assoc_field`.
  #
  # In order to join, `join/4` needs to be called either on the source schema or the
  # schema of the association field. This again, depends whether the association
  # fields have been given to `opts` or not; whether a binding name for this
  # association has been retrieved through `binding/0` or `binding/1`. If we have no
  # specific binding name for this schema and association field, then `join/4` is
  # called on the schema of the association field; otherwise, `join/4`is called on
  # the source schema.

  defmacro __using__(opts) do
    assoc_fields = Keyword.get(opts, :assoc_fields, [])

    [
      quote do
        require Ecto.Query

        # assign a named binding (the schema's module name) to the schema of the root query
        def _query(), do: Ecto.Query.from(x in __MODULE__, as: unquote(__CALLER__.module))

        def _binding(), do: __MODULE__

        Module.register_attribute(__MODULE__, :assoc_fields, accumulate: true)
      end,
      for assoc_field <- assoc_fields do
        binding = String.to_atom("#{__CALLER__.module}__#{assoc_field}")

        quote do
          def _join(query, type, source_binding, unquote(assoc_field)) do
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
          def _binding(unquote(assoc_field)),
            do: unquote(binding)
        end
      end,
      quote do
        def _binding(_), do: nil
      end,
      quote do
        def _join(query, type, source_binding, assoc_field) do
          Ecto.Query.join(query, type, [{^source_binding, x}], y in assoc(x, ^assoc_field),
            as: unquote(__CALLER__.module)
          )
        end
      end
    ]
  end
end
