defmodule QueryBuilder.AssocList.Node do
  @moduledoc false

  alias QueryBuilder.AssocList.JoinSpec
  alias QueryBuilder.AssocList.PreloadSpec

  @type t :: %__MODULE__{
          assoc_binding: atom(),
          assoc_field: atom(),
          assoc_schema: module(),
          cardinality: :one | :many,
          join_spec: JoinSpec.t(),
          preload_spec: PreloadSpec.t() | nil,
          nested_assocs: %{optional(atom()) => t()},
          source_binding: atom(),
          source_schema: module()
        }

  @enforce_keys [
    :assoc_binding,
    :assoc_field,
    :assoc_schema,
    :cardinality,
    :join_spec,
    :nested_assocs,
    :source_binding,
    :source_schema
  ]

  defstruct assoc_binding: nil,
            assoc_field: nil,
            assoc_schema: nil,
            cardinality: :one,
            join_spec: nil,
            preload_spec: nil,
            nested_assocs: %{},
            source_binding: nil,
            source_schema: nil
end
