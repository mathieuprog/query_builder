defmodule QueryBuilder.Query do
  @moduledoc false
  defstruct(
    ecto_query: nil,
    operations: []
  )
end

defimpl Ecto.Queryable, for: QueryBuilder.Query do
  def to_query(%{
    ecto_query: ecto_query,
    operations: operations
  }) do
    source_schema = QueryBuilder.Utils.root_schema(ecto_query)

    assoc_list =
      Enum.reduce(operations, [], fn %{assocs: assocs, type: type}, accumulated_assocs ->
        opts =
          case type do
            :preload ->
              [join: :inner_if_cardinality_is_one, preload: true]

            :left_join ->
              [join: :left]

            _ ->
              []
          end

        QueryBuilder.AssocList.build(source_schema, accumulated_assocs, assocs, opts)
      end)

    {ecto_query, assoc_list} =
      QueryBuilder.JoinMaker.make_joins(ecto_query, assoc_list)

    ecto_query =
      Enum.reduce(operations, ecto_query, fn
        %{type: type, args: args}, ecto_query ->
          Module.concat(QueryBuilder.Query, to_string(type) |> Macro.camelize())
          |> apply(type, [ecto_query | [assoc_list | args]])
      end)

    Ecto.Queryable.to_query(ecto_query)
  end
end
