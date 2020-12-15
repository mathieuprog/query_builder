defmodule QueryBuilder.Query do
  @moduledoc false
  defstruct(
    ecto_query: nil,
    operations: []
  )
end

defimpl Inspect, for: QueryBuilder.Query do
  def inspect(query, opts) do
    query
    |> Ecto.Queryable.to_query()
    |> Inspect.inspect(opts)
  end
end

defimpl Ecto.Queryable, for: QueryBuilder.Query do
  @authorizer Application.get_env(:query_builder, :authorizer)

  def to_query(%{ecto_query: ecto_query} = query) do
    source_schema = QueryBuilder.Utils.root_schema(ecto_query)

    %{ecto_query: ecto_query, operations: operations} =
      case authorizer() do
        nil ->
          query

        authorizer ->
          authorizer.reject_unauthorized(query, source_schema)
      end

    assoc_list =
      Enum.reduce(operations, [], fn %{assocs: assocs, type: type} = operation, accumulated_assocs ->
        opts =
          case type do
            :preload ->
              [join: :inner_if_cardinality_is_one, preload: true]

            :left_join ->
              [join: :left, join_filters: operation.join_filters]

            _ ->
              []
          end

        opts = [{:authorizer, @authorizer} | opts]

        QueryBuilder.AssocList.build(source_schema, accumulated_assocs, assocs, opts)
      end)

    {ecto_query, assoc_list} =
      QueryBuilder.JoinMaker.make_joins(ecto_query, assoc_list)

    ecto_query =
      Enum.reduce(operations, ecto_query, fn
        %{type: :left_join}, ecto_query ->
          ecto_query

        %{type: type, args: args}, ecto_query ->
          Module.concat(QueryBuilder.Query, to_string(type) |> Macro.camelize())
          |> apply(type, [ecto_query | [assoc_list | args]])
      end)

    Ecto.Queryable.to_query(ecto_query)
  end

  defp authorizer() do
    Application.get_env(:query_builder, :authorizer)
  end
end
