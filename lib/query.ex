defmodule QueryBuilder.Query do
  @moduledoc false
  defstruct(
    ecto_query: nil,
    operations: []
  )

  @doc false
  def to_query_and_assoc_list(%__MODULE__{ecto_query: ecto_query} = query) do
    source_schema = QueryBuilder.Utils.root_schema(ecto_query)

    %{ecto_query: ecto_query, operations: operations} = query

    operations = Enum.reverse(operations)
    validate_select_operations!(operations)

    assoc_list =
      Enum.reduce(operations, [], fn %{assocs: assocs, type: type} = operation,
                                     accumulated_assocs ->
        opts =
          case type do
            :preload ->
              preload_strategy = Map.get(operation, :preload_strategy, nil)
              [join: :none, preload: true, preload_strategy: preload_strategy]

            :inner_join ->
              [join: :inner]

            :left_join ->
              left_join_mode = Map.get(operation, :left_join_mode, :leaf)
              [join: :left, join_filters: operation.join_filters, left_join_mode: left_join_mode]

            _ ->
              []
          end

        QueryBuilder.AssocList.build(source_schema, accumulated_assocs, assocs, opts)
      end)

    {ecto_query, assoc_list} =
      QueryBuilder.JoinMaker.make_joins(ecto_query, assoc_list)

    ecto_query =
      Enum.reduce(operations, ecto_query, fn
        %{type: :left_join}, ecto_query ->
          ecto_query

        %{type: :inner_join}, ecto_query ->
          ecto_query

        %{type: type, args: args}, ecto_query ->
          Module.concat(QueryBuilder.Query, to_string(type) |> Macro.camelize())
          |> apply(type, [ecto_query | [assoc_list | args]])
      end)

    {Ecto.Queryable.to_query(ecto_query), assoc_list}
  end

  defp validate_select_operations!(operations) do
    select_indexes =
      operations
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {%{type: :select}, index} -> [index]
        {_op, _index} -> []
      end)

    case select_indexes do
      [] ->
        :ok

      [select_index] ->
        if Enum.any?(Enum.take(operations, select_index), fn %{type: type} ->
             type == :select_merge
           end) do
          raise ArgumentError,
                "only one select expression is allowed in query; " <>
                  "calling `select/*` after `select_merge/*` is not supported (Ecto semantics)"
        end

        :ok

      _many ->
        raise ArgumentError,
              "only one select expression is allowed in query; " <>
                "call `select/*` at most once and use `select_merge/*` to add fields"
    end
  end
end

defimpl Ecto.Queryable, for: QueryBuilder.Query do
  def to_query(%QueryBuilder.Query{} = query) do
    {ecto_query, _assoc_list} = QueryBuilder.Query.to_query_and_assoc_list(query)
    ecto_query
  end
end
