defmodule QueryBuilder.JoinOps do
  @moduledoc false

  def inner_join(%QueryBuilder.Query{} = query, assoc_fields) do
    %{
      query
      | operations: [{:inner_join, assoc_fields, []} | query.operations]
    }
  end

  def left_join(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    if assoc_fields_nested?(assoc_fields) do
      raise ArgumentError,
            "left_join/4 does not support nested association paths (it would be ambiguous whether intermediate hops " <>
              "should be inner-joined or left-joined). " <>
              "Use `left_join_leaf/4` for “INNER path + LEFT leaf”, or `left_join_path/4` for “LEFT every hop”. " <>
              "Got: #{inspect(assoc_fields)}"
    end

    left_join_leaf(query, assoc_fields, filters, or_filters)
  end

  def left_join_leaf(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    if is_nil(filters) do
      raise ArgumentError, "left_join_leaf/4 expects `filters` to be a list/keyword list, got nil"
    end

    if is_nil(or_filters) do
      raise ArgumentError, "left_join_leaf/4 expects `or_filters` to be a keyword list, got nil"
    end

    filters = List.wrap(filters)
    or_filters = List.wrap(or_filters)

    join_filters =
      if filters == [] and or_filters == [] do
        []
      else
        [filters, or_filters]
      end

    %{
      query
      | operations: [
          {:left_join, assoc_fields, [:leaf, join_filters]}
          | query.operations
        ]
    }
  end

  def left_join_path(%QueryBuilder.Query{} = query, assoc_fields, filters, or_filters) do
    if is_nil(filters) do
      raise ArgumentError, "left_join_path/4 expects `filters` to be a list/keyword list, got nil"
    end

    if is_nil(or_filters) do
      raise ArgumentError, "left_join_path/4 expects `or_filters` to be a keyword list, got nil"
    end

    filters = List.wrap(filters)
    or_filters = List.wrap(or_filters)

    join_filters =
      if filters == [] and or_filters == [] do
        []
      else
        [filters, or_filters]
      end

    %{
      query
      | operations: [
          {:left_join, assoc_fields, [:path, join_filters]}
          | query.operations
        ]
    }
  end

  defp assoc_fields_nested?(assoc_fields) do
    assoc_fields
    |> List.wrap()
    |> Enum.any?(fn
      {_field, nested_assoc_fields} -> nested_assoc_fields != []
      _ -> false
    end)
  end
end
