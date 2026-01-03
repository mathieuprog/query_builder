defmodule QueryBuilder.JoinMaker do
  @moduledoc false

  require Ecto.Query

  @doc """
  Emits joins described by the assoc tree.

  Associations marked as `join?: false` are not joined.
  """
  def make_joins(ecto_query, assoc_list) do
    do_make_joins(ecto_query, assoc_list, [], [], assoc_list)
    # returns {ecto_query, new_assoc_list}
  end

  defp do_make_joins(ecto_query, [], _, new_assoc_list, _original_assoc_list),
    do: {ecto_query, new_assoc_list}

  defp do_make_joins(
         ecto_query,
         [assoc_data | tail],
         bindings,
         new_assoc_list,
         original_assoc_list
       ) do
    {ecto_query, assoc_data, bindings} =
      maybe_join(ecto_query, assoc_data, bindings, original_assoc_list)

    {ecto_query, nested_assocs} =
      if assoc_data.has_joined do
        do_make_joins(ecto_query, assoc_data.nested_assocs, bindings, [], original_assoc_list)
      else
        {ecto_query, assoc_data.nested_assocs}
      end

    assoc_data = %{assoc_data | nested_assocs: nested_assocs}

    {ecto_query, new_assoc_list} =
      do_make_joins(ecto_query, tail, bindings, new_assoc_list, original_assoc_list)

    {ecto_query, [assoc_data | new_assoc_list]}
  end

  defp maybe_join(
         ecto_query,
         %{join?: false} = assoc_data,
         bindings,
         _original_assoc_list
       ),
       do: {ecto_query, assoc_data, bindings}

  defp maybe_join(ecto_query, assoc_data, bindings, original_assoc_list) do
    %{
      source_binding: source_binding,
      source_schema: source_schema,
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema,
      join_type: join_type
    } = assoc_data

    if Ecto.Query.has_named_binding?(ecto_query, assoc_binding) do
      if assoc_data.join_filters != [] do
        raise ArgumentError,
              "QueryBuilder attempted to join #{inspect(source_schema)}.#{inspect(assoc_field)} " <>
                "(assoc schema #{inspect(assoc_schema)}) using named binding #{inspect(assoc_binding)}, " <>
                "but the query already has a named binding with that name. " <>
                "This association also has join filters (from left_join/4 and/or the authorizer), " <>
                "and QueryBuilder cannot safely apply those filters to an already-existing join. " <>
                "Fix: remove the pre-joined binding, or join it under a different named binding."
      end

      expected_qualifier = join_type

      existing_join =
        ecto_query.joins
        |> Enum.find(fn
          %Ecto.Query.JoinExpr{as: as} when as == assoc_binding -> true
          _ -> false
        end)

      cond do
        is_nil(existing_join) ->
          raise ArgumentError,
                "QueryBuilder attempted to reuse an existing named binding #{inspect(assoc_binding)} for " <>
                  "#{inspect(source_schema)}.#{inspect(assoc_field)}, but could not find a corresponding join " <>
                  "expression in the query. This is likely a query construction bug; please report it."

        existing_join.qual != expected_qualifier ->
          raise ArgumentError,
                "QueryBuilder attempted to join #{inspect(source_schema)}.#{inspect(assoc_field)} " <>
                  "(assoc schema #{inspect(assoc_schema)}) using named binding #{inspect(assoc_binding)}, " <>
                  "but the query already has that binding joined as #{inspect(existing_join.qual)} while " <>
                  "QueryBuilder requires #{inspect(expected_qualifier)}. " <>
                  "QueryBuilder cannot change the join qualifier of an existing join. " <>
                  "Fix: remove the pre-joined binding, or join it with the required qualifier under that binding."

        true ->
          :ok
      end

      expected_source_index = binding_index!(ecto_query, source_binding)
      expected_assoc = {expected_source_index, assoc_field}

      case existing_join.assoc do
        ^expected_assoc ->
          :ok

        nil ->
          raise ArgumentError,
                "QueryBuilder attempted to reuse existing named binding #{inspect(assoc_binding)} for " <>
                  "#{inspect(source_schema)}.#{inspect(assoc_field)}, but the existing join under that binding " <>
                  "is not an association join for that association. " <>
                  "QueryBuilder can only reuse a named binding when it was created by joining the same association " <>
                  "(e.g. `join: x in assoc(u, #{inspect(assoc_field)}), as: ^#{inspect(assoc_binding)}`). " <>
                  "Fix: rename the existing binding, or join the correct association under that binding."

        {actual_source_index, actual_assoc_field} ->
          actual_source_binding = binding_name_for_index(ecto_query, actual_source_index)

          raise ArgumentError,
                "QueryBuilder attempted to reuse existing named binding #{inspect(assoc_binding)} for " <>
                  "#{inspect(source_schema)}.#{inspect(assoc_field)}, but the existing join under that binding " <>
                  "is an association join for #{inspect(actual_source_binding)}.#{inspect(actual_assoc_field)} " <>
                  "instead. QueryBuilder cannot safely reuse a binding for a different association join. " <>
                  "Fix: rename the existing binding, or join the correct association under that binding."

        other ->
          raise ArgumentError,
                "QueryBuilder attempted to reuse existing named binding #{inspect(assoc_binding)} for " <>
                  "#{inspect(source_schema)}.#{inspect(assoc_field)}, but the existing join under that binding " <>
                  "has an unexpected association descriptor: #{inspect(other)}. " <>
                  "Fix: rename the existing binding, or join the correct association under that binding."
      end

      bindings =
        if Enum.member?(bindings, assoc_binding) do
          bindings
        else
          [assoc_binding | bindings]
        end

      {ecto_query, %{assoc_data | has_joined: true}, bindings}
    else
      on =
        if assoc_data.join_filters != [] do
          assoc_data.join_filters
          |> Enum.map(fn [filters, or_filters] ->
            QueryBuilder.Query.Where.build_dynamic_query(
              ecto_query,
              original_assoc_list,
              filters,
              or_filters
            )
          end)
          |> Enum.reduce(&Ecto.Query.dynamic(^&1 and ^&2))
        else
          []
        end

      unless Enum.member?(bindings, assoc_binding) do
        ecto_query = source_schema._join(ecto_query, join_type, source_binding, assoc_field, on)

        {
          ecto_query,
          %{assoc_data | has_joined: true},
          [assoc_binding | bindings]
        }
      else
        {ecto_query, assoc_data, bindings}
      end
    end
  end

  defp binding_index!(ecto_query, binding) do
    cond do
      ecto_query.from.as == binding ->
        0

      true ->
        ecto_query.joins
        |> Enum.with_index(1)
        |> Enum.find_value(fn
          {%Ecto.Query.JoinExpr{as: as}, index} when as == binding -> index
          _ -> nil
        end)
        |> case do
          nil ->
            raise ArgumentError,
                  "QueryBuilder expected the query to have named binding #{inspect(binding)}, " <>
                    "but it was not found. This is likely a query construction bug; please report it."

          index ->
            index
        end
    end
  end

  defp binding_name_for_index(%Ecto.Query{from: %{as: as}}, 0) when not is_nil(as), do: as
  defp binding_name_for_index(%Ecto.Query{}, 0), do: :root

  defp binding_name_for_index(%Ecto.Query{joins: joins}, index)
       when is_integer(index) and index > 0 do
    case Enum.at(joins, index - 1) do
      %Ecto.Query.JoinExpr{as: as} when not is_nil(as) -> as
      _ -> {:binding_index, index}
    end
  end
end
