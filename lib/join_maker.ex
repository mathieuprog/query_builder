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

      bindings =
        if Enum.member?(bindings, assoc_binding) do
          bindings
        else
          [assoc_binding | bindings]
        end

      {ecto_query, %{assoc_data | has_joined: true}, bindings}
    else
      join_type = if join_type == :left, do: :left, else: :inner

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
end
