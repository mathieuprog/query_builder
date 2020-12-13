defmodule QueryBuilder.JoinMaker do
  @moduledoc false

  require Ecto.Query

  @doc ~S"""
  Options may be:
  * `:mode`: if set to `:if_preferable`, schemas are joined only if it is better
  performance-wise; this happens only for one case: when the association has a
  one-to-one cardinality, it is better to join and include the association's result
  in the result set of the query, rather than emitting a new DB query.
  * `:type`: see `Ecto.Query.join/5`'s qualifier argument for possible values.
  """
  def make_joins(ecto_query, assoc_list) do
    do_make_joins(ecto_query, assoc_list, [], [])
    # returns {ecto_query, new_assoc_list}
  end

  defp do_make_joins(ecto_query, [], _, new_assoc_list), do: {ecto_query, new_assoc_list}

  defp do_make_joins(ecto_query, [assoc_data | tail], bindings, new_assoc_list) do
    {ecto_query, assoc_data, bindings} = maybe_join(ecto_query, assoc_data, bindings)

    {ecto_query, nested_assocs} =
      if assoc_data.has_joined do
        do_make_joins(ecto_query, assoc_data.nested_assocs, bindings, [])
      else
        {ecto_query, assoc_data.nested_assocs}
      end

    assoc_data = %{assoc_data | nested_assocs: nested_assocs}

    {ecto_query, new_assoc_list} = do_make_joins(ecto_query, tail, bindings, new_assoc_list)

    {ecto_query, [assoc_data | new_assoc_list]}
  end

#  defp maybe_join(ecto_query, %{has_joined: true} = assoc_data, bindings),
#    do: {ecto_query, assoc_data, bindings}

  defp maybe_join(ecto_query, %{cardinality: :many, join_type: :inner_if_cardinality_is_one} = assoc_data, bindings),
    do: {ecto_query, assoc_data, bindings}

  defp maybe_join(ecto_query, assoc_data, bindings) do
    %{
      source_binding: source_binding,
      source_schema: source_schema,
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema,
      join_type: join_type
    } = assoc_data

    has_joined = Ecto.Query.has_named_binding?(ecto_query, assoc_binding)
    if has_joined do
      raise "has already joined"
    end

    join_type =
      case join_type do
        :left ->
          :left

        _ ->
          :inner
      end

    unless Enum.member?(bindings, assoc_binding) do
      # see schema.ex's module doc in order to understand what's going on here
      ecto_query =
        if String.contains?(to_string(assoc_binding), "__") do
          source_schema._join(ecto_query, join_type, source_binding, assoc_field)
        else
          assoc_schema._join(ecto_query, join_type, source_binding, assoc_field)
        end

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
