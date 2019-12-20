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
  def make_joins(query, token, options \\ []) do
    _make_joins(query, token, bindings(token), options, [])
  end

  defp _make_joins(query, [], _, _, new_token), do: {query, new_token}

  defp _make_joins(query, [assoc_data | tail], bindings, options, new_token) do
    mode = Keyword.get(options, :mode)
    type = Keyword.get(options, :type, :inner)

    {query, assoc_data, bindings} = maybe_join(query, assoc_data, bindings, mode, type)

    {query, nested_assocs} =
      if assoc_data.has_joined do
        _make_joins(query, assoc_data.nested_assocs, bindings, options, [])
      else
        {query, assoc_data.nested_assocs}
      end

    assoc_data = %{assoc_data | nested_assocs: nested_assocs}

    {query, new_token} = _make_joins(query, tail, bindings, options, new_token)

    {query, [assoc_data | new_token]}
  end

  defp maybe_join(query, %{has_joined: true} = assoc_data, bindings, _, _),
    do: {query, assoc_data, bindings}

  defp maybe_join(query, %{cardinality: :many} = assoc_data, bindings, :if_preferable, _type),
    do: {query, assoc_data, bindings}

  defp maybe_join(query, assoc_data, bindings, _mode, type) do
    %{
      source_binding: source_binding,
      source_schema: source_schema,
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema
    } = assoc_data

    unless Enum.member?(bindings, assoc_binding) do
      query =
        if String.contains?(to_string(assoc_binding), "__") do
          source_schema.join(query, type, source_binding, assoc_field)
        else
          assoc_schema.join(query, type, source_binding, assoc_field)
        end

      {
        query,
        %{assoc_data | has_joined: true},
        [assoc_binding | bindings]
      }
    else
      {query, assoc_data, bindings}
    end
  end

  defp bindings([]), do: []

  defp bindings([assoc_data | tail]) do
    list = bindings(assoc_data.nested_assocs) ++ bindings(tail)

    if assoc_data.has_joined do
      [assoc_data.assoc_binding | list]
    else
      list
    end
  end
end
