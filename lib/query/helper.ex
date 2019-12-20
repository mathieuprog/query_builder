defmodule QueryBuilder.Query.Helper do
  @moduledoc false

  def field_and_binding(query, token, field) do
    split_field = String.split(to_string(field), "@")
    [field, assoc_field] = [Enum.at(split_field, 0), Enum.at(split_field, 1)]

    field = String.to_existing_atom(field)
    assoc_field = String.to_existing_atom(assoc_field || "nil")

    _field_and_binding(query, token, [field, assoc_field])
  end

  defp _field_and_binding(query, _token, [field, nil]) do
    {field, QueryBuilder.Utils.root_schema(query)}
  end

  defp _field_and_binding(_query, token, [field, assoc_field]) do
    {:ok, binding} = find_binding_for_assoc_field(assoc_field, token)
    {field, binding}
  end

  defp find_binding_for_assoc_field(_assoc_field, []), do: {:error, :not_found}

  defp find_binding_for_assoc_field(assoc_field, [assoc_data | tail]) do
    if assoc_field == Map.fetch!(assoc_data, :assoc_field) do
      {:ok, assoc_data.assoc_binding}
    else
      find_binding_for_assoc_field(assoc_field, assoc_data.nested_assocs) ||
        find_binding_for_assoc_field(assoc_field, tail)
    end
  end
end
