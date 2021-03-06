defmodule QueryBuilder.Utils do
  @moduledoc false

  def root_schema(query) do
    %{from: %{source: {_, context}}} = Ecto.Queryable.to_query(query)
    context
  end

  def find_field_and_binding_from_token(query, assoc_list, field) do
    split_field = String.split(to_string(field), "@")
    [field, assoc_field] = [Enum.at(split_field, 0), Enum.at(split_field, 1)]

    field = String.to_existing_atom(field)
    assoc_field = String.to_existing_atom(assoc_field || "nil")

    do_find_field_and_binding_from_token(query, assoc_list, [field, assoc_field])
  end

  defp do_find_field_and_binding_from_token(query, _assoc_list, [field, nil]) do
    {field, QueryBuilder.Utils.root_schema(query)}
  end

  defp do_find_field_and_binding_from_token(_query, assoc_list, [field, assoc_field]) do
    {:ok, binding} = find_binding_from_token(assoc_list, assoc_field)
    {field, binding}
  end

  defp find_binding_from_token([], _field), do: {:error, :not_found}

  defp find_binding_from_token([assoc_data | tail], field) do
    if field == Map.fetch!(assoc_data, :assoc_field) do
      {:ok, assoc_data.assoc_binding}
    else
      with {:ok, binding} <- find_binding_from_token(assoc_data.nested_assocs, field) do
        {:ok, binding}
      else
        {:error, :not_found} ->
          find_binding_from_token(tail, field)
      end
    end
  end
end
