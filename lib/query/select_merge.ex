defmodule QueryBuilder.Query.SelectMerge do
  @moduledoc """
  Handles select_merge query expressions for QueryBuilder.

  This module provides the implementation for merging field selections into existing
  query selections. Unlike select which replaces the selection, select_merge adds to
  the existing selection, preserving Ecto's select_merge semantics.
  """

  require Ecto.Query
  import QueryBuilder.Utils

  @doc """
  Applies a select_merge operation to an Ecto query.

  Merges new selections with existing ones. Handles maps, lists, single fields,
  and custom functions. Supports QueryBuilder's association field syntax using `@`.

  ## Parameters
    - `ecto_query`: The Ecto query to merge selections into
    - `assoc_list`: List of associations that have been joined
    - `selection`: The fields to merge (can be atom, string, list, map, or function)

  ## Examples

      # Single field
      select_merge(query, [], :email)

      # List of fields
      select_merge(query, [], [:created_at, :updated_at])

      # Map with renamed fields
      select_merge(query, [], %{last_login: :updated_at})

      # With associations
      select_merge(query, [:role], %{role_name: :name@role})
  """
  def select_merge(ecto_query, assoc_list, selection) do
    case selection do
      # Map selection - the standard case
      %{} = map ->
        map_expr =
          Enum.map(map, fn {k, v} ->
            case v do
              field when is_atom(field) or is_binary(field) ->
                # Handle association fields properly
                {field_name, binding} =
                  find_field_and_binding_from_token(ecto_query, assoc_list, field)

                {k, Ecto.Query.dynamic([{^binding, x}], field(x, ^field_name))}

              value ->
                {k, value}
            end
          end)
          |> Map.new()

        Ecto.Query.select_merge(ecto_query, ^map_expr)

      # List of fields - convert to map
      fields when is_list(fields) ->
        map = build_map_from_field_list(fields)
        select_merge(ecto_query, assoc_list, map)

      # Single field - convert to map
      field when is_atom(field) or is_binary(field) ->
        field_str = to_string(field)

        key =
          if String.contains?(field_str, "@") do
            String.to_atom(field_str)
          else
            to_atom(field)
          end

        select_merge(ecto_query, assoc_list, %{key => field})

      # Custom function
      fun when is_function(fun) ->
        dynamic = fun.(&find_field_and_binding_from_token(ecto_query, assoc_list, &1))
        Ecto.Query.select_merge(ecto_query, ^dynamic)

      # Unsupported type
      other ->
        raise ArgumentError,
              "select_merge expects a map, list, or single field, got: #{inspect(other)}"
    end
  end

  # Helper to build a map from a field list, preserving association field names as keys
  defp build_map_from_field_list(fields) do
    Enum.reduce(fields, %{}, fn field, acc ->
      # For association fields, we want to keep the full name as the key
      field_str = to_string(field)

      key =
        if String.contains?(field_str, "@") do
          # Keep the association field name as-is for the key
          String.to_atom(field_str)
        else
          # Regular field
          to_atom(field)
        end

      Map.put(acc, key, field)
    end)
  end

  defp to_atom(value) when is_atom(value), do: value
  defp to_atom(value) when is_binary(value), do: String.to_existing_atom(value)
  defp to_atom(value), do: value
end
