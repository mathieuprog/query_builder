defmodule QueryBuilder.Query.Select do
  @moduledoc """
  Handles select query expressions for QueryBuilder.

  This module provides the implementation for selecting specific fields from queries,
  supporting various selection formats including lists, maps, tuples, single fields,
  and custom functions. It also supports QueryBuilder's association field syntax using `@`.
  """

  require Ecto.Query
  import QueryBuilder.Utils

  @doc """
  Applies a select operation to an Ecto query.

  Handles various selection types including lists, maps, tuples, single fields,
  and custom functions. Supports QueryBuilder's association field syntax using `@`.

  ## Parameters
    - `ecto_query`: The Ecto query to apply the selection to
    - `assoc_list`: List of associations that have been joined
    - `selection`: The fields to select (can be atom, string, list, map, tuple, or function)

  ## Examples

      # Single field
      select(query, [], :name)

      # List of fields
      select(query, [], [:id, :name, :email])

      # Map with renamed fields
      select(query, [], %{user_id: :id, user_name: :name})

      # Tuple selection
      select(query, [], {:id, :name})

      # With associations
      select(query, [:role], [:id, :name, :name@role])
  """
  def select(ecto_query, assoc_list, selection) do
    case selection do
      # Single field
      field when is_atom(field) or is_binary(field) ->
        # Handle association fields
        {field_name, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, field)
        dynamic = Ecto.Query.dynamic([{^binding, x}], field(x, ^field_name))
        Ecto.Query.select(ecto_query, ^dynamic)

      # List of fields - convert to map
      fields when is_list(fields) ->
        # Convert list to map where keys are field names
        map = build_map_from_field_list(ecto_query, assoc_list, fields)
        select(ecto_query, assoc_list, map)

      # Map selection
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

        Ecto.Query.select(ecto_query, ^map_expr)

      # Tuple selection - for now, only support simple field tuples
      tuple when is_tuple(tuple) ->
        # Convert tuple fields to list
        fields = Tuple.to_list(tuple)

        # Get all bindings needed
        source_schema = root_schema(ecto_query)

        bindings_needed =
          fields
          |> Enum.map(fn field ->
            {_, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, field)
            binding
          end)
          |> Enum.uniq()

        # If all fields are from the root schema, use simple select
        if bindings_needed == [source_schema] do
          field_atoms =
            Enum.map(fields, fn field ->
              {field_atom, _} = find_field_and_binding_from_token(ecto_query, assoc_list, field)
              field_atom
            end)

          case field_atoms do
            [f1, f2] ->
              Ecto.Query.select(ecto_query, [q], {field(q, ^f1), field(q, ^f2)})

            [f1, f2, f3] ->
              Ecto.Query.select(ecto_query, [q], {field(q, ^f1), field(q, ^f2), field(q, ^f3)})

            _ ->
              raise ArgumentError, "Tuple selection currently supports only 2 or 3 fields"
          end
        else
          # For mixed bindings, build list of expressions and convert to tuple
          # This is more complex and may need further work
          raise ArgumentError,
                "Tuple selection with fields from different associations is not yet supported"
        end

      # Custom function
      fun when is_function(fun) ->
        dynamic = fun.(&find_field_and_binding_from_token(ecto_query, assoc_list, &1))
        Ecto.Query.select(ecto_query, ^dynamic)

      # Unsupported type
      other ->
        raise FunctionClauseError,
          module: __MODULE__,
          function: :select,
          arity: 3,
          kind: :def,
          args: [ecto_query, assoc_list, other],
          clauses: []
    end
  end

  # Helper to build a map from a field list, preserving association field names as keys
  defp build_map_from_field_list(_ecto_query, _assoc_list, fields) do
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
