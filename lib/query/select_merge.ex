defmodule QueryBuilder.Query.SelectMerge do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def select_merge(ecto_query, assoc_list, selection) do
    select_expr = build_select_merge_expr!(ecto_query, assoc_list, selection)
    Ecto.Query.select_merge(ecto_query, ^select_expr)
  end

  defp build_select_merge_expr!(ecto_query, assoc_list, selection)
       when is_function(selection, 1) do
    selection.(&find_field_and_binding_from_token(ecto_query, assoc_list, &1))
  end

  defp build_select_merge_expr!(ecto_query, assoc_list, %{} = selection) do
    Enum.reduce(selection, %{}, fn {key, value}, acc ->
      Map.put(acc, key, build_select_merge_value_expr!(ecto_query, assoc_list, value))
    end)
  end

  defp build_select_merge_expr!(ecto_query, assoc_list, selection)
       when is_atom(selection) or is_binary(selection) do
    token = to_string(selection)

    if String.contains?(token, "@") do
      raise ArgumentError,
            "select_merge does not support merging an association token (`field@assoc` / `field@assoc@nested_assoc...`) " <>
              "without an explicit key; " <>
              "use a map (e.g. `%{role_name: :name@role}`), got: #{inspect(selection)}"
    end

    field =
      try do
        String.to_existing_atom(token)
      rescue
        ArgumentError ->
          raise ArgumentError, "unknown field #{inspect(token)}"
      end

    build_select_merge_expr!(ecto_query, assoc_list, %{field => selection})
  end

  defp build_select_merge_expr!(ecto_query, assoc_list, selection) when is_list(selection) do
    if Keyword.keyword?(selection) do
      if length(selection) != length(Enum.uniq_by(selection, &elem(&1, 0))) do
        raise ArgumentError,
              "select_merge keyword list contains duplicate keys; got: #{inspect(selection)}"
      end

      build_select_merge_expr!(ecto_query, assoc_list, Map.new(selection))
    else
      tokens =
        Enum.map(selection, fn token ->
          if is_atom(token) or is_binary(token) do
            token
          else
            raise ArgumentError,
                  "select_merge list expects root field tokens (atoms/strings), got: #{inspect(token)}"
          end
        end)

      if Enum.any?(tokens, &(to_string(&1) |> String.contains?("@"))) do
        raise ArgumentError,
              "select_merge does not support merging a list that contains association tokens (`field@assoc` / `field@assoc@nested_assoc...`); " <>
                "use a map with explicit keys instead (e.g. `%{role_name: :name@role}`), got: #{inspect(selection)}"
      end

      if length(tokens) != length(Enum.uniq_by(tokens, &to_string/1)) do
        raise ArgumentError,
              "select_merge list contains duplicates; got: #{inspect(selection)}"
      end

      map =
        Enum.reduce(tokens, %{}, fn token, acc ->
          key_str = to_string(token)

          key =
            try do
              String.to_existing_atom(key_str)
            rescue
              ArgumentError ->
                raise ArgumentError, "unknown field #{inspect(key_str)}"
            end

          Map.put(acc, key, token)
        end)

      build_select_merge_expr!(ecto_query, assoc_list, map)
    end
  end

  defp build_select_merge_expr!(_ecto_query, _assoc_list, selection) do
    raise ArgumentError,
          "select_merge expects a map, a list of root fields, a single root field, or a 1-arity function; got: #{inspect(selection)}"
  end

  defp build_select_merge_value_expr!(_ecto_query, _assoc_list, {:literal, value}), do: value

  defp build_select_merge_value_expr!(ecto_query, assoc_list, value)
       when is_atom(value) or is_binary(value) do
    {field, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, value)
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field))
  end

  defp build_select_merge_value_expr!(_ecto_query, _assoc_list, value), do: value
end
