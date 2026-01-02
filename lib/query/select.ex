defmodule QueryBuilder.Query.Select do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def select(ecto_query, assoc_list, selection) do
    select_expr = build_select_expr!(ecto_query, assoc_list, selection)
    Ecto.Query.select(ecto_query, ^select_expr)
  end

  defp build_select_expr!(ecto_query, assoc_list, selection) when is_function(selection, 1) do
    selection.(&find_field_and_binding_from_token(ecto_query, assoc_list, &1))
  end

  defp build_select_expr!(ecto_query, assoc_list, selection)
       when is_atom(selection) or is_binary(selection) do
    {field, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, selection)
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field))
  end

  defp build_select_expr!(ecto_query, assoc_list, selection) when is_tuple(selection) do
    build_tuple_dynamic!(ecto_query, assoc_list, selection)
  end

  defp build_select_expr!(ecto_query, assoc_list, selection) when is_list(selection) do
    if Keyword.keyword?(selection) do
      if length(selection) != length(Enum.uniq_by(selection, &elem(&1, 0))) do
        raise ArgumentError,
              "select keyword list contains duplicate keys; got: #{inspect(selection)}"
      end

      build_select_expr!(ecto_query, assoc_list, Map.new(selection))
    else
      tokens =
        Enum.map(selection, fn token ->
          if is_atom(token) or is_binary(token) do
            token
          else
            raise ArgumentError,
                  "select list expects field tokens (atoms/strings), got: #{inspect(token)}"
          end
        end)

      token_strings = Enum.map(tokens, &to_string/1)

      if length(token_strings) != length(Enum.uniq(token_strings)) do
        raise ArgumentError,
              "select list contains duplicate keys; " <>
                "keys match the given tokens (and are compared by token string), got: #{inspect(selection)}"
      end

      selection_map = Enum.reduce(tokens, %{}, fn token, acc -> Map.put(acc, token, token) end)
      build_select_map_expr!(selection_map, ecto_query, assoc_list)
    end
  end

  defp build_select_expr!(ecto_query, assoc_list, %{} = selection) do
    build_select_map_expr!(selection, ecto_query, assoc_list)
  end

  defp build_select_expr!(_ecto_query, _assoc_list, selection) do
    raise ArgumentError,
          "select expects a field token, a list of field tokens, a map, or a 1-arity function; got: #{inspect(selection)}"
  end

  defp build_select_map_expr!(selection, ecto_query, assoc_list) do
    Enum.reduce(selection, %{}, fn {key, value}, acc ->
      Map.put(acc, key, build_select_value_expr!(ecto_query, assoc_list, value))
    end)
  end

  defp build_select_value_expr!(_ecto_query, _assoc_list, {:literal, value}), do: value

  defp build_select_value_expr!(ecto_query, assoc_list, value) when is_tuple(value) do
    build_tuple_dynamic!(ecto_query, assoc_list, value)
  end

  defp build_select_value_expr!(ecto_query, assoc_list, value)
       when is_atom(value) or is_binary(value) do
    {field, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, value)
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field))
  end

  defp build_select_value_expr!(_ecto_query, _assoc_list, value), do: value

  defp build_tuple_dynamic!(ecto_query, assoc_list, tuple) do
    element_dynamics =
      tuple
      |> Tuple.to_list()
      |> Enum.map(fn
        %Ecto.Query.DynamicExpr{} = dynamic ->
          dynamic

        {:literal, value} ->
          Ecto.Query.dynamic([], ^value)

        element when is_atom(element) or is_binary(element) ->
          {field, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, element)
          Ecto.Query.dynamic([{^binding, x}], field(x, ^field))

        element when is_tuple(element) ->
          build_tuple_dynamic!(ecto_query, assoc_list, element)

        element ->
          Ecto.Query.dynamic([], ^element)
      end)

    tuple_expr =
      element_dynamics
      |> Enum.with_index()
      |> Enum.map(fn {_dynamic, index} -> {:^, [], [index]} end)
      |> List.to_tuple()

    params = Enum.map(element_dynamics, &{&1, :any})

    %Ecto.Query.DynamicExpr{
      binding: [],
      file: __ENV__.file,
      line: __ENV__.line,
      fun: fn _query ->
        {tuple_expr, params, [], %{}}
      end
    }
  end
end
