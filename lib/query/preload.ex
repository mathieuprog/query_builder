defmodule QueryBuilder.Query.Preload do
  @moduledoc false

  require Ecto.Query

  def preload(query, _assoc_list, []), do: query

  def preload(ecto_query, assoc_list) do
    flattened_assoc_data = flatten_assoc_data(assoc_list)

    # Firstly, give `Ecto.Query.preload/3` the list of associations that have been joined, such as:
    # `Ecto.Query.preload(query, [articles: a, user: u, role: r], [articles: {a, [user: {u, [role: r]}]}])`
    ecto_query =
      flattened_assoc_data
      # Filter only the associations that have been joined
      |> Enum.map(fn assoc_data_list ->
        assoc_data_list
        |> Enum.take_while(&effective_joined?(ecto_query, &1))
        |> Enum.map(fn assoc_data -> {assoc_data.assoc_binding, assoc_data.assoc_field} end)
      end)
      |> Enum.reject(&(&1 == []))
      # Get rid of the associations' lists that are redundant;
      # for example for the 4 lists below:
      # `[{:binding1, :field1}]`
      # `[{:binding1, :field1}, {:binding2, :field2}]`
      # `[{:binding1, :field1}, {:binding2, :field2}]`
      # `[{:binding1, :field1}, {:binding2, :field2}, {:binding3, :field3}]`
      # only the last list should be preserved.
      |> Enum.uniq()
      |> (fn lists ->
            Enum.filter(
              lists,
              &(!Enum.any?(lists -- [&1], fn list ->
                  Keyword.equal?(&1, Enum.slice(list, 0, length(&1)))
                end))
            )
          end).()
      |> Enum.reduce(ecto_query, fn list, ecto_query ->
        do_preload_with_bindings(ecto_query, list)
      end)

    # Secondly, give `Ecto.Query.preload/3` the list of associations that have not
    # been joined, such as:
    # `Ecto.Query.preload(query, [articles: [comments: :comment_likes]])`
    ecto_query =
      flattened_assoc_data
      |> Enum.map(fn assoc_data_list ->
        Enum.reverse(assoc_data_list)
        |> Enum.drop_while(&effective_joined?(ecto_query, &1))
        |> Enum.map(& &1.assoc_field)
        |> Enum.reverse()
      end)
      |> Enum.reject(&Enum.empty?(&1))
      |> Enum.map(&convert_list_to_nested_keyword_list(&1))
      |> Enum.reduce(ecto_query, fn list, ecto_query ->
        atom_or_tuple = hd(list)
        preload = List.wrap(atom_or_tuple)
        Ecto.Query.preload(ecto_query, ^preload)
      end)

    ecto_query
  end

  defp flatten_assoc_data(assoc_list) do
    assoc_list
    |> Enum.flat_map(&do_flatten_assoc_data/1)
    |> Enum.filter(&(!is_nil(&1)))
  end

  defp do_flatten_assoc_data(%{nested_assocs: [], preload: preload} = assoc_data) do
    if preload do
      [[Map.delete(assoc_data, :nested_assocs)]]
    else
      []
    end
  end

  defp do_flatten_assoc_data(%{nested_assocs: nested_assocs, preload: preload} = assoc_data) do
    assoc_data_without_nested = Map.delete(assoc_data, :nested_assocs)

    nested_paths =
      for nested_assoc_data <- nested_assocs,
          rest <- do_flatten_assoc_data(nested_assoc_data) do
        if preload do
          [assoc_data_without_nested | rest]
        else
          rest
        end
      end

    if preload and nested_paths == [] do
      [[assoc_data_without_nested]]
    else
      nested_paths
    end
  end

  defp convert_list_to_nested_keyword_list(list) do
    do_convert_list_to_nested_keyword_list(list)
    |> List.wrap()
  end

  defp do_convert_list_to_nested_keyword_list([]), do: []
  defp do_convert_list_to_nested_keyword_list([e]), do: e

  defp do_convert_list_to_nested_keyword_list([head | [penultimate, last]]),
    do: [{head, [{penultimate, last}]}]

  defp do_convert_list_to_nested_keyword_list([head | tail]),
    do: [{head, do_convert_list_to_nested_keyword_list(tail)}]

  defp do_preload_with_bindings(query, []), do: query

  defp do_preload_with_bindings(query, bindings) when is_list(bindings) do
    Ecto.Query.preload(query, ^build_join_preload(bindings))
  end

  defp build_join_preload([{assoc_binding, assoc_field}]) do
    binding_expr = Ecto.Query.dynamic([{^assoc_binding, x}], x)
    [{assoc_field, binding_expr}]
  end

  defp build_join_preload([{assoc_binding, assoc_field} | rest]) do
    binding_expr = Ecto.Query.dynamic([{^assoc_binding, x}], x)
    [{assoc_field, {binding_expr, build_join_preload(rest)}}]
  end

  defp effective_joined?(ecto_query, %{has_joined: true, assoc_binding: assoc_binding}) do
    Ecto.Query.has_named_binding?(ecto_query, assoc_binding)
  end

  defp effective_joined?(_ecto_query, _assoc_data), do: false
end
