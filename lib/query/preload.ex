defmodule QueryBuilder.Query.Preload do
  @moduledoc false

  require Ecto.Query

  def preload(query, value) do
    token = QueryBuilder.Token.token(query, value)
    {query, token} = QueryBuilder.JoinMaker.make_joins(query, token, mode: :if_preferable)

    do_preload(query, token)
  end

  defp do_preload(query, token) do
    flattened_assoc_data = flatten_assoc_data(token)

    query =
      flattened_assoc_data
      |> Enum.map(fn assoc_data_list ->
        Enum.flat_map(assoc_data_list, fn
          %{has_joined: false} -> []
          assoc_data -> [{assoc_data.assoc_binding, assoc_data.assoc_field}]
        end)
      end)
      |> Enum.uniq()
      |> (fn lists ->
            Enum.filter(
              lists,
              &(!Enum.any?(lists -- [&1], fn list ->
                  # TODO use Keyword list functions
                  String.starts_with?(
                    Enum.map_join(list, fn {k, v} -> "#{k}#{v}" end),
                    Enum.map_join(&1, fn {k, v} -> "#{k}#{v}" end)
                  )
                end))
            )
          end).()
      |> Enum.reduce(query, fn list, query ->
        do_preload_with_bindings(query, list)
      end)

    query =
      flattened_assoc_data
      |> Enum.map(fn assoc_data_list ->
        Enum.reverse(assoc_data_list)
        |> Enum.drop_while(& &1.has_joined)
        |> Enum.map(& &1.assoc_field)
        |> Enum.reverse()
      end)
      |> Enum.reject(&Enum.empty?(&1))
      |> Enum.map(&convert_list_to_nested_keyword_list(&1))
      |> Enum.reduce(query, fn list, query ->
        atom_or_tuple = hd(list)
        Ecto.Query.preload(query, ^atom_or_tuple)
      end)

    query
  end

  defp flatten_assoc_data(token) do
    Enum.flat_map(token, &_flatten_assoc_data/1)
  end

  defp _flatten_assoc_data(%{nested_assocs: []} = assoc_data) do
    [[Map.delete(assoc_data, :nested_assocs)]]
  end

  defp _flatten_assoc_data(%{nested_assocs: nested_assocs} = assoc_data) do
    for nested_assoc_data <- nested_assocs,
        rest <- _flatten_assoc_data(nested_assoc_data) do
      [Map.delete(assoc_data, :nested_assocs) | rest]
    end
  end

  defp convert_list_to_nested_keyword_list(list) do
    _convert_list_to_nested_keyword_list(list)
    |> List.wrap()
  end

  defp _convert_list_to_nested_keyword_list([]), do: []
  defp _convert_list_to_nested_keyword_list([e]), do: e

  defp _convert_list_to_nested_keyword_list([head | [penultimate, last]]),
    do: [{head, [{penultimate, last}]}]

  defp _convert_list_to_nested_keyword_list([head | tail]),
    do: [{head, _convert_list_to_nested_keyword_list(tail)}]

  defp do_preload_with_bindings(query, []), do: query

  # 🤢
  defp do_preload_with_bindings(query, [{assoc_binding, assoc_field}]) do
    Ecto.Query.preload(query, [{^assoc_binding, x}], [
      {^assoc_field, x}
    ])
  end

  # 🤢🤢
  defp do_preload_with_bindings(query, [
         {assoc_binding1, assoc_field1},
         {assoc_binding2, assoc_field2}
       ]) do
    Ecto.Query.preload(query, [{^assoc_binding1, x}, {^assoc_binding2, y}], [
      {^assoc_field1, {x, [{^assoc_field2, y}]}}
    ])
  end

  # 🤢🤢🤮
  defp do_preload_with_bindings(query, [
         {assoc_binding1, assoc_field1},
         {assoc_binding2, assoc_field2},
         {assoc_binding3, assoc_field3}
       ]) do
    Ecto.Query.preload(
      query,
      [{^assoc_binding1, x}, {^assoc_binding2, y}, {^assoc_binding3, z}],
      [
        {^assoc_field1, {x, [{^assoc_field2, {y, [{^assoc_field3, z}]}}]}}
      ]
    )
  end

  # 🤢🤢🤮🤮
  defp do_preload_with_bindings(query, [
         {assoc_binding1, assoc_field1},
         {assoc_binding2, assoc_field2},
         {assoc_binding3, assoc_field3},
         {assoc_binding4, assoc_field4}
       ]) do
    Ecto.Query.preload(
      query,
      [{^assoc_binding1, x}, {^assoc_binding2, y}, {^assoc_binding3, z}, {^assoc_binding4, a}],
      [
        {^assoc_field1, {x, [{^assoc_field2, {y, [{^assoc_field3, {z, [{^assoc_field4, a}]}}]}}]}}
      ]
    )
  end

  # 🤢🤢🤮🤮🤮
  defp do_preload_with_bindings(query, [
         {assoc_binding1, assoc_field1},
         {assoc_binding2, assoc_field2},
         {assoc_binding3, assoc_field3},
         {assoc_binding4, assoc_field4},
         {assoc_binding5, assoc_field5}
       ]) do
    Ecto.Query.preload(
      query,
      [
        {^assoc_binding1, x},
        {^assoc_binding2, y},
        {^assoc_binding3, z},
        {^assoc_binding4, a},
        {^assoc_binding5, b}
      ],
      [
        {^assoc_field1,
         {x,
          [
            {^assoc_field2,
             {y, [{^assoc_field3, {z, [{^assoc_field4, {a, [{^assoc_field5, b}]}}]}}]}}
          ]}}
      ]
    )
  end

  # 🤢🤢🤮🤮🤮🤮
  defp do_preload_with_bindings(query, [
         {assoc_binding1, assoc_field1},
         {assoc_binding2, assoc_field2},
         {assoc_binding3, assoc_field3},
         {assoc_binding4, assoc_field4},
         {assoc_binding5, assoc_field5},
         {assoc_binding6, assoc_field6}
       ]) do
    Ecto.Query.preload(
      query,
      [
        {^assoc_binding1, x},
        {^assoc_binding2, y},
        {^assoc_binding3, z},
        {^assoc_binding4, a},
        {^assoc_binding5, b},
        {^assoc_binding6, c}
      ],
      [
        {^assoc_field1,
         {x,
          [
            {^assoc_field2,
             {y,
              [
                {^assoc_field3,
                 {z, [{^assoc_field4, {a, [{^assoc_field5, {b, [{^assoc_field6, c}]}}]}}]}}
              ]}}
          ]}}
      ]
    )
  end
end
