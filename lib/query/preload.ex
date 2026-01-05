defmodule QueryBuilder.Query.Preload do
  @moduledoc false

  require Ecto.Query
  alias QueryBuilder.AssocList.PreloadSpec
  alias QueryBuilder.AssocList.JoinSpec
  alias QueryBuilder.Query.{OrderBy, Where}

  def preload(ecto_query, assoc_list) do
    flattened_assoc_data = flatten_assoc_data(assoc_list)
    validate_scoped_separate_preload_leaf_only!(flattened_assoc_data)

    # Firstly, give `Ecto.Query.preload/3` the list of associations that have been joined, such as:
    # `Ecto.Query.preload(query, [articles: a, user: u, role: r], [articles: {a, [user: {u, [role: r]}]}])`
    ecto_query =
      flattened_assoc_data
      |> Enum.map(&join_preload_chain_for_path(ecto_query, &1))
      |> Enum.reject(&Enum.empty?/1)
      |> maximal_preload_chains()
      |> Enum.reduce(ecto_query, fn chain, ecto_query ->
        do_preload_with_bindings(ecto_query, chain)
      end)

    # Secondly, give `Ecto.Query.preload/3` the list of associations that have not
    # been joined, such as:
    # `Ecto.Query.preload(query, [articles: [comments: :comment_likes]])`
    ecto_query =
      flattened_assoc_data
      |> Enum.map(&separate_preload_for_path(ecto_query, &1))
      |> Enum.reject(&(&1 == []))
      |> Enum.reduce(ecto_query, fn preload, ecto_query ->
        Ecto.Query.preload(ecto_query, ^preload)
      end)

    ecto_query
  end

  defp validate_scoped_separate_preload_leaf_only!(flattened_assoc_data) do
    Enum.each(flattened_assoc_data, fn assoc_data_list ->
      scoped_index =
        Enum.find_index(assoc_data_list, fn assoc_data ->
          case assoc_data.preload_spec do
            %PreloadSpec{query_opts: query_opts} when not is_nil(query_opts) -> true
            _ -> false
          end
        end)

      if scoped_index != nil and scoped_index != length(assoc_data_list) - 1 do
        assoc_data = Enum.at(assoc_data_list, scoped_index)

        nested =
          assoc_data_list
          |> Enum.drop(scoped_index + 1)
          |> Enum.map(& &1.assoc_field)

        raise ArgumentError,
              "invalid scoped separate preload for #{inspect(assoc_data.source_schema)}.#{inspect(assoc_data.assoc_field)}: " <>
                "cannot combine `preload_separate_scoped/3` with nested preloads under that association " <>
                "(nested: #{inspect(nested)}). Use an explicit Ecto query-based preload query instead."
      end
    end)
  end

  defp join_preload?(ecto_query, assoc_data) do
    case Map.get(assoc_data, :preload_spec) do
      %PreloadSpec{strategy: :through_join} ->
        ensure_effective_joined!(ecto_query, assoc_data)
        true

      %PreloadSpec{strategy: :separate} ->
        false

      %PreloadSpec{strategy: :auto} ->
        effective_joined?(ecto_query, assoc_data)

      nil ->
        false
    end
  end

  defp preload_through_join?(%{preload_spec: %PreloadSpec{strategy: :through_join}}), do: true
  defp preload_through_join?(_assoc_data), do: false

  defp validate_through_join_prefix!(assoc_data_list) do
    {_prefix, rest} = Enum.split_while(assoc_data_list, &preload_through_join?/1)

    if Enum.any?(rest, &preload_through_join?/1) do
      raise ArgumentError,
            "invalid mixed preload strategies: `preload_through_join` must apply to a prefix " <>
              "of an association path; got: #{inspect(Enum.map(assoc_data_list, & &1.assoc_field))}"
    end
  end

  defp ensure_effective_joined!(ecto_query, assoc_data) do
    unless effective_joined?(ecto_query, assoc_data) do
      raise ArgumentError,
            "preload_through_join requested join-preload for #{inspect(assoc_data.source_schema)}.#{inspect(assoc_data.assoc_field)}, " <>
              "but that association is not joined. " <>
              "Join it first (e.g. `QueryBuilder.left_join(query, #{inspect(assoc_data.assoc_field)})` or filter/order_by through it), " <>
              "or use `preload_separate/2`."
    end
  end

  defp flatten_assoc_data(assoc_list) do
    assoc_list
    |> Enum.flat_map(&do_flatten_assoc_data/1)
  end

  defp do_flatten_assoc_data(%{nested_assocs: [], preload_spec: preload_spec} = assoc_data) do
    if preload_spec != nil do
      [[Map.delete(assoc_data, :nested_assocs)]]
    else
      []
    end
  end

  defp do_flatten_assoc_data(
         %{
           nested_assocs: nested_assocs,
           preload_spec: preload_spec
         } = assoc_data
       ) do
    assoc_data_without_nested = Map.delete(assoc_data, :nested_assocs)

    nested_paths =
      for nested_assoc_data <- nested_assocs,
          rest <- do_flatten_assoc_data(nested_assoc_data) do
        if preload_spec != nil do
          [assoc_data_without_nested | rest]
        else
          rest
        end
      end

    if preload_spec != nil and nested_paths == [] do
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

  defp effective_joined?(
         ecto_query,
         %{join_spec: %JoinSpec{joined?: true}, assoc_binding: assoc_binding}
       ) do
    Ecto.Query.has_named_binding?(ecto_query, assoc_binding)
  end

  defp effective_joined?(_ecto_query, _assoc_data), do: false

  defp join_preload_chain_for_path(ecto_query, assoc_data_list) do
    validate_through_join_prefix!(assoc_data_list)

    assoc_data_list
    |> Enum.take_while(&join_preload?(ecto_query, &1))
    |> Enum.map(fn assoc_data -> {assoc_data.assoc_binding, assoc_data.assoc_field} end)
  end

  defp separate_preload_for_path(ecto_query, assoc_data_list) do
    separate_assoc_data_list =
      assoc_data_list
      |> Enum.reverse()
      |> Enum.drop_while(&join_preload?(ecto_query, &1))
      |> Enum.reverse()

    case separate_assoc_data_list do
      [] ->
        []

      assoc_data_list ->
        assoc_fields = Enum.map(assoc_data_list, & &1.assoc_field)
        leaf = List.last(assoc_data_list)

        assoc_fields =
          case Map.get(leaf, :preload_spec) do
            %PreloadSpec{query_opts: nil} ->
              assoc_fields

            %PreloadSpec{query_opts: opts} ->
              assoc_fields ++ [build_scoped_preload_query!(leaf, opts)]

            nil ->
              assoc_fields
          end

        convert_list_to_nested_keyword_list(assoc_fields)
    end
  end

  defp build_scoped_preload_query!(
         %{assoc_schema: assoc_schema},
         opts
       ) do
    query = assoc_schema._query()

    query =
      case Keyword.get(opts, :where, []) do
        [] -> query
        filters -> Where.where(query, [], filters, [])
      end

    case Keyword.get(opts, :order_by, []) do
      [] -> query
      order_by -> OrderBy.order_by(query, [], order_by)
    end
  end

  # Removes redundant prefix chains so we don't emit join-preload for both:
  # - [a]
  # - [a, b]
  #
  # When [a, b] exists, [a] is redundant because Ecto join-preload for [a, b]
  # already covers [a].
  defp maximal_preload_chains(chains) do
    chains = Enum.uniq(chains)

    Enum.filter(chains, fn chain ->
      not Enum.any?(chains, fn other_chain ->
        other_chain != chain and prefix_chain?(chain, other_chain)
      end)
    end)
  end

  defp prefix_chain?(prefix, list) do
    prefix_length = length(prefix)
    prefix_length <= length(list) and Enum.take(list, prefix_length) == prefix
  end
end
