defmodule QueryBuilder.Query.Preload do
  @moduledoc false

  require Ecto.Query
  alias Ecto.Query
  alias QueryBuilder.AssocList.PreloadSpec
  alias QueryBuilder.AssocList
  alias QueryBuilder.Query.OrderBy
  alias QueryBuilder.Query.Where

  # Pagination optimization needs to treat separate preloads as "deferrable hydration":
  # we can run the root page query without them (so we don't preload the lookahead row),
  # then `Repo.preload/2` only the trimmed page entries. Join-preloads must stay in-query.
  def split_for_pagination(%Query{} = ecto_query, %AssocList{} = assoc_list) do
    deferred_preloads = deferred_preload_entries(assoc_list)

    if deferred_preloads == [] do
      {ecto_query, []}
    else
      ecto_query =
        ecto_query
        |> Query.exclude(:preload)
        |> apply_through_join_preloads(assoc_list)

      {ecto_query, deferred_preloads}
    end
  end

  defp deferred_preload_entries(%AssocList{} = assoc_list) do
    build_deferred_preload_entries(assoc_list.roots)
  end

  defp apply_through_join_preloads(%Query{} = ecto_query, %AssocList{} = assoc_list) do
    preloads = build_through_join_preload_entries(ecto_query, assoc_list.roots, true, [])

    if preloads == [] do
      ecto_query
    else
      Query.preload(ecto_query, ^preloads)
    end
  end

  def preload(ecto_query, %AssocList{} = assoc_list) do
    preloads = build_preload_entries(ecto_query, assoc_list.roots, true, [])

    if preloads == [] do
      ecto_query
    else
      Query.preload(ecto_query, ^preloads)
    end
  end

  defp build_preload_entries(_ecto_query, nodes_map, _through_join_allowed?, _path_rev)
       when is_map(nodes_map) and map_size(nodes_map) == 0,
       do: []

  defp build_preload_entries(ecto_query, nodes_map, through_join_allowed?, path_rev)
       when is_map(nodes_map) do
    nodes_map
    |> Enum.reduce([], fn {_assoc_field, assoc_data}, acc ->
      case build_preload_entry(ecto_query, assoc_data, through_join_allowed?, path_rev) do
        nil -> acc
        entry -> [entry | acc]
      end
    end)
    |> Enum.reverse()
  end

  defp build_deferred_preload_entries(nodes_map)
       when is_map(nodes_map) and map_size(nodes_map) == 0,
       do: []

  defp build_deferred_preload_entries(nodes_map) when is_map(nodes_map) do
    nodes_map
    |> Enum.reduce([], fn {_assoc_field, assoc_data}, acc ->
      case build_deferred_preload_entry(assoc_data) do
        nil -> acc
        entry -> [entry | acc]
      end
    end)
    |> Enum.reverse()
  end

  defp build_deferred_preload_entry(
         %{preload_spec: nil, nested_assocs: nested_assocs} = assoc_data
       )
       when is_map(nested_assocs) do
    nested_preloads = build_deferred_preload_entries(nested_assocs)

    if nested_preloads == [] do
      nil
    else
      raise ArgumentError,
            "internal error: found nested preloads under #{inspect(assoc_data.source_schema)}.#{inspect(assoc_data.assoc_field)} " <>
              "without a preload spec; nested: #{inspect(nested_preloads)}"
    end
  end

  defp build_deferred_preload_entry(
         %{preload_spec: %PreloadSpec{strategy: :through_join}, nested_assocs: nested_assocs} =
           assoc_data
       )
       when is_map(nested_assocs) do
    nested_preloads = build_deferred_preload_entries(nested_assocs)

    if nested_preloads == [] do
      nil
    else
      {assoc_data.assoc_field, nested_preloads}
    end
  end

  defp build_deferred_preload_entry(
         %{
           preload_spec: %PreloadSpec{strategy: :separate, query_opts: nil},
           nested_assocs: nested_assocs
         } =
           assoc_data
       )
       when is_map(nested_assocs) do
    nested_preloads = build_deferred_preload_entries(nested_assocs)

    case nested_preloads do
      [] -> assoc_data.assoc_field
      nested -> {assoc_data.assoc_field, nested}
    end
  end

  defp build_deferred_preload_entry(
         %{
           preload_spec: %PreloadSpec{strategy: :separate, query_opts: opts},
           nested_assocs: nested_assocs
         } =
           assoc_data
       )
       when is_map(nested_assocs) do
    nested_preloads = build_deferred_preload_entries(nested_assocs)

    if nested_preloads != [] do
      nested = nested_preloaded_assoc_fields(nested_assocs)

      raise ArgumentError,
            "invalid scoped separate preload for #{inspect(assoc_data.source_schema)}.#{inspect(assoc_data.assoc_field)}: " <>
              "cannot combine `preload_separate_scoped/3` with nested preloads under that association " <>
              "(nested: #{inspect(nested)}). Use an explicit Ecto query-based preload query instead."
    end

    {assoc_data.assoc_field, build_scoped_preload_query!(assoc_data, opts)}
  end

  defp build_through_join_preload_entries(
         _ecto_query,
         nodes_map,
         _through_join_allowed?,
         _path_rev
       )
       when is_map(nodes_map) and map_size(nodes_map) == 0,
       do: []

  defp build_through_join_preload_entries(ecto_query, nodes_map, through_join_allowed?, path_rev)
       when is_map(nodes_map) do
    nodes_map
    |> Enum.reduce([], fn {_assoc_field, assoc_data}, acc ->
      case build_through_join_preload_entry(
             ecto_query,
             assoc_data,
             through_join_allowed?,
             path_rev
           ) do
        nil -> acc
        entry -> [entry | acc]
      end
    end)
    |> Enum.reverse()
  end

  defp build_through_join_preload_entry(
         ecto_query,
         %{preload_spec: %PreloadSpec{strategy: :through_join}} = assoc_data,
         through_join_allowed?,
         path_rev
       ) do
    path_rev = [assoc_data.assoc_field | path_rev]

    if not through_join_allowed? do
      path = Enum.reverse(path_rev)

      raise ArgumentError,
            "invalid mixed preload strategies: `preload_through_join` must apply to a prefix " <>
              "of an association path; got: #{inspect(path)}"
    end

    ensure_effective_joined!(ecto_query, assoc_data)

    nested_preloads =
      build_through_join_preload_entries(
        ecto_query,
        assoc_data.nested_assocs,
        true,
        path_rev
      )

    binding_expr = Ecto.Query.dynamic([{^assoc_data.assoc_binding, x}], x)

    case nested_preloads do
      [] -> {assoc_data.assoc_field, binding_expr}
      nested -> {assoc_data.assoc_field, {binding_expr, nested}}
    end
  end

  defp build_through_join_preload_entry(
         _ecto_query,
         %{preload_spec: %PreloadSpec{strategy: :separate}},
         _through_join_allowed?,
         _path_rev
       ) do
    nil
  end

  defp build_through_join_preload_entry(
         ecto_query,
         %{preload_spec: nil, nested_assocs: nested_assocs} = assoc_data,
         through_join_allowed?,
         path_rev
       )
       when is_map(nested_assocs) do
    nested_preloads =
      build_through_join_preload_entries(
        ecto_query,
        nested_assocs,
        through_join_allowed?,
        [assoc_data.assoc_field | path_rev]
      )

    if nested_preloads == [] do
      nil
    else
      raise ArgumentError,
            "internal error: found nested through-join preloads under #{inspect(assoc_data.source_schema)}.#{inspect(assoc_data.assoc_field)} " <>
              "without a preload spec; nested: #{inspect(nested_preloads)}"
    end
  end

  defp build_preload_entry(
         _ecto_query,
         %{preload_spec: nil, nested_assocs: nested_assocs},
         _through_join_allowed?,
         _path_rev
       )
       when is_map(nested_assocs) and map_size(nested_assocs) == 0 do
    nil
  end

  defp build_preload_entry(
         ecto_query,
         %{preload_spec: nil, nested_assocs: nested_assocs} = assoc_data,
         through_join_allowed?,
         path_rev
       )
       when is_map(nested_assocs) do
    nested_preloads =
      build_preload_entries(
        ecto_query,
        nested_assocs,
        through_join_allowed?,
        [assoc_data.assoc_field | path_rev]
      )

    if nested_preloads == [] do
      nil
    else
      raise ArgumentError,
            "internal error: found nested preloads under #{inspect(assoc_data.source_schema)}.#{inspect(assoc_data.assoc_field)} " <>
              "without a preload spec; nested: #{inspect(nested_preloads)}"
    end
  end

  defp build_preload_entry(
         ecto_query,
         %{preload_spec: %PreloadSpec{} = preload_spec} = assoc_data,
         through_join_allowed?,
         path_rev
       ) do
    path_rev = [assoc_data.assoc_field | path_rev]

    child_through_join_allowed? =
      case preload_spec.strategy do
        :through_join ->
          if not through_join_allowed? do
            path = Enum.reverse(path_rev)

            raise ArgumentError,
                  "invalid mixed preload strategies: `preload_through_join` must apply to a prefix " <>
                    "of an association path; got: #{inspect(path)}"
          end

          true

        :separate ->
          false
      end

    nested_preloads =
      build_preload_entries(
        ecto_query,
        assoc_data.nested_assocs,
        child_through_join_allowed?,
        path_rev
      )

    case preload_spec do
      %PreloadSpec{strategy: :separate, query_opts: nil} ->
        case nested_preloads do
          [] -> assoc_data.assoc_field
          nested -> {assoc_data.assoc_field, nested}
        end

      %PreloadSpec{strategy: :separate, query_opts: opts} ->
        if nested_preloads != [] do
          nested = nested_preloaded_assoc_fields(assoc_data.nested_assocs)

          raise ArgumentError,
                "invalid scoped separate preload for #{inspect(assoc_data.source_schema)}.#{inspect(assoc_data.assoc_field)}: " <>
                  "cannot combine `preload_separate_scoped/3` with nested preloads under that association " <>
                  "(nested: #{inspect(nested)}). Use an explicit Ecto query-based preload query instead."
        end

        {assoc_data.assoc_field, build_scoped_preload_query!(assoc_data, opts)}

      %PreloadSpec{strategy: :through_join, query_opts: nil} ->
        ensure_effective_joined!(ecto_query, assoc_data)

        binding_expr = Ecto.Query.dynamic([{^assoc_data.assoc_binding, x}], x)

        case nested_preloads do
          [] -> {assoc_data.assoc_field, binding_expr}
          nested -> {assoc_data.assoc_field, {binding_expr, nested}}
        end

      %PreloadSpec{strategy: :through_join, query_opts: opts} ->
        raise ArgumentError,
              "internal error: invalid preload spec for #{inspect(assoc_data.source_schema)}.#{inspect(assoc_data.assoc_field)}; " <>
                "scoped preload opts require separate preload, got: #{inspect(opts)}"
    end
  end

  defp nested_preloaded_assoc_fields(nodes_map) when is_map(nodes_map) do
    nodes_map
    |> Enum.sort_by(fn {assoc_field, _} -> assoc_field end)
    |> Enum.flat_map(fn {_assoc_field, assoc_data} ->
      case assoc_data.preload_spec do
        %PreloadSpec{} ->
          [assoc_data.assoc_field | nested_preloaded_assoc_fields(assoc_data.nested_assocs)]

        _ ->
          nested_preloaded_assoc_fields(assoc_data.nested_assocs)
      end
    end)
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

  defp effective_joined?(ecto_query, %{assoc_binding: assoc_binding} = assoc_data) do
    if Ecto.Query.has_named_binding?(ecto_query, assoc_binding) do
      QueryBuilder.JoinMaker.validate_existing_assoc_join!(ecto_query, assoc_data, :any)
      true
    else
      false
    end
  end

  defp effective_joined?(_ecto_query, _assoc_data), do: false

  defp build_scoped_preload_query!(
         %{assoc_schema: assoc_schema},
         opts
       ) do
    query = assoc_schema._query()
    assoc_list = AssocList.new(assoc_schema)

    query =
      case Keyword.get(opts, :where, []) do
        [] -> query
        filters -> Where.where(query, assoc_list, filters, [])
      end

    case Keyword.get(opts, :order_by, []) do
      [] -> query
      order_by -> OrderBy.order_by(query, assoc_list, order_by)
    end
  end
end
