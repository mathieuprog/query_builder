defmodule QueryBuilder.AssocList.Builder do
  @moduledoc false

  alias QueryBuilder.AssocList
  alias QueryBuilder.AssocList.JoinSpec
  alias QueryBuilder.AssocList.Node
  alias QueryBuilder.AssocList.PreloadSpec

  defmodule State do
    @moduledoc false

    # AssocList is built recursively and needs to carry a handful of values between
    # recursive calls. This struct keeps the recursive function signatures readable.

    defstruct source_binding: nil,
              source_schema: nil,
              lock_left?: false
  end

  def build(source_schema, assoc_list, assoc_fields, opts \\ [])

  def build(source_schema, %AssocList{} = assoc_list, assoc_fields, opts)
      when is_atom(source_schema) do
    if assoc_list.root_schema != source_schema do
      raise ArgumentError,
            "AssocList.build/4 expected root schema #{inspect(assoc_list.root_schema)}, " <>
              "got: #{inspect(source_schema)}"
    end

    state = %State{
      # the name of the binding of the query's root schema is the schema itself
      source_binding: source_schema,
      source_schema: source_schema
    }

    {roots, assoc_list} =
      do_build_nodes(assoc_list, assoc_list.roots, List.wrap(assoc_fields), state, opts, [])

    %{assoc_list | roots: roots}
  end

  def build(source_schema, other_assoc_list, _assoc_fields, _opts) do
    raise ArgumentError,
          "AssocList.build/4 expects the assoc_list argument to be a %QueryBuilder.AssocList{}, " <>
            "got: #{inspect(other_assoc_list)} for root schema #{inspect(source_schema)}"
  end

  defp do_build_nodes(%AssocList{} = assoc_list, nodes_map, [], _state, _opts, _prefix_rev)
       when is_map(nodes_map) do
    {nodes_map, assoc_list}
  end

  defp do_build_nodes(
         %AssocList{} = assoc_list,
         nodes_map,
         [{assoc_field, nested_assoc_fields} | tail],
         state,
         opts,
         prefix_rev
       ) do
    %{
      source_binding: source_binding,
      source_schema: source_schema
    } = state

    {assoc_field, join_type_marker} = normalize_assoc_field_and_join_type!(assoc_field)

    {join_required?, join_qualifier, join_filter_group} =
      case Keyword.get(opts, :join, :any) do
        :none ->
          {false, :any, []}

        :left ->
          left_join_mode = Keyword.get(opts, :left_join_mode, :leaf)

          if nested_assoc_fields == [] do
            join_filter_group = Keyword.get(opts, :join_filters, [])
            {true, :left, List.wrap(join_filter_group)}
          else
            case left_join_mode do
              :path ->
                {true, :left, []}

              :leaf ->
                {true, :inner, []}

              other ->
                raise ArgumentError,
                      "invalid left_join_mode #{inspect(other)}; expected :leaf or :path"
            end
          end

        join_qualifier when join_qualifier in [:inner, :left, :any] ->
          {true, join_qualifier, []}

        other ->
          raise ArgumentError,
                "invalid join option #{inspect(other)}; expected :none, :left, :inner, or :any"
      end

    join_type_marker =
      if join_required? do
        if state.lock_left? do
          if join_type_marker == :inner do
            raise ArgumentError,
                  "invalid assoc_fields: cannot require an inner join (`!`) under an optional association path (`?`) " <>
                    "for #{inspect(source_schema)}.#{inspect(assoc_field)}"
          end

          :left
        else
          join_type_marker
        end
      else
        # Join qualifiers are irrelevant when the operation does not request joins (e.g. preload),
        # but we still accept `:assoc?` / `:assoc!` to strip markers and keep assoc_fields uniform.
        :any
      end

    join_qualifier =
      JoinSpec.merge_qualifiers!(join_qualifier, join_type_marker, source_schema, assoc_field)

    join_filters =
      case join_filter_group do
        [] ->
          []

        [filters, or_filters] when is_list(filters) and is_list(or_filters) ->
          [{filters, or_filters}]

        other ->
          raise ArgumentError,
                "invalid join filters for #{inspect(source_schema)}.#{inspect(assoc_field)}: " <>
                  "expected `[filters, or_filters]` (as set by left_join*/*), got: #{inspect(other)}"
      end

    join_spec =
      JoinSpec.new(
        join_required?,
        join_qualifier,
        join_filters
      )

    preload_spec =
      case Keyword.get(opts, :preload_spec, nil) do
        nil -> nil
        %PreloadSpec{} = preload_spec -> preload_spec
        other -> raise ArgumentError, "invalid preload spec: #{inspect(other)}"
      end

    {nodes_map, assoc_list, assoc_data} =
      upsert_current_assoc_data(
        assoc_list,
        nodes_map,
        prefix_rev,
        assoc_data(source_binding, source_schema, assoc_field, join_spec, preload_spec)
      )

    {nested_assocs, assoc_list} =
      do_build_nodes(
        assoc_list,
        assoc_data.nested_assocs,
        List.wrap(nested_assoc_fields),
        %{
          state
          | source_binding: assoc_data.assoc_binding,
            source_schema: assoc_data.assoc_schema,
            lock_left?: state.lock_left? || join_qualifier == :left
        },
        opts,
        [assoc_field | prefix_rev]
      )

    assoc_data = %{assoc_data | nested_assocs: nested_assocs}
    nodes_map = Map.put(nodes_map, assoc_field, assoc_data)

    do_build_nodes(assoc_list, nodes_map, tail, state, opts, prefix_rev)
  end

  defp do_build_nodes(
         %AssocList{} = assoc_list,
         nodes_map,
         [assoc_field | tail],
         state,
         opts,
         prefix_rev
       ) do
    do_build_nodes(assoc_list, nodes_map, [{assoc_field, []} | tail], state, opts, prefix_rev)
  end

  defp upsert_current_assoc_data(
         %AssocList{} = assoc_list,
         nodes_map,
         prefix_rev,
         %Node{} = new_node
       )
       when is_map(nodes_map) and is_list(prefix_rev) do
    {nodes_map, {node, inserted?}} =
      case Map.get(nodes_map, new_node.assoc_field) do
        nil ->
          {Map.put(nodes_map, new_node.assoc_field, new_node), {new_node, true}}

        %Node{} = existing ->
          if existing.assoc_binding != new_node.assoc_binding or
               existing.source_binding != new_node.source_binding or
               existing.source_schema != new_node.source_schema or
               existing.assoc_schema != new_node.assoc_schema do
            raise ArgumentError,
                  "association tree conflict for #{inspect(existing.source_schema)}.#{inspect(existing.assoc_field)}; " <>
                    "this is likely a QueryBuilder bug. Please report it."
          end

          join_spec =
            JoinSpec.merge!(
              existing.join_spec,
              new_node.join_spec,
              existing.source_schema,
              existing.assoc_field
            )

          preload_spec =
            PreloadSpec.merge!(
              existing.preload_spec,
              new_node.preload_spec,
              existing.source_schema,
              existing.assoc_field
            )

          merged = %{existing | join_spec: join_spec, preload_spec: preload_spec}

          {Map.put(nodes_map, merged.assoc_field, merged), {merged, false}}
      end

    assoc_list =
      if inserted? do
        path = Enum.reverse([new_node.assoc_field | prefix_rev])

        assoc_list
        |> then(&%{&1 | revision: &1.revision + 1})
        |> register_path!(path, node.assoc_binding)
        |> register_name!(node.assoc_field, node.assoc_binding, path)
        |> register_binding!(node, path)
      else
        assoc_list
      end

    {nodes_map, assoc_list, node}
  end

  defp register_path!(%AssocList{} = assoc_list, path, binding) do
    case Map.get(assoc_list.by_path, path) do
      nil ->
        %{assoc_list | by_path: Map.put(assoc_list.by_path, path, binding)}

      ^binding ->
        assoc_list

      other ->
        raise ArgumentError,
              "association path collision for #{inspect(path)}; it resolves to both " <>
                "#{inspect(binding)} and #{inspect(other)}. This is likely a QueryBuilder bug; please report it."
    end
  end

  defp register_name!(%AssocList{} = assoc_list, assoc_field, binding, path) do
    match = %{binding: binding, path: path}
    matches = Map.get(assoc_list.by_name, assoc_field, [])
    %{assoc_list | by_name: Map.put(assoc_list.by_name, assoc_field, [match | matches])}
  end

  defp register_binding!(%AssocList{} = assoc_list, %Node{} = node, path) do
    descriptor = %{
      source_binding: node.source_binding,
      source_schema: node.source_schema,
      assoc_field: node.assoc_field,
      path: path
    }

    case Map.get(assoc_list.by_binding, node.assoc_binding) do
      nil ->
        %{assoc_list | by_binding: Map.put(assoc_list.by_binding, node.assoc_binding, descriptor)}

      %{source_binding: same_source, source_schema: same_schema, assoc_field: same_field} =
          existing
      when same_source == node.source_binding and same_schema == node.source_schema and
             same_field == node.assoc_field ->
        if existing.path != path do
          raise ArgumentError,
                "association binding collision for #{inspect(node.assoc_binding)}; " <>
                  "it is used for multiple association paths (#{inspect(existing.path)} and #{inspect(path)}). " <>
                  "QueryBuilder's binding naming scheme cannot represent the same association join multiple times " <>
                  "under different parent joins. Use explicit Ecto joins with distinct `as:` bindings for this case."
        end

        assoc_list

      _other ->
        raise ArgumentError,
              "association binding collision for #{inspect(node.assoc_binding)}; " <>
                "QueryBuilder attempted to use it for #{inspect(node.source_schema)}.#{inspect(node.assoc_field)} " <>
                "at path #{inspect(path)}, but it is already used elsewhere in the association tree. " <>
                "QueryBuilder cannot safely represent multiple distinct joins under the same binding. " <>
                "Use explicit Ecto joins with distinct `as:` bindings for this case."
    end
  end

  defp normalize_assoc_field_and_join_type!(assoc_field) when is_atom(assoc_field) do
    assoc_field_string = Atom.to_string(assoc_field)

    cond do
      String.ends_with?(assoc_field_string, "?") ->
        base = String.trim_trailing(assoc_field_string, "?")

        if base == "" do
          raise ArgumentError,
                "invalid assoc field #{inspect(assoc_field)} (cannot be only a marker)"
        end

        assoc_atom =
          try do
            String.to_existing_atom(base)
          rescue
            ArgumentError ->
              raise ArgumentError,
                    "invalid assoc field #{inspect(assoc_field)}; expected an existing association atom (like :role?), got unknown association #{inspect(base)}"
          end

        {assoc_atom, :left}

      String.ends_with?(assoc_field_string, "!") ->
        base = String.trim_trailing(assoc_field_string, "!")

        if base == "" do
          raise ArgumentError,
                "invalid assoc field #{inspect(assoc_field)} (cannot be only a marker)"
        end

        assoc_atom =
          try do
            String.to_existing_atom(base)
          rescue
            ArgumentError ->
              raise ArgumentError,
                    "invalid assoc field #{inspect(assoc_field)}; expected an existing association atom (like :role!), got unknown association #{inspect(base)}"
          end

        {assoc_atom, :inner}

      true ->
        {assoc_field, :any}
    end
  end

  defp normalize_assoc_field_and_join_type!(assoc_field) do
    raise ArgumentError,
          "invalid assoc field #{inspect(assoc_field)}; expected an association atom (optionally suffixed with ? or !)"
  end

  defp assoc_data(
         source_binding,
         source_schema,
         assoc_field,
         join_spec,
         preload_spec
       ) do
    assoc_schema = source_schema._assoc_schema(assoc_field)
    cardinality = source_schema._assoc_cardinality(assoc_field)
    assoc_binding = source_schema._binding(assoc_field)

    %Node{
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema,
      cardinality: cardinality,
      join_spec: join_spec,
      preload_spec: preload_spec,
      nested_assocs: %{},
      source_binding: source_binding,
      source_schema: source_schema
    }
  end
end
