defmodule QueryBuilder.JoinMaker do
  @moduledoc false

  require Ecto.Query
  alias QueryBuilder.AssocList.JoinSpec

  @doc """
  Emits joins described by the assoc tree.

  Associations whose join spec has `required?: false` are not joined.
  """
  def make_joins(ecto_query, %QueryBuilder.AssocList{} = assoc_list) do
    {ecto_query, _joins_tuple} = do_make_joins(ecto_query, assoc_list.roots, assoc_list, nil)
    ecto_query
  end

  def make_joins(_ecto_query, other) do
    raise ArgumentError,
          "QueryBuilder.JoinMaker.make_joins/2 expects a %QueryBuilder.AssocList{}, got: #{inspect(other)}"
  end

  defp do_make_joins(ecto_query, nodes_map, _original_assoc_list, joins_tuple)
       when is_map(nodes_map) and map_size(nodes_map) == 0,
       do: {ecto_query, joins_tuple}

  defp do_make_joins(ecto_query, nodes_map, original_assoc_list, joins_tuple)
       when is_map(nodes_map) do
    Enum.reduce(nodes_map, {ecto_query, joins_tuple}, fn {_assoc_field, assoc_data},
                                                         {ecto_query, joins_tuple} ->
      case assoc_data do
        %{join_spec: %JoinSpec{required?: false}} ->
          {ecto_query, joins_tuple}

        _ ->
          {ecto_query, joins_tuple} =
            maybe_join(ecto_query, assoc_data, original_assoc_list, joins_tuple)

          do_make_joins(ecto_query, assoc_data.nested_assocs, original_assoc_list, joins_tuple)
      end
    end)
  end

  defp maybe_join(ecto_query, assoc_data, original_assoc_list, joins_tuple) do
    %{
      source_binding: source_binding,
      source_schema: source_schema,
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema,
      join_spec: %JoinSpec{qualifier: join_qualifier, filters: join_filters}
    } = assoc_data

    if Ecto.Query.has_named_binding?(ecto_query, assoc_binding) do
      if join_filters != [] do
        raise ArgumentError,
              "QueryBuilder attempted to join #{inspect(source_schema)}.#{inspect(assoc_field)} " <>
                "(assoc schema #{inspect(assoc_schema)}) using named binding #{inspect(assoc_binding)}, " <>
                "but the query already has a named binding with that name. " <>
                "This association also has join filters (from left_join*/*), " <>
                "and QueryBuilder cannot safely apply those filters to an already-existing join. " <>
                "Fix: remove the pre-joined binding, or join it under a different named binding."
      end

      joins_tuple =
        validate_existing_assoc_join_cached!(ecto_query, assoc_data, join_qualifier, joins_tuple)

      {ecto_query, joins_tuple}
    else
      on =
        case join_filters do
          [] ->
            true

          [{filters, or_filters}] ->
            QueryBuilder.Query.Where.build_dynamic_query(
              ecto_query,
              original_assoc_list,
              filters,
              or_filters
            )

          join_filters ->
            Enum.reduce(join_filters, nil, fn {filters, or_filters}, acc ->
              dynamic =
                QueryBuilder.Query.Where.build_dynamic_query(
                  ecto_query,
                  original_assoc_list,
                  filters,
                  or_filters
                )

              case acc do
                nil -> dynamic
                _ -> Ecto.Query.dynamic(^acc and ^dynamic)
              end
            end)
        end

      join_type =
        case join_qualifier do
          :any -> :left
          other -> other
        end

      ecto_query = source_schema._join(ecto_query, join_type, source_binding, assoc_field, on)
      {ecto_query, joins_tuple}
    end
  end

  def validate_existing_assoc_join!(ecto_query, assoc_data, expected_qualifier \\ :any)

  def validate_existing_assoc_join!(%Ecto.Query{} = ecto_query, assoc_data, expected_qualifier) do
    _joins_tuple =
      validate_existing_assoc_join_cached!(ecto_query, assoc_data, expected_qualifier, nil)

    :ok
  end

  defp validate_existing_assoc_join_cached!(
         %Ecto.Query{} = ecto_query,
         assoc_data,
         expected_qualifier,
         joins_tuple
       ) do
    %{
      source_binding: source_binding,
      source_schema: source_schema,
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema
    } = assoc_data

    join_index =
      case Map.fetch(ecto_query.aliases, assoc_binding) do
        {:ok, index} when is_integer(index) and index > 0 ->
          index

        {:ok, 0} ->
          raise ArgumentError,
                "QueryBuilder attempted to reuse existing named binding #{inspect(assoc_binding)} for " <>
                  "#{inspect(source_schema)}.#{inspect(assoc_field)}, but that binding refers to the root query. " <>
                  "This is likely a query construction bug; please report it."

        :error ->
          raise ArgumentError,
                "QueryBuilder attempted to reuse an existing named binding #{inspect(assoc_binding)} for " <>
                  "#{inspect(source_schema)}.#{inspect(assoc_field)}, but it was not found in the query aliases map. " <>
                  "This is likely a query construction bug; please report it."
      end

    {existing_join, joins_tuple} =
      fetch_existing_join_expr!(ecto_query, assoc_binding, join_index, joins_tuple, assoc_data)

    cond do
      expected_qualifier != :any and existing_join.qual != expected_qualifier ->
        raise ArgumentError,
              "QueryBuilder attempted to join #{inspect(source_schema)}.#{inspect(assoc_field)} " <>
                "(assoc schema #{inspect(assoc_schema)}) using named binding #{inspect(assoc_binding)}, " <>
                "but the query already has that binding joined as #{inspect(existing_join.qual)} while " <>
                "QueryBuilder requires #{inspect(expected_qualifier)}. " <>
                "QueryBuilder cannot change the join qualifier of an existing join. " <>
                "Fix: remove the pre-joined binding, or join it with the required qualifier under that binding."

      expected_qualifier == :any and existing_join.qual not in [:inner, :left] ->
        raise ArgumentError,
              "QueryBuilder attempted to reuse existing named binding #{inspect(assoc_binding)} for " <>
                "#{inspect(source_schema)}.#{inspect(assoc_field)}, but the existing join under that binding " <>
                "has qualifier #{inspect(existing_join.qual)}. QueryBuilder can only reuse :inner or :left " <>
                "association joins under named bindings."

      true ->
        :ok
    end

    expected_source_index = binding_index!(ecto_query, source_binding)
    expected_assoc = {expected_source_index, assoc_field}

    case existing_join.assoc do
      ^expected_assoc ->
        :ok

      nil ->
        raise ArgumentError,
              "QueryBuilder attempted to reuse existing named binding #{inspect(assoc_binding)} for " <>
                "#{inspect(source_schema)}.#{inspect(assoc_field)}, but the existing join under that binding " <>
                "is not an association join for that association. " <>
                "QueryBuilder can only reuse a named binding when it was created by joining the same association " <>
                "(e.g. `join: x in assoc(u, #{inspect(assoc_field)}), as: ^#{inspect(assoc_binding)}`). " <>
                "Fix: rename the existing binding, or join the correct association under that binding."

      {actual_source_index, actual_assoc_field} ->
        actual_source_binding = binding_name_for_index(ecto_query, actual_source_index)

        raise ArgumentError,
              "QueryBuilder attempted to reuse existing named binding #{inspect(assoc_binding)} for " <>
                "#{inspect(source_schema)}.#{inspect(assoc_field)}, but the existing join under that binding " <>
                "is an association join for #{inspect(actual_source_binding)}.#{inspect(actual_assoc_field)} " <>
                "instead. QueryBuilder cannot safely reuse a binding for a different association join. " <>
                "Fix: rename the existing binding, or join the correct association under that binding."

      other ->
        raise ArgumentError,
              "QueryBuilder attempted to reuse existing named binding #{inspect(assoc_binding)} for " <>
                "#{inspect(source_schema)}.#{inspect(assoc_field)}, but the existing join under that binding " <>
                "has an unexpected association descriptor: #{inspect(other)}. " <>
                "Fix: rename the existing binding, or join the correct association under that binding."
    end

    joins_tuple
  end

  defp fetch_existing_join_expr!(
         %Ecto.Query{} = ecto_query,
         assoc_binding,
         join_index,
         joins_tuple,
         assoc_data
       )
       when is_integer(join_index) and join_index > 0 do
    join_pos = join_index - 1

    joins_tuple =
      cond do
        is_tuple(joins_tuple) and tuple_size(joins_tuple) > join_pos ->
          joins_tuple

        true ->
          List.to_tuple(ecto_query.joins)
      end

    existing_join =
      if join_pos < tuple_size(joins_tuple) do
        elem(joins_tuple, join_pos)
      else
        nil
      end

    case existing_join do
      %Ecto.Query.JoinExpr{} = join ->
        {join, joins_tuple}

      other ->
        %{
          source_schema: source_schema,
          assoc_field: assoc_field
        } = assoc_data

        raise ArgumentError,
              "QueryBuilder attempted to reuse an existing named binding #{inspect(assoc_binding)} for " <>
                "#{inspect(source_schema)}.#{inspect(assoc_field)}, but could not find a corresponding join " <>
                "expression in the query. This is likely a query construction bug; please report it. " <>
                "aliases[#{inspect(assoc_binding)}]=#{inspect(join_index)} join_at_index=#{inspect(other)}"
    end
  end

  defp binding_index!(ecto_query, binding) do
    cond do
      ecto_query.from.as == binding ->
        0

      true ->
        case Map.fetch(ecto_query.aliases, binding) do
          {:ok, index} when is_integer(index) and index > 0 ->
            index

          {:ok, 0} ->
            # `binding` was not the root binding (handled above), so this implies an inconsistent aliases map.
            raise ArgumentError,
                  "QueryBuilder expected named binding #{inspect(binding)} to refer to a join, " <>
                    "but the query aliases map reports index 0. This is likely a query construction bug; please report it."

          :error ->
            raise ArgumentError,
                  "QueryBuilder expected the query to have named binding #{inspect(binding)}, " <>
                    "but it was not found. This is likely a query construction bug; please report it."
        end
    end
  end

  defp binding_name_for_index(%Ecto.Query{from: %{as: as}}, 0) when not is_nil(as), do: as
  defp binding_name_for_index(%Ecto.Query{}, 0), do: :root

  defp binding_name_for_index(%Ecto.Query{joins: joins}, index)
       when is_integer(index) and index > 0 do
    case Enum.at(joins, index - 1) do
      %Ecto.Query.JoinExpr{as: as} when not is_nil(as) -> as
      _ -> {:binding_index, index}
    end
  end
end
