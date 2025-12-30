defmodule QueryBuilder.Utils do
  @moduledoc false

  def root_schema(query) do
    query =
      try do
        Ecto.Queryable.to_query(query)
      rescue
        Protocol.UndefinedError ->
          raise ArgumentError,
                "expected an Ecto.Queryable (schema module, Ecto.Query, or QueryBuilder.Query), got: #{inspect(query)}"
      end

    case query do
      %{from: %{source: {_, context}}} when is_atom(context) and not is_nil(context) ->
        context

      %{from: %{source: {_, nil}}} ->
        raise ArgumentError,
              "expected a query with a schema source, got a query without schema: #{inspect(query)}"

      _ ->
        raise ArgumentError, "expected a query with a schema source, got: #{inspect(query)}"
    end
  end

  def find_field_and_binding_from_token(query, assoc_list, field) do
    token = to_string(field)

    {field, assoc_field} =
      case String.split(token, "@", parts: 3) do
        [field] ->
          {field, nil}

        [field, assoc_field] ->
          {field, assoc_field}

        _ ->
          raise ArgumentError,
                "invalid token #{inspect(token)}; expected `field` or `field@assoc` (at most one '@')"
      end

    field =
      try do
        String.to_existing_atom(field)
      rescue
        ArgumentError ->
          raise ArgumentError, "unknown field #{inspect(field)} in token #{inspect(token)}"
      end

    assoc_field =
      if is_nil(assoc_field) do
        nil
      else
        try do
          String.to_existing_atom(assoc_field)
        rescue
          ArgumentError ->
            raise ArgumentError,
                  "unknown association #{inspect(assoc_field)} in token #{inspect(token)}"
        end
      end

    do_find_field_and_binding_from_token(query, assoc_list, [field, assoc_field])
  end

  defp do_find_field_and_binding_from_token(query, _assoc_list, [field, nil]) do
    {field, QueryBuilder.Utils.root_schema(query)}
  end

  defp do_find_field_and_binding_from_token(_query, assoc_list, [field, assoc_field]) do
    case find_binding_from_token(assoc_list, assoc_field) do
      {:ok, binding} ->
        {field, binding}

      {:error, :not_found} ->
        raise ArgumentError,
              "unknown association token @#{assoc_field} in #{inspect(field)}@#{assoc_field}; " <>
                "include it in the assoc_fields argument (e.g. where(query, [:#{assoc_field}], ...)) " <>
                "or join/preload it before filtering"

      {:error, {:ambiguous, matches}} ->
        paths =
          matches
          |> Enum.map(fn %{path: path} -> Enum.map_join(path, ".", &to_string/1) end)
          |> Enum.uniq()
          |> Enum.sort()
          |> Enum.join(", ")

        raise ArgumentError,
              "ambiguous association token @#{assoc_field} in #{inspect(field)}@#{assoc_field}; " <>
                "it matches multiple association paths: #{paths}. " <>
                "QueryBuilder token resolution is by association name only; rename one of the associations " <>
                "(e.g. :comment_user vs :like_user) or avoid mixing multiple `:#{assoc_field}` associations in one query"
    end
  end

  defp find_binding_from_token(assoc_list, field) do
    matches = find_binding_matches(assoc_list, field, [])

    case matches do
      [] -> {:error, :not_found}
      [%{binding: binding}] -> {:ok, binding}
      matches -> {:error, {:ambiguous, matches}}
    end
  end

  defp find_binding_matches([], _field, _rev_path), do: []

  defp find_binding_matches([assoc_data | tail], field, rev_path) do
    rev_path_here = [assoc_data.assoc_field | rev_path]

    matches =
      if assoc_data.assoc_field == field do
        [%{binding: assoc_data.assoc_binding, path: Enum.reverse(rev_path_here)}]
      else
        []
      end

    matches ++
      find_binding_matches(assoc_data.nested_assocs, field, rev_path_here) ++
      find_binding_matches(tail, field, rev_path)
  end
end
