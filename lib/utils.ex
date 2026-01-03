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

    parts = String.split(token, "@")

    if Enum.any?(parts, &(&1 == "")) do
      raise ArgumentError,
            "invalid token #{inspect(token)}; expected `field` or `field@assoc` or `field@assoc@nested_assoc...`"
    end

    [field_part | assoc_parts] = parts

    field =
      try do
        String.to_existing_atom(field_part)
      rescue
        ArgumentError ->
          raise ArgumentError, "unknown field #{inspect(field_part)} in token #{inspect(token)}"
      end

    assoc_path =
      Enum.map(assoc_parts, fn assoc_part ->
        try do
          String.to_existing_atom(assoc_part)
        rescue
          ArgumentError ->
            raise ArgumentError,
                  "unknown association #{inspect(assoc_part)} in token #{inspect(token)}"
        end
      end)

    do_find_field_and_binding_from_token(query, assoc_list, field, assoc_path, token)
  end

  defp do_find_field_and_binding_from_token(query, _assoc_list, field, [], _token) do
    {field, QueryBuilder.Utils.root_schema(query)}
  end

  defp do_find_field_and_binding_from_token(_query, assoc_list, field, [assoc_field], _token) do
    case find_binding_from_assoc_name(assoc_list, assoc_field) do
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
          |> Enum.map(fn %{path: path} -> Enum.map_join(path, "@", &to_string/1) end)
          |> Enum.uniq()
          |> Enum.sort()
          |> Enum.join(", ")

        example_token =
          case matches do
            [%{path: path} | _] ->
              "#{field}@#{Enum.map_join(path, "@", &to_string/1)}"

            _ ->
              "#{field}@#{assoc_field}"
          end

        raise ArgumentError,
              "ambiguous association token @#{assoc_field} in #{inspect(field)}@#{assoc_field}; " <>
                "it matches multiple association paths: #{paths}. " <>
                "Use a full-path token like #{example_token} to disambiguate, " <>
                "or rename one of the associations (e.g. :comment_user vs :like_user)."
    end
  end

  defp do_find_field_and_binding_from_token(_query, assoc_list, field, assoc_path, token)
       when is_list(assoc_path) do
    case find_binding_from_assoc_path(assoc_list, assoc_path) do
      {:ok, binding} ->
        {field, binding}

      {:error, :not_found} ->
        example_assoc_fields = assoc_path_to_nested_keyword_list(assoc_path)

        raise ArgumentError,
              "unknown association path token @#{Enum.map_join(assoc_path, "@", &to_string/1)} in #{inspect(token)}; " <>
                "include it in the assoc_fields argument (e.g. where(query, #{inspect(example_assoc_fields)}, ...)) " <>
                "or join/preload it before filtering"
    end
  end

  defp find_binding_from_assoc_name(assoc_list, assoc_field) do
    matches = find_binding_matches(assoc_list, assoc_field, [])

    case matches do
      [] -> {:error, :not_found}
      [%{binding: binding}] -> {:ok, binding}
      matches -> {:error, {:ambiguous, matches}}
    end
  end

  defp find_binding_from_assoc_path(assoc_list, assoc_path) when is_list(assoc_path) do
    case assoc_path do
      [] ->
        {:error, :not_found}

      [assoc_field | rest] ->
        case Enum.find(assoc_list, &(&1.assoc_field == assoc_field)) do
          nil ->
            {:error, :not_found}

          assoc_data ->
            if rest == [] do
              {:ok, assoc_data.assoc_binding}
            else
              find_binding_from_assoc_path(assoc_data.nested_assocs, rest)
            end
        end
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

  defp assoc_path_to_nested_keyword_list(path) when is_list(path) do
    case path do
      [] ->
        []

      [e] ->
        [e]

      [head | tail] ->
        [{head, assoc_path_to_nested_keyword_list(tail) |> unwrap_single_assoc()}]
    end
  end

  defp unwrap_single_assoc([e]) when is_atom(e), do: e
  defp unwrap_single_assoc(other), do: other
end
