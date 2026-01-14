defmodule QueryBuilder.Utils do
  @moduledoc false

  @token_cache_key {__MODULE__, :token_cache}

  def with_token_cache(fun) when is_function(fun, 0) do
    case Process.get(@token_cache_key) do
      cache when is_map(cache) ->
        fun.()

      _other ->
        Process.put(@token_cache_key, %{})

        try do
          fun.()
        after
          Process.delete(@token_cache_key)
        end
    end
  end

  def root_schema(%{__struct__: QueryBuilder.Query, ecto_query: ecto_query}) do
    root_schema(ecto_query)
  end

  def root_schema(%Ecto.Query{} = query) do
    do_root_schema(query)
  end

  def root_schema(query) when is_atom(query) do
    if Code.ensure_loaded?(query) and function_exported?(query, :__schema__, 1) do
      query
      |> Ecto.Queryable.to_query()
      |> do_root_schema()
    else
      raise ArgumentError,
            "expected an Ecto.Queryable (schema module, Ecto.Query, or QueryBuilder.Query), got: #{inspect(query)}"
    end
  end

  def root_schema(query) do
    if Ecto.Queryable.impl_for(query) do
      query
      |> Ecto.Queryable.to_query()
      |> do_root_schema()
    else
      raise ArgumentError,
            "expected an Ecto.Queryable (schema module, Ecto.Query, or QueryBuilder.Query), got: #{inspect(query)}"
    end
  end

  defp do_root_schema(query) do
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

  def find_field_and_binding_from_token(%QueryBuilder.AssocList{} = assoc_list, field)
      when is_atom(field) do
    resolve_field_and_binding_from_token_cached!(assoc_list, field)
  end

  def find_field_and_binding_from_token(%QueryBuilder.AssocList{} = assoc_list, token)
      when is_binary(token) do
    resolve_field_and_binding_from_token_cached!(assoc_list, token)
  end

  def find_field_and_binding_from_token(other_assoc_list, _field) do
    raise ArgumentError,
          "QueryBuilder token resolution expects an association tree (%QueryBuilder.AssocList{}), " <>
            "got: #{inspect(other_assoc_list)}"
  end

  def normalize_or_groups!(or_groups, opt_key, context)
      when is_atom(opt_key) and is_binary(context) do
    cond do
      is_nil(or_groups) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` to be a list of filter groups; got nil"

      Keyword.keyword?(or_groups) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` to be a list of filter groups like `[[...], [...]]`; " <>
                "got a keyword list. Wrap it in a list if you intended a single group."

      not is_list(or_groups) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` to be a list of filter groups like `[[...], [...]]`; got: #{inspect(or_groups)}"

      Enum.any?(or_groups, &(not is_list(&1))) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` groups to be lists (e.g. `[[title: \"A\"], [title: \"B\"]]`); got: #{inspect(or_groups)}"

      true ->
        or_groups
    end
  end

  defp resolve_field_and_binding_from_token_cached!(assoc_list, token)
       when is_atom(token) or is_binary(token) do
    case Process.get(@token_cache_key) do
      cache when is_map(cache) ->
        cache_key = {assoc_list.id, assoc_list.revision, token}

        resolved =
          case Map.fetch(cache, cache_key) do
            {:ok, {field, binding}} ->
              {field, binding}

            :error ->
              resolved = resolve_field_and_binding_from_token!(assoc_list, token)
              Process.put(@token_cache_key, Map.put(cache, cache_key, resolved))
              resolved
          end

        resolved

      _other ->
        resolve_field_and_binding_from_token!(assoc_list, token)
    end
  end

  defp resolve_field_and_binding_from_token!(assoc_list, token) when is_atom(token) do
    token_string = Atom.to_string(token)

    if String.contains?(token_string, "@") do
      resolve_field_and_binding_from_token!(assoc_list, token_string)
    else
      {token, assoc_list.root_schema}
    end
  end

  defp resolve_field_and_binding_from_token!(assoc_list, token) when is_binary(token) do
    if not String.contains?(token, "@") do
      field =
        try do
          String.to_existing_atom(token)
        rescue
          ArgumentError ->
            raise ArgumentError, "unknown field #{inspect(token)} in token #{inspect(token)}"
        end

      {field, assoc_list.root_schema}
    else
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

      do_find_field_and_binding_from_token(assoc_list, field, assoc_path, token)
    end
  end

  defp do_find_field_and_binding_from_token(
         %QueryBuilder.AssocList{root_schema: root_schema},
         field,
         [],
         _token
       ) do
    {field, root_schema}
  end

  defp do_find_field_and_binding_from_token(assoc_list, field, [assoc_field], _token) do
    case QueryBuilder.AssocList.binding_from_assoc_name(assoc_list, assoc_field) do
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

  defp do_find_field_and_binding_from_token(assoc_list, field, assoc_path, token)
       when is_list(assoc_path) do
    case QueryBuilder.AssocList.binding_from_assoc_path(assoc_list, assoc_path) do
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
