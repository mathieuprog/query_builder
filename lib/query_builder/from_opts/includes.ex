defmodule QueryBuilder.FromOpts.Includes do
  @moduledoc false

  def normalize_from_opts_includes_allowlist!(nil) do
    raise ArgumentError, "from_opts/3 expects `includes:` to be a keyword list or a map, got nil"
  end

  def normalize_from_opts_includes_allowlist!(allowlist)
      when allowlist == %{} or allowlist == [] do
    %{}
  end

  def normalize_from_opts_includes_allowlist!(allowlist) when is_list(allowlist) do
    unless Keyword.keyword?(allowlist) do
      raise ArgumentError,
            "from_opts/3 expects `includes:` to be a keyword list or a map, got: #{inspect(allowlist)}"
    end

    keys = Keyword.keys(allowlist)

    if Enum.uniq(keys) != keys do
      raise ArgumentError,
            "from_opts/3 expects `includes:` keys to be unique, got: #{inspect(keys)}"
    end

    allowlist
    |> Enum.reduce(%{}, fn {include_key, include_spec}, acc ->
      unless is_atom(include_key) do
        raise ArgumentError,
              "from_opts/3 expects `includes:` keys to be atoms, got: #{inspect(include_key)}"
      end

      Map.put(acc, include_key, normalize_from_opts_include_spec!(include_key, include_spec))
    end)
  end

  def normalize_from_opts_includes_allowlist!(allowlist) when is_map(allowlist) do
    allowlist
    |> Enum.reduce(%{}, fn {include_key, include_spec}, acc ->
      unless is_atom(include_key) do
        raise ArgumentError,
              "from_opts/3 expects `includes:` keys to be atoms, got: #{inspect(include_key)}"
      end

      Map.put(acc, include_key, normalize_from_opts_include_spec!(include_key, include_spec))
    end)
  end

  def normalize_from_opts_includes_allowlist!(other) do
    raise ArgumentError,
          "from_opts/3 expects `includes:` to be a keyword list or a map, got: #{inspect(other)}"
  end

  def extract_requested_includes_from_opts!(opts) do
    cond do
      is_nil(opts) ->
        {[], nil}

      opts == [] ->
        {[], []}

      not is_list(opts) ->
        raise ArgumentError,
              "from_opts/2 expects opts to be a keyword list like `[where: ...]`, got: #{inspect(opts)}"

      true ->
        {includes_rev, rest_rev} =
          Enum.reduce(opts, {[], []}, fn
            {:include, value}, {includes_rev, rest_rev} ->
              include_keys = normalize_requested_include_keys!(value)

              includes_rev = Enum.reverse(include_keys, includes_rev)

              {includes_rev, rest_rev}

            {_, _} = entry, {includes_rev, rest_rev} ->
              {includes_rev, [entry | rest_rev]}

            invalid, _acc ->
              raise ArgumentError,
                    "from_opts/2 expects opts to be a keyword list (list of `{operation, value}` pairs); " <>
                      "got invalid entry: #{inspect(invalid)} in #{inspect(opts)}"
          end)

        {Enum.reverse(includes_rev), Enum.reverse(rest_rev)}
    end
  end

  def apply_includes_allowlist!(query, [], _allowlist), do: query

  def apply_includes_allowlist!(query, requested_includes, allowlist) when is_map(allowlist) do
    allowed_keys = Map.keys(allowlist)

    string_to_atom =
      if Enum.any?(requested_includes, &is_binary/1) do
        Map.new(allowed_keys, fn key -> {Atom.to_string(key), key} end)
      else
        %{}
      end

    {_, query} =
      Enum.reduce(requested_includes, {MapSet.new(), query}, fn include_key, {seen, acc} ->
        include_key = normalize_requested_include_key!(include_key, allowlist, string_to_atom)

        if MapSet.member?(seen, include_key) do
          {seen, acc}
        else
          include_spec = Map.fetch!(allowlist, include_key)

          acc =
            case include_spec do
              {:preload_separate, assoc_fields} ->
                QueryBuilder.preload_separate(acc, assoc_fields)

              {:preload_separate_scoped, assoc_field, opts} ->
                QueryBuilder.preload_separate_scoped(acc, assoc_field, opts)

              {:preload_through_join, assoc_fields} ->
                QueryBuilder.preload_through_join(acc, assoc_fields)
            end

          {MapSet.put(seen, include_key), acc}
        end
      end)

    query
  end

  defp normalize_from_opts_include_spec!(_include_key, include_spec)
       when is_function(include_spec) do
    raise ArgumentError,
          "from_opts/3 does not accept function include handlers in `includes:`; " <>
            "use a declarative preload spec (e.g. `:role`, `{:preload_separate, ...}`, `{:preload_separate_scoped, ...}`, `{:preload_through_join, ...}`)"
  end

  defp normalize_from_opts_include_spec!(_include_key, include_spec)
       when is_atom(include_spec) or is_list(include_spec) do
    {:preload_separate, include_spec}
  end

  defp normalize_from_opts_include_spec!(_include_key, {:preload_separate, assoc_fields}) do
    {:preload_separate, assoc_fields}
  end

  defp normalize_from_opts_include_spec!(
         _include_key,
         {:preload_separate_scoped, assoc_field, opts}
       ) do
    {:preload_separate_scoped, assoc_field, opts}
  end

  defp normalize_from_opts_include_spec!(_include_key, {:preload_through_join, assoc_fields}) do
    {:preload_through_join, assoc_fields}
  end

  defp normalize_from_opts_include_spec!(include_key, other) do
    raise ArgumentError,
          "from_opts/3 got an invalid include spec for #{inspect(include_key)} in `includes:`: #{inspect(other)}. " <>
            "Expected an assoc tree (atom/keyword list), `{:preload_separate, assoc_fields}`, " <>
            "`{:preload_separate_scoped, assoc_field, opts}`, or `{:preload_through_join, assoc_fields}`."
  end

  defp normalize_requested_include_keys!(nil) do
    raise ArgumentError,
          "from_opts/2 does not accept nil for :include; omit the key or pass `include: []`"
  end

  defp normalize_requested_include_keys!([]), do: []

  defp normalize_requested_include_keys!(key) when is_atom(key) or is_binary(key), do: [key]

  defp normalize_requested_include_keys!(keys) when is_list(keys) do
    Enum.each(keys, fn
      key when is_atom(key) or is_binary(key) ->
        :ok

      other ->
        raise ArgumentError,
              "from_opts/2 expects `include:` to be a list of atoms/strings, got: #{inspect(other)} in #{inspect(keys)}"
    end)

    keys
  end

  defp normalize_requested_include_keys!(other) do
    raise ArgumentError,
          "from_opts/2 expects `include:` to be a list of include keys (atoms/strings), got: #{inspect(other)}"
  end

  defp normalize_requested_include_key!(include_key, allowlist, string_to_atom) do
    {normalized_key, raw_key_for_error} =
      cond do
        is_atom(include_key) ->
          {include_key, include_key}

        is_binary(include_key) ->
          {Map.get(string_to_atom, include_key), include_key}

        true ->
          raise ArgumentError,
                "from_opts/2 expects include keys to be atoms/strings, got: #{inspect(include_key)}"
      end

    if is_nil(normalized_key) or not Map.has_key?(allowlist, normalized_key) do
      allowed_includes_string =
        allowlist
        |> Map.keys()
        |> Enum.sort()
        |> Enum.map_join(", ", &inspect/1)

      raise ArgumentError,
            "from_opts/2 got unknown include key #{inspect(raw_key_for_error)}; allowed includes: #{allowed_includes_string}"
    end

    normalized_key
  end
end
