defmodule QueryBuilder.PreloadOps do
  @moduledoc false

  def preload_separate(%QueryBuilder.Query{} = query, assoc_fields) do
    %{
      query
      | operations: [
          {:preload, assoc_fields, [QueryBuilder.AssocList.PreloadSpec.new(:separate)]}
          | query.operations
        ]
    }
  end

  def preload_separate_scoped(%QueryBuilder.Query{} = query, assoc_field, opts)
      when is_atom(assoc_field) do
    opts = normalize_preload_separate_scoped_opts!(opts, assoc_field)

    %{
      query
      | operations: [
          {:preload, assoc_field, [QueryBuilder.AssocList.PreloadSpec.new(:separate, opts)]}
          | query.operations
        ]
    }
  end

  def preload_through_join(%QueryBuilder.Query{} = query, assoc_fields) do
    %{
      query
      | operations: [
          {:preload, assoc_fields, [QueryBuilder.AssocList.PreloadSpec.new(:through_join)]}
          | query.operations
        ]
    }
  end

  defp normalize_preload_separate_scoped_opts!(opts, assoc_field) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "preload_separate_scoped/3 expects opts to be a keyword list, got: #{inspect(opts)}"
    end

    allowed_keys = [:where, :order_by]

    unknown =
      opts
      |> Keyword.keys()
      |> Enum.uniq()
      |> Enum.reject(&(&1 in allowed_keys))

    if unknown != [] do
      raise ArgumentError,
            "preload_separate_scoped/3 got unknown options #{inspect(unknown)} for " <>
              "#{inspect(assoc_field)} (supported: :where, :order_by)"
    end

    where_filters = Keyword.get(opts, :where, [])
    order_by = Keyword.get(opts, :order_by, [])

    validate_scoped_preload_where_filters!(assoc_field, where_filters)
    validate_scoped_preload_order_by!(assoc_field, order_by)

    if where_filters == [] and order_by == [] do
      nil
    else
      [where: where_filters, order_by: order_by]
    end
  end

  defp validate_scoped_preload_where_filters!(assoc_field, nil) do
    raise ArgumentError,
          "preload_separate_scoped/3 expects `where:` to be a keyword list (or list of filters) for " <>
            "#{inspect(assoc_field)}, got nil"
  end

  defp validate_scoped_preload_where_filters!(assoc_field, filters) do
    filters = List.wrap(filters)

    Enum.each(filters, fn
      fun when is_function(fun) ->
        raise ArgumentError,
              "preload_separate_scoped/3 does not accept custom filter functions in `where:` for " <>
                "#{inspect(assoc_field)}; use an explicit Ecto preload query instead"

      %QueryBuilder.Aggregate{} = aggregate ->
        raise ArgumentError,
              "preload_separate_scoped/3 does not accept aggregate filters in `where:` for " <>
                "#{inspect(assoc_field)}; got: #{inspect(aggregate)}"

      {%QueryBuilder.Aggregate{} = aggregate, _value} ->
        raise ArgumentError,
              "preload_separate_scoped/3 does not accept aggregate filters in `where:` for " <>
                "#{inspect(assoc_field)}; got: #{inspect(aggregate)}"

      {field, value} ->
        validate_scoped_preload_field_token!(assoc_field, field)
        validate_scoped_preload_value_token!(assoc_field, value)

      {field, _operator, value} ->
        validate_scoped_preload_field_token!(assoc_field, field)
        validate_scoped_preload_value_token!(assoc_field, value)

      {field, _operator, value, _operator_opts} ->
        validate_scoped_preload_field_token!(assoc_field, field)
        validate_scoped_preload_value_token!(assoc_field, value)

      other ->
        raise ArgumentError,
              "preload_separate_scoped/3 got an invalid `where:` entry for #{inspect(assoc_field)}: " <>
                "#{inspect(other)}"
    end)
  end

  defp validate_scoped_preload_order_by!(assoc_field, nil) do
    raise ArgumentError,
          "preload_separate_scoped/3 expects `order_by:` to be a keyword list for " <>
            "#{inspect(assoc_field)}, got nil"
  end

  defp validate_scoped_preload_order_by!(assoc_field, order_by) do
    unless Keyword.keyword?(order_by) do
      raise ArgumentError,
            "preload_separate_scoped/3 expects `order_by:` to be a keyword list for " <>
              "#{inspect(assoc_field)}, got: #{inspect(order_by)}"
    end

    Enum.each(order_by, fn
      {direction, field} when is_atom(direction) and is_atom(field) ->
        validate_scoped_preload_field_token!(assoc_field, field)

      {direction, other} when is_atom(direction) ->
        raise ArgumentError,
              "preload_separate_scoped/3 expects `order_by:` fields to be tokens (atoms) for " <>
                "#{inspect(assoc_field)}, got: #{inspect(other)}"

      other ->
        raise ArgumentError,
              "preload_separate_scoped/3 expects `order_by:` entries to be `{direction, token}` for " <>
                "#{inspect(assoc_field)}, got: #{inspect(other)}"
    end)
  end

  defp validate_scoped_preload_field_token!(assoc_field, field) when is_atom(field) do
    token = Atom.to_string(field)

    if String.contains?(token, "@") do
      raise ArgumentError,
            "preload_separate_scoped/3 does not allow assoc tokens (containing `@`) for " <>
              "#{inspect(assoc_field)}; got: #{inspect(field)}"
    end

    :ok
  end

  defp validate_scoped_preload_field_token!(assoc_field, other) do
    raise ArgumentError,
          "preload_separate_scoped/3 expects field tokens to be atoms for " <>
            "#{inspect(assoc_field)}, got: #{inspect(other)}"
  end

  defp validate_scoped_preload_value_token!(_assoc_field, value) when not is_atom(value), do: :ok

  defp validate_scoped_preload_value_token!(assoc_field, value) when is_atom(value) do
    marker = "@self"
    value_str = Atom.to_string(value)

    if String.ends_with?(value_str, marker) do
      referenced = binary_part(value_str, 0, byte_size(value_str) - byte_size(marker))

      if String.contains?(referenced, "@") do
        raise ArgumentError,
              "preload_separate_scoped/3 does not allow assoc tokens (containing `@`) in field-to-field " <>
                "filters for #{inspect(assoc_field)}; got: #{inspect(value)}"
      end
    end

    :ok
  end
end
