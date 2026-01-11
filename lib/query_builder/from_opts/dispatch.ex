defmodule QueryBuilder.FromOpts.Dispatch do
  @moduledoc false

  alias QueryBuilder.FromOpts.Dispatch.Boundary
  alias QueryBuilder.FromOpts.Dispatch.Validation

  def apply(query, opts, apply_module, mode, extension_config) do
    do_from_opts(query, opts, apply_module, mode, extension_config)
  end

  defp do_from_opts(query, nil, _apply_module, _mode, _extension_config), do: query
  defp do_from_opts(query, [], _apply_module, _mode, _extension_config), do: query

  defp do_from_opts(_query, opts, _apply_module, _mode, _extension_config)
       when not is_list(opts) do
    raise ArgumentError,
          "from_opts/2 expects opts to be a keyword list like `[where: ...]`, got: #{inspect(opts)}"
  end

  defp do_from_opts(_query, [invalid | _] = opts, _apply_module, _mode, _extension_config)
       when not is_tuple(invalid) or tuple_size(invalid) != 2 do
    raise ArgumentError,
          "from_opts/2 expects opts to be a keyword list (list of `{operation, value}` pairs); " <>
            "got invalid entry: #{inspect(invalid)} in #{inspect(opts)}"
  end

  defp do_from_opts(
         query,
         [{operation, raw_arguments} | tail],
         apply_module,
         mode,
         extension_config
       ) do
    unless is_atom(operation) do
      raise ArgumentError,
            "from_opts/2 expects operation keys to be atoms, got: #{inspect(operation)}"
    end

    if is_nil(raw_arguments) do
      raise ArgumentError,
            "from_opts/2 does not accept nil for #{inspect(operation)}; omit the operation or pass []"
    end

    if is_tuple(raw_arguments) do
      Validation.validate_tuple_arguments!(query, apply_module, operation, raw_arguments, mode)
    end

    arguments = Validation.normalize_arguments!(raw_arguments, mode)
    arity = 1 + length(arguments)

    if apply_module == QueryBuilder do
      Validation.validate_query_builder_operation!(operation, arity, mode)
    else
      QueryBuilder.FromOpts.Extension.validate_extension_from_opts_operation!(
        apply_module,
        operation,
        arity,
        mode,
        extension_config
      )
    end

    if mode == :boundary and operation in QueryBuilder.FromOpts.supported_operations(:boundary) do
      Boundary.validate_arguments!(operation, arguments)
    end

    result =
      if apply_module == QueryBuilder and mode == :boundary do
        Boundary.apply_operation!(query, operation, arguments)
      else
        apply(apply_module, operation, [query | arguments])
      end

    do_from_opts(result, tail, apply_module, mode, extension_config)
  end
end
