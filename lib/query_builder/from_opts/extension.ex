defmodule QueryBuilder.FromOpts.Extension do
  @moduledoc false

  def extension_from_opts_config!(apply_module) do
    config =
      if function_exported?(apply_module, :__query_builder_extension_from_opts_config__, 0) do
        apply(apply_module, :__query_builder_extension_from_opts_config__, [])
      else
        %{}
      end

    unless is_map(config) do
      raise ArgumentError,
            "#{inspect(apply_module)}.__query_builder_extension_from_opts_config__/0 must return a map, got: #{inspect(config)}"
    end

    validate_extension_config_ops_list!(apply_module, config, :from_opts_full_ops)
    validate_extension_config_ops_list!(apply_module, config, :boundary_ops_user_asserted)

    config
  end

  def validate_extension_from_opts_operation!(
        apply_module,
        operation,
        arity,
        mode,
        extension_config
      ) do
    supported_operations = QueryBuilder.FromOpts.supported_operations(mode)
    supported_operations_string = QueryBuilder.FromOpts.supported_operations_string(mode)

    extension_boundary_ops = Map.get(extension_config, :boundary_ops_user_asserted, [])

    extension_full_ops =
      Map.get(extension_config, :from_opts_full_ops, [])
      |> Kernel.++(extension_boundary_ops)
      |> Enum.uniq()

    cond do
      mode == :boundary and operation in supported_operations ->
        :ok

      mode == :boundary and operation in extension_boundary_ops ->
        if function_exported?(QueryBuilder, operation, arity) do
          raise ArgumentError,
                "operation #{inspect(operation)}/#{arity} is a QueryBuilder operation and is not supported in from_opts/2 (mode: :boundary). " <>
                  "Use `mode: :full` instead of extending boundary mode."
        end

        :ok

      mode == :boundary ->
        raise ArgumentError,
              "operation #{inspect(operation)}/#{arity} is not supported in from_opts/2 (mode: :boundary); " <>
                "supported operations: #{supported_operations_string}. If you intended to use full mode, pass `mode: :full`."

      mode == :full and function_exported?(QueryBuilder, operation, arity) and
          operation not in supported_operations ->
        raise ArgumentError,
              "operation #{inspect(operation)}/#{arity} is not supported in from_opts/2 (mode: :full); " <>
                "supported operations: #{supported_operations_string}"

      mode == :full and operation in supported_operations ->
        :ok

      mode == :full and operation in extension_full_ops ->
        :ok

      mode == :full ->
        raise ArgumentError,
              "operation #{inspect(operation)}/#{arity} is not supported in from_opts/2 (mode: :full) for #{inspect(apply_module)}. " <>
                "To allow custom extension operations, pass `use QueryBuilder.Extension, from_opts_full_ops: [...]` when defining #{inspect(apply_module)}."
    end

    unless function_exported?(apply_module, operation, arity) do
      available =
        apply_module.__info__(:functions)
        |> Enum.map(&elem(&1, 0))
        |> Enum.uniq()
        |> Enum.sort()
        |> Enum.join(", ")

      raise ArgumentError,
            "unknown operation #{inspect(operation)}/#{arity} in from_opts/2; " <>
              "expected a public function on #{inspect(apply_module)}. Available operations: #{available}"
    end
  end

  defp validate_extension_config_ops_list!(apply_module, config, key) do
    case Map.get(config, key, []) do
      list when is_list(list) ->
        Enum.each(list, fn
          op when is_atom(op) ->
            :ok

          other ->
            raise ArgumentError,
                  "#{inspect(apply_module)}.__query_builder_extension_from_opts_config__/0 expects #{inspect(key)} to be a list of atoms, got: #{inspect(other)} in #{inspect(list)}"
        end)

      other ->
        raise ArgumentError,
              "#{inspect(apply_module)}.__query_builder_extension_from_opts_config__/0 expects #{inspect(key)} to be a list of atoms, got: #{inspect(other)}"
    end
  end
end
