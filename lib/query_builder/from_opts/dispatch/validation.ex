defmodule QueryBuilder.FromOpts.Dispatch.Validation do
  @moduledoc false

  def normalize_arguments!(raw_arguments, mode) do
    case raw_arguments do
      %QueryBuilder.Args{} when mode == :boundary ->
        raise ArgumentError,
              "from_opts/2 does not accept QueryBuilder.args/* wrappers in boundary mode; " <>
                "pass a single argument (where/order_by/limit/offset) and avoid assoc traversal. " <>
                "If you intended to use the full from_opts surface, pass `mode: :full`."

      %QueryBuilder.Args{args: args} when is_list(args) and length(args) >= 2 ->
        args

      %QueryBuilder.Args{args: args} when is_list(args) ->
        raise ArgumentError,
              "from_opts/2 expects QueryBuilder.args/* to wrap at least 2 arguments, got: #{inspect(args)}"

      %QueryBuilder.Args{} = args ->
        raise ArgumentError,
              "from_opts/2 expects QueryBuilder.args/* to wrap a list of arguments; got: #{inspect(args)}"

      other ->
        [other]
    end
  end

  def validate_query_builder_operation!(operation, arity, mode) do
    supported_operations = QueryBuilder.FromOpts.supported_operations(mode)
    supported_operations_string = QueryBuilder.FromOpts.supported_operations_string(mode)

    unless function_exported?(QueryBuilder, operation, arity) do
      raise ArgumentError,
            "unknown operation #{inspect(operation)}/#{arity} in from_opts/2; " <>
              "supported operations: #{supported_operations_string}"
    end

    unless operation in supported_operations do
      extra =
        if mode == :boundary do
          " If you intended to use joins/preloads/assoc traversal, pass `mode: :full`."
        else
          ""
        end

      raise ArgumentError,
            "operation #{inspect(operation)}/#{arity} is not supported in from_opts/2 (mode: #{inspect(mode)}); " <>
              "supported operations: #{supported_operations_string}." <> extra
    end
  end

  def validate_tuple_arguments!(query, apply_module, operation, raw_arguments, mode)
      when is_tuple(raw_arguments) do
    cond do
      operation == :where and tuple_size(raw_arguments) < 2 ->
        raise ArgumentError,
              "from_opts/2 expects `where:` tuple filters to have at least 2 elements " <>
                "(e.g. `{field, value}` or `{field, operator, value}`); got: #{inspect(raw_arguments)}"

      # Migration guard: v1's from_list/from_opts expanded `{assoc_fields, filters, ...}` tuples
      # into multi-arg calls. v2 treats tuple values as data, so we fail fast and point callers
      # at the explicit wrapper (`QueryBuilder.args/*`).
      operation == :where and where_tuple_looks_like_assoc_pack?(query, raw_arguments) ->
        raise ArgumentError,
              "from_opts/2 does not treat `where: {assoc_fields, filters, ...}` as a multi-arg call. " <>
                "Use `where: QueryBuilder.args(assoc_fields, filters, ...)` with `mode: :full` instead; " <>
                "got: #{inspect(raw_arguments)}"

      operation in [:where, :select] ->
        :ok

      operation in QueryBuilder.FromOpts.supported_operations(:full) ->
        case mode do
          :boundary ->
            raise ArgumentError,
                  "from_opts/2 boundary mode does not accept tuple values for #{inspect(operation)}. " <>
                    "Pass a single argument value. If you intended a multi-arg call, use `mode: :full` " <>
                    "and wrap arguments with `QueryBuilder.args/*`. Got: #{inspect(raw_arguments)}"

          :full ->
            raise ArgumentError,
                  "from_opts/2 does not accept tuple values for #{inspect(operation)}. " <>
                    "If you intended to call #{inspect(operation)} with multiple arguments, " <>
                    "wrap them with `QueryBuilder.args/*`. Got: #{inspect(raw_arguments)}"
        end

      apply_module != QueryBuilder and
        function_exported?(apply_module, operation, tuple_size(raw_arguments) + 1) and
          not function_exported?(apply_module, operation, 2) ->
        case mode do
          :boundary ->
            raise ArgumentError,
                  "from_opts/2 boundary mode does not expand tuple values into multiple arguments for #{inspect(operation)}. " <>
                    "Pass a single argument value. If you intended a multi-arg call, use `mode: :full` " <>
                    "and wrap arguments with `#{inspect(apply_module)}.args/*` (or `QueryBuilder.args/*`). " <>
                    "Got: #{inspect(raw_arguments)}"

          :full ->
            raise ArgumentError,
                  "from_opts/2 does not expand tuple values into multiple arguments for #{inspect(operation)}. " <>
                    "Use `#{inspect(apply_module)}.args/*` (or `QueryBuilder.args/*`) to wrap multiple arguments; " <>
                    "got: #{inspect(raw_arguments)}"
        end

      true ->
        :ok
    end
  end

  defp where_tuple_looks_like_assoc_pack?(query, tuple) when is_tuple(tuple) do
    if tuple_size(tuple) < 2 do
      false
    else
      assoc_fields = elem(tuple, 0)
      second = elem(tuple, 1)

      cond do
        is_list(assoc_fields) ->
          true

        # Likely a filter tuple: {field, operator, value} / {field, operator, value, opts}
        is_atom(assoc_fields) and tuple_size(tuple) >= 3 and is_atom(second) ->
          false

        # Likely a filter tuple: {field, value} (scalar value)
        is_atom(assoc_fields) and tuple_size(tuple) == 2 and not is_list(second) ->
          false

        is_atom(assoc_fields) ->
          source_schema = QueryBuilder.Utils.root_schema(query)
          assoc_fields in source_schema.__schema__(:associations)

        true ->
          false
      end
    end
  end
end
