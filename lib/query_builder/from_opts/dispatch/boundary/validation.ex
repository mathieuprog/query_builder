defmodule QueryBuilder.FromOpts.Dispatch.Boundary.Validation do
  @moduledoc false

  def validate_arguments!(:where, [filters]) do
    validate_filters!(filters, "where")
  end

  def validate_arguments!(:where_any, [or_groups]) do
    validate_or_groups!(or_groups, "where_any")
  end

  def validate_arguments!(:order_by, [value]) do
    validate_order_by!(value)
  end

  def validate_arguments!(:limit, [value]) do
    validate_non_negative_limit_offset!(value, :limit)
  end

  def validate_arguments!(:offset, [value]) do
    validate_non_negative_limit_offset!(value, :offset)
  end

  def validate_arguments!(operation, _arguments) do
    raise ArgumentError,
          "operation #{inspect(operation)} is not supported in from_opts/2 (mode: :boundary); " <>
            "supported operations: #{QueryBuilder.FromOpts.supported_operations_string(:boundary)}. " <>
            "If you intended to use full mode, pass `mode: :full`."
  end

  defp validate_non_negative_limit_offset!(value, operation)
       when operation in [:limit, :offset] and is_integer(value) do
    if value < 0 do
      raise ArgumentError,
            "from_opts/2 boundary mode expects #{operation} to be non-negative, got: #{inspect(value)}"
    end

    :ok
  end

  defp validate_non_negative_limit_offset!(value, operation)
       when operation in [:limit, :offset] and is_binary(value) do
    trimmed = String.trim(value)

    case Integer.parse(trimmed) do
      {int_value, ""} when int_value < 0 ->
        raise ArgumentError,
              "from_opts/2 boundary mode expects #{operation} to be non-negative, got: #{inspect(value)}"

      _ ->
        :ok
    end
  end

  defp validate_non_negative_limit_offset!(_value, _operation), do: :ok

  defp validate_or_groups!(or_groups, context) do
    or_groups =
      QueryBuilder.Filters.normalize_or_groups!(
        or_groups,
        :where_any,
        "#{context} boundary validation"
      )

    Enum.each(or_groups, &validate_filters!(&1, context))
    :ok
  end

  defp validate_filters!(filters, context) do
    cond do
      filters == [] ->
        :ok

      is_list(filters) ->
        Enum.each(filters, &validate_filter!(&1, context))

      is_tuple(filters) ->
        validate_filter!(filters, context)

      is_function(filters) ->
        raise ArgumentError,
              "from_opts/2 boundary mode does not allow function filters in #{context}; " <>
                "use explicit QueryBuilder calls instead"

      true ->
        raise ArgumentError,
              "from_opts/2 boundary mode expects #{context} filters to be a keyword list, a list of filters, or a filter tuple; " <>
                "got: #{inspect(filters)}"
    end
  end

  defp validate_filter!(%QueryBuilder.Aggregate{} = aggregate, context) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregate expressions in #{context}: #{inspect(aggregate)}"
  end

  defp validate_filter!(
         {%QueryBuilder.Aggregate{} = aggregate, _value},
         context
       ) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregate expressions in #{context}: #{inspect(aggregate)}"
  end

  defp validate_filter!(
         {%QueryBuilder.Aggregate{} = aggregate, _operator, _value},
         context
       ) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregate expressions in #{context}: #{inspect(aggregate)}"
  end

  defp validate_filter!(
         {%QueryBuilder.Aggregate{} = aggregate, _operator, _value, _opts},
         context
       ) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregate expressions in #{context}: #{inspect(aggregate)}"
  end

  defp validate_filter!(fun, context) when is_function(fun) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow function filters in #{context}; " <>
            "use explicit QueryBuilder calls instead"
  end

  defp validate_filter!({field, value}, context) do
    validate_token!(field, context)
    validate_filter_value!(value, context)
    :ok
  end

  defp validate_filter!({field, operator, value}, context) do
    validate_filter!({field, operator, value, []}, context)
  end

  defp validate_filter!({field, operator, value, _operator_opts}, context)
       when is_atom(operator) do
    validate_token!(field, context)
    validate_filter_value!(value, context)
    :ok
  end

  defp validate_filter!({field, operator, _value, _operator_opts}, context) do
    raise ArgumentError,
          "from_opts/2 boundary mode expects #{context} filter operators to be atoms, got: #{inspect(operator)} for field #{inspect(field)}"
  end

  defp validate_filter!(other, context) do
    raise ArgumentError,
          "from_opts/2 boundary mode received an invalid #{context} filter: #{inspect(other)}"
  end

  defp validate_filter_value!(value, context)
       when is_struct(value, Ecto.Query) or is_struct(value, Ecto.SubQuery) or
              is_struct(value, QueryBuilder.Query) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow subqueries in #{context} filters; got: #{inspect(value)}"
  end

  defp validate_filter_value!(value, context)
       when is_struct(value, Ecto.Query.DynamicExpr) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow dynamic expressions in #{context} filters; got: #{inspect(value)}"
  end

  defp validate_filter_value!(value, context) when is_atom(value) do
    str = Atom.to_string(value)

    if String.ends_with?(str, "@self") do
      referenced = binary_part(str, 0, byte_size(str) - byte_size("@self"))

      if referenced == "" do
        raise ArgumentError,
              "from_opts/2 boundary mode expects @self filter values to be like :field@self, got: #{inspect(value)} in #{context}"
      end

      validate_token!(referenced, context)
    end

    :ok
  end

  defp validate_filter_value!(_value, _context), do: :ok

  defp validate_order_by!(value) do
    cond do
      value == [] ->
        :ok

      is_list(value) ->
        Enum.each(value, &validate_order_expr!/1)

      true ->
        raise ArgumentError,
              "from_opts/2 boundary mode expects order_by to be a keyword list (or list of order expressions), got: #{inspect(value)}"
    end
  end

  defp validate_order_expr!({direction, expr}) when is_atom(direction) do
    validate_order_expr_value!(expr)
  end

  defp validate_order_expr!(other) do
    raise ArgumentError,
          "from_opts/2 boundary mode received an invalid order_by expression: #{inspect(other)}"
  end

  defp validate_order_expr_value!(%QueryBuilder.Aggregate{} = aggregate) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow aggregates in order_by: #{inspect(aggregate)}"
  end

  defp validate_order_expr_value!(%Ecto.Query.DynamicExpr{} = dynamic) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow dynamic expressions in order_by: #{inspect(dynamic)}"
  end

  defp validate_order_expr_value!(fun) when is_function(fun) do
    raise ArgumentError,
          "from_opts/2 boundary mode does not allow function order_by expressions; " <>
            "use explicit QueryBuilder calls instead"
  end

  defp validate_order_expr_value!(token)
       when is_atom(token) or is_binary(token) do
    validate_token!(token, "order_by")
    :ok
  end

  defp validate_order_expr_value!(other) do
    raise ArgumentError,
          "from_opts/2 boundary mode expects order_by expressions to be tokens (atoms/strings), got: #{inspect(other)}"
  end

  defp validate_token!(token, context) when is_atom(token) or is_binary(token) do
    if token |> to_string() |> String.contains?("@") do
      raise ArgumentError,
            "from_opts/2 boundary mode does not allow assoc tokens (field@assoc) in #{context}: #{inspect(token)}"
    end

    :ok
  end

  defp validate_token!(token, context) do
    raise ArgumentError,
          "from_opts/2 boundary mode expects #{context} field tokens to be atoms or strings, got: #{inspect(token)}"
  end
end
