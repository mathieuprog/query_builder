defmodule QueryBuilder.Query.Where do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  @value_is_field_marker "@self"

  def where(ecto_query, assoc_list, filters, or_filters) do
    dynamic_query = build_dynamic_query(ecto_query, assoc_list, filters, or_filters)

    Ecto.Query.where(ecto_query, ^dynamic_query)
  end

  def build_dynamic_query(ecto_query, assoc_list, filters, or_filters) do
    resolve = &find_field_and_binding_from_token(assoc_list, &1)
    build_dynamic_query_with_resolver(ecto_query, filters, or_filters, resolve)
  end

  def build_dynamic_query_with_resolver(ecto_query, filters, or_filters, resolve)
      when is_function(resolve, 1) do
    [filters | Keyword.get_values(or_filters, :or)]
    |> Enum.reduce(nil, fn filters, or_acc ->
      filters
      |> List.wrap()
      |> build_dynamic_group_with_resolver(ecto_query, resolve)
      |> maybe_or_dynamic(or_acc)
    end)
    |> case do
      nil -> Ecto.Query.dynamic(true)
      dynamic_query -> dynamic_query
    end
  end

  defp build_dynamic_group_with_resolver([], _ecto_query, _resolve), do: nil

  defp build_dynamic_group_with_resolver([filter | tail], ecto_query, resolve) do
    first = apply_filter_with_resolver(ecto_query, resolve, filter)

    Enum.reduce(tail, first, fn filter, and_acc ->
      filter_dynamic = apply_filter_with_resolver(ecto_query, resolve, filter)
      Ecto.Query.dynamic(^and_acc and ^filter_dynamic)
    end)
  end

  defp maybe_or_dynamic(nil, nil), do: nil
  defp maybe_or_dynamic(nil, acc), do: acc
  defp maybe_or_dynamic(dynamic, nil), do: dynamic

  defp maybe_or_dynamic(dynamic, acc) do
    Ecto.Query.dynamic(^acc or ^dynamic)
  end

  defp apply_filter_with_resolver(_query, _resolve, %QueryBuilder.Aggregate{} = aggregate) do
    raise ArgumentError,
          "invalid where filter: aggregate expression #{inspect(aggregate)} cannot be used in WHERE; " <>
            "use HAVING (QueryBuilder.having/*) after GROUP BY (QueryBuilder.group_by/*) instead"
  end

  defp apply_filter_with_resolver(
         _query,
         _resolve,
         {%QueryBuilder.Aggregate{} = aggregate, _value}
       ) do
    raise ArgumentError,
          "invalid where filter: aggregate expression #{inspect(aggregate)} cannot be used in WHERE; " <>
            "use HAVING (QueryBuilder.having/*) after GROUP BY (QueryBuilder.group_by/*) instead"
  end

  defp apply_filter_with_resolver(
         _query,
         _resolve,
         {%QueryBuilder.Aggregate{} = aggregate, _operator, _value}
       ) do
    raise ArgumentError,
          "invalid where filter: aggregate expression #{inspect(aggregate)} cannot be used in WHERE; " <>
            "use HAVING (QueryBuilder.having/*) after GROUP BY (QueryBuilder.group_by/*) instead"
  end

  defp apply_filter_with_resolver(
         _query,
         _resolve,
         {%QueryBuilder.Aggregate{} = aggregate, _operator, _value, _operator_opts}
       ) do
    raise ArgumentError,
          "invalid where filter: aggregate expression #{inspect(aggregate)} cannot be used in WHERE; " <>
            "use HAVING (QueryBuilder.having/*) after GROUP BY (QueryBuilder.group_by/*) instead"
  end

  defp apply_filter_with_resolver(query, resolve, {field, value}) do
    apply_filter_with_resolver(query, resolve, {field, :eq, value, []})
  end

  defp apply_filter_with_resolver(query, resolve, {field, operator, value}) do
    apply_filter_with_resolver(query, resolve, {field, operator, value, []})
  end

  defp apply_filter_with_resolver(query, resolve, {field, operator, value, operator_opts})
       when is_atom(value) do
    {field, binding_field1} = resolve.(field)

    if value_is_field(value) do
      field2 = referenced_field_in_value(value)
      {field2, binding_field2} = resolve.(field2)

      do_where(
        query,
        binding_field1,
        binding_field2,
        {field, operator, field2, operator_opts}
      )
    else
      do_where(query, binding_field1, {field, operator, value, operator_opts})
    end
  end

  defp apply_filter_with_resolver(query, resolve, {field, operator, value, operator_opts}) do
    {field, binding} = resolve.(field)

    do_where(query, binding, {field, operator, value, operator_opts})
  end

  defp apply_filter_with_resolver(_query, resolve, custom_fun) when is_function(custom_fun, 1) do
    custom_fun.(resolve)
  end

  defp apply_filter_with_resolver(_query, _resolve, other) do
    raise ArgumentError,
          "got an invalid filter entry: #{inspect(other)}. " <>
            "Expected `{field, value}`, `{field, operator, value}`, `{field, operator, value, operator_opts}`, " <>
            "or a 1-arity function."
  end

  defp do_where(_query, binding, {field, :in, values, []}) when is_list(values) do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) in ^values)
  end

  defp do_where(query, binding, {field, :in, subqueryable, []}) when is_struct(subqueryable) do
    if Ecto.Queryable.impl_for(subqueryable) do
      Ecto.Query.dynamic([{^binding, x}], field(x, ^field) in subquery(subqueryable))
    else
      raise Ecto.QueryError,
        message:
          "expected an Ecto.Queryable subquery for :in on #{inspect(field)}, got: #{inspect(subqueryable)}",
        query: query
    end
  end

  defp do_where(_query, binding, {field, :not_in, values, []}) when is_list(values) do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) not in ^values)
  end

  defp do_where(query, binding, {field, :not_in, subqueryable, []})
       when is_struct(subqueryable) do
    if Ecto.Queryable.impl_for(subqueryable) do
      Ecto.Query.dynamic([{^binding, x}], field(x, ^field) not in subquery(subqueryable))
    else
      raise Ecto.QueryError,
        message:
          "expected an Ecto.Queryable subquery for :not_in on #{inspect(field)}, got: #{inspect(subqueryable)}",
        query: query
    end
  end

  defp do_where(_query, binding, {field, :include, value, []}) do
    Ecto.Query.dynamic([{^binding, x}], ^value in field(x, ^field))
  end

  defp do_where(_query, binding, {field, :exclude, value, []}) do
    Ecto.Query.dynamic([{^binding, x}], ^value not in field(x, ^field))
  end

  defp do_where(_query, binding, {field, operator, nil, []}) when operator in [:eq, :equal_to] do
    Ecto.Query.dynamic([{^binding, x}], is_nil(field(x, ^field)))
  end

  defp do_where(_query, binding, {field, operator, value, []})
       when operator in [:eq, :equal_to] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) == ^value)
  end

  defp do_where(_query, binding, {field, operator, nil, []})
       when operator in [:ne, :other_than] do
    Ecto.Query.dynamic([{^binding, x}], not is_nil(field(x, ^field)))
  end

  defp do_where(_query, binding, {field, operator, value, []})
       when operator in [:ne, :other_than] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) != ^value)
  end

  defp do_where(_query, binding, {field, operator, value, []})
       when operator in [:gt, :greater_than] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) > ^value)
  end

  defp do_where(_query, binding, {field, operator, value, []})
       when operator in [:ge, :greater_than_or_equal_to] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) >= ^value)
  end

  defp do_where(_query, binding, {field, operator, value, []})
       when operator in [:lt, :less_than] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) < ^value)
  end

  defp do_where(_query, binding, {field, operator, value, []})
       when operator in [:le, :less_than_or_equal_to] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) <= ^value)
  end

  defp do_where(query, binding, {field, search_operation, value, operator_opts})
       when search_operation in [:starts_with, :ends_with, :contains] do
    unless is_binary(value) do
      raise Ecto.QueryError,
        message:
          "expected a string for #{inspect(search_operation)} on #{inspect(field)}, got: #{inspect(value)}",
        query: query
    end

    value =
      value
      |> String.replace("%", "\\%")
      |> String.replace("_", "\\_")

    value =
      case search_operation do
        :starts_with -> "#{value}%"
        :ends_with -> "%#{value}"
        :contains -> "%#{value}%"
      end

    case Keyword.get(operator_opts, :case, :sensitive) do
      :sensitive ->
        Ecto.Query.dynamic([{^binding, x}], like(field(x, ^field), ^value))

      case_sensitivity when case_sensitivity in [:insensitive, :i] ->
        Ecto.Query.dynamic([{^binding, x}], ilike(field(x, ^field), ^value))

      other ->
        raise Ecto.QueryError,
          message:
            "invalid :case option #{inspect(other)} for #{inspect(search_operation)} on #{inspect(field)} " <>
              "(supported: :sensitive, :insensitive, :i)",
          query: query
    end
  end

  defp do_where(_query, binding, {field, :like, value, []}) do
    Ecto.Query.dynamic([{^binding, x}], like(field(x, ^field), ^value))
  end

  defp do_where(_query, binding, {field, :ilike, value, []}) do
    Ecto.Query.dynamic([{^binding, x}], ilike(field(x, ^field), ^value))
  end

  defp do_where(query, _binding, {field, operator, value, operator_opts}) do
    raise Ecto.QueryError,
      message:
        "unsupported filter {#{inspect(field)}, #{inspect(operator)}, #{inspect(value)}, #{inspect(operator_opts)}}",
      query: query
  end

  defp do_where(_query, b1, b2, {f1, operator, f2, []}) when operator in [:eq, :equal_to] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) == field(y, ^f2))
  end

  defp do_where(_query, b1, b2, {f1, operator, f2, []}) when operator in [:ne, :other_than] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) != field(y, ^f2))
  end

  defp do_where(_query, b1, b2, {f1, operator, f2, []}) when operator in [:gt, :greater_than] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) > field(y, ^f2))
  end

  defp do_where(_query, b1, b2, {f1, operator, f2, []})
       when operator in [:ge, :greater_than_or_equal_to] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) >= field(y, ^f2))
  end

  defp do_where(_query, b1, b2, {f1, operator, f2, []}) when operator in [:lt, :less_than] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) < field(y, ^f2))
  end

  defp do_where(_query, b1, b2, {f1, operator, f2, []})
       when operator in [:le, :less_than_or_equal_to] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) <= field(y, ^f2))
  end

  defp do_where(query, b1, b2, {f1, search_operation, f2, operator_opts})
       when search_operation in [:starts_with, :ends_with, :contains] do
    case Keyword.get(operator_opts, :case, :sensitive) do
      :sensitive ->
        case search_operation do
          :starts_with ->
            Ecto.Query.dynamic(
              [{^b1, x}, {^b2, y}],
              fragment("? like concat(?, '%')", field(x, ^f1), field(y, ^f2))
            )

          :ends_with ->
            Ecto.Query.dynamic(
              [{^b1, x}, {^b2, y}],
              fragment("? like concat('%', ?)", field(x, ^f1), field(y, ^f2))
            )

          :contains ->
            Ecto.Query.dynamic(
              [{^b1, x}, {^b2, y}],
              fragment("? like concat('%', ?, '%')", field(x, ^f1), field(y, ^f2))
            )
        end

      case_sensitivity when case_sensitivity in [:insensitive, :i] ->
        case search_operation do
          :starts_with ->
            Ecto.Query.dynamic(
              [{^b1, x}, {^b2, y}],
              fragment("? ilike concat(?, '%')", field(x, ^f1), field(y, ^f2))
            )

          :ends_with ->
            Ecto.Query.dynamic(
              [{^b1, x}, {^b2, y}],
              fragment("? ilike concat('%', ?)", field(x, ^f1), field(y, ^f2))
            )

          :contains ->
            Ecto.Query.dynamic(
              [{^b1, x}, {^b2, y}],
              fragment("? ilike concat('%', ?, '%')", field(x, ^f1), field(y, ^f2))
            )
        end

      other ->
        raise Ecto.QueryError,
          message:
            "invalid :case option #{inspect(other)} for field-to-field #{inspect(search_operation)} " <>
              "on #{inspect(f1)} vs #{inspect(f2)} (supported: :sensitive, :insensitive, :i)",
          query: query
    end
  end

  defp do_where(query, _b1, _b2, {f1, operator, f2, operator_opts}) do
    raise Ecto.QueryError,
      message:
        "unsupported field-to-field filter {#{inspect(f1)}, #{inspect(operator)}, #{inspect(f2)}, #{inspect(operator_opts)}}",
      query: query
  end

  defp value_is_field(val) when val in [nil, false, true], do: false

  defp value_is_field(val) do
    val |> to_string() |> String.ends_with?(@value_is_field_marker)
  end

  defp referenced_field_in_value(val) do
    str_val = to_string(val)

    str_val
    |> binary_part(0, byte_size(str_val) - byte_size(@value_is_field_marker))
  end
end
