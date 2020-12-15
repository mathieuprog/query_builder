defmodule QueryBuilder.Query.Where do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def where(ecto_query, assoc_list, filters, or_filters) do
    dynamic_query = build_dynamic_query(ecto_query, assoc_list, filters, or_filters)

    Ecto.Query.where(ecto_query, ^dynamic_query)
  end

  def build_dynamic_query(ecto_query, assoc_list, filters, or_filters) do
    filters_list = [filters | Keyword.get_values(or_filters, :or)]

    filters_list
    |> Enum.filter(&(&1 != []))
    |> Enum.map(fn filters ->
      apply_filters(ecto_query, assoc_list, List.wrap(filters))
      |> Enum.reduce(&Ecto.Query.dynamic(^&1 and ^&2))
    end)
    |> Enum.reduce(&Ecto.Query.dynamic(^&1 or ^&2))
  end

  defp apply_filters(_query, _assoc_list, []), do: []

  defp apply_filters(query, assoc_list, [filter | tail]) do
    [apply_filter(query, assoc_list, filter) | apply_filters(query, assoc_list, tail)]
  end

  defp apply_filter(query, assoc_list, {field, value}) do
    apply_filter(query, assoc_list, {field, :eq, value, []})
  end

  defp apply_filter(query, assoc_list, {field, operator, value}) do
    apply_filter(query, assoc_list, {field, operator, value, []})
  end

  defp apply_filter(query, assoc_list, {field1, operator, field2, operator_opts})
       when is_atom(field2) and field2 not in [nil, false, true] do
    {field1, binding_field1} = find_field_and_binding_from_token(query, assoc_list, field1)
    {field2, binding_field2} = find_field_and_binding_from_token(query, assoc_list, field2)

    do_where(
      binding_field1,
      binding_field2,
      {field1, operator, field2, operator_opts}
    )
  end

  defp apply_filter(query, assoc_list, {field, operator, value, operator_opts}) do
    {field, binding} = find_field_and_binding_from_token(query, assoc_list, field)

    do_where(binding, {field, operator, value, operator_opts})
  end

  defp apply_filter(query, assoc_list, custom_fun) when is_function(custom_fun) do
    custom_fun.(&(find_field_and_binding_from_token(query, assoc_list, &1)))
  end

  defp do_where(binding, {field, :in, values, []}) when is_list(values) do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) in ^values)
  end

  defp do_where(binding, {field, :not_in, values, []}) when is_list(values) do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) not in ^values)
  end

  defp do_where(binding, {field, :include, value, []}) do
    Ecto.Query.dynamic([{^binding, x}], ^value in field(x, ^field))
  end

  defp do_where(binding, {field, :exclude, value, []}) do
    Ecto.Query.dynamic([{^binding, x}], ^value not in field(x, ^field))
  end

  defp do_where(binding, {field, operator, nil, []}) when operator in [:eq, :equal_to] do
    Ecto.Query.dynamic([{^binding, x}], is_nil(field(x, ^field)))
  end

  defp do_where(binding, {field, operator, value, []}) when operator in [:eq, :equal_to] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) == ^value)
  end

  defp do_where(binding, {field, operator, nil, []}) when operator in [:ne, :other_than] do
    Ecto.Query.dynamic([{^binding, x}], not is_nil(field(x, ^field)))
  end

  defp do_where(binding, {field, operator, value, []}) when operator in [:ne, :other_than] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) != ^value)
  end

  defp do_where(binding, {field, operator, value, []}) when operator in [:gt, :greater_than] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) > ^value)
  end

  defp do_where(binding, {field, operator, value, []}) when operator in [:ge, :greater_than_or_equal_to] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) >= ^value)
  end

  defp do_where(binding, {field, operator, value, []}) when operator in [:lt, :less_than] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) < ^value)
  end

  defp do_where(binding, {field, operator, value, []}) when operator in [:le, :less_than_or_equal_to] do
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field) <= ^value)
  end

  defp do_where(binding, {field, search_operation, value, operator_opts})
       when search_operation in [:starts_with, :ends_with, :contains] do
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
    end
  end

  defp do_where(binding, {field, :like, value, []}) do
    Ecto.Query.dynamic([{^binding, x}], like(field(x, ^field), ^value))
  end

  defp do_where(binding, {field, :ilike, value, []}) do
    Ecto.Query.dynamic([{^binding, x}], ilike(field(x, ^field), ^value))
  end

  defp do_where(b1, b2, {f1, operator, f2, []}) when operator in [:eq, :equal_to] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) == field(y, ^f2))
  end

  defp do_where(b1, b2, {f1, operator, f2, []}) when operator in [:ne, :other_than] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) != field(y, ^f2))
  end

  defp do_where(b1, b2, {f1, operator, f2, []}) when operator in [:gt, :greater_than] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) > field(y, ^f2))
  end

  defp do_where(b1, b2, {f1, operator, f2, []}) when operator in [:ge, :greater_than_or_equal_to] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) >= field(y, ^f2))
  end

  defp do_where(b1, b2, {f1, operator, f2, []}) when operator in [:lt, :less_than] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) < field(y, ^f2))
  end

  defp do_where(b1, b2, {f1, operator, f2, []}) when operator in [:le, :less_than_or_equal_to] do
    Ecto.Query.dynamic([{^b1, x}, {^b2, y}], field(x, ^f1) <= field(y, ^f2))
  end

  defp do_where(b1, b2, {f1, search_operation, f2, operator_opts})
       when search_operation in [:starts_with, :ends_with, :contains] do
    case Keyword.get(operator_opts, :case, :sensitive) do
      :sensitive ->
        case search_operation do
          :starts_with ->
            Ecto.Query.dynamic([{^b1, x}, {^b2, y}], fragment("? like concat(?, '%')", field(x, ^f1), field(y, ^f2)))
          :ends_with ->
            Ecto.Query.dynamic([{^b1, x}, {^b2, y}], fragment("? like concat('%', ?)", field(x, ^f1), field(y, ^f2)))
          :contains ->
            Ecto.Query.dynamic([{^b1, x}, {^b2, y}], fragment("? like concat('%', ?, '%')", field(x, ^f1), field(y, ^f2)))
        end

      case_sensitivity when case_sensitivity in [:insensitive, :i] ->
        case search_operation do
          :starts_with ->
            Ecto.Query.dynamic([{^b1, x}, {^b2, y}], fragment("? ilike concat(?, '%')", field(x, ^f1), field(y, ^f2)))
          :ends_with ->
            Ecto.Query.dynamic([{^b1, x}, {^b2, y}], fragment("? ilike concat('%', ?)", field(x, ^f1), field(y, ^f2)))
          :contains ->
            Ecto.Query.dynamic([{^b1, x}, {^b2, y}], fragment("? ilike concat('%', ?, '%')", field(x, ^f1), field(y, ^f2)))
        end
    end
  end
end
