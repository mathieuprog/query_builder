defmodule QueryBuilder.Query.Where do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def where(query, assoc_fields, filters, where_type \\ :and) do
    token = QueryBuilder.Token.token(query, assoc_fields)

    {query, token} = QueryBuilder.JoinMaker.make_joins(query, token)

    apply_filters(query, token, List.wrap(filters), where_type)
  end

  defp apply_filters(query, _token, [], _where_type), do: query

  defp apply_filters(query, token, [filter | tail], where_type) do
    query = apply_filter(query, token, filter, where_type)
    apply_filters(query, token, tail, where_type)
  end

  defp apply_filter(query, token, {field, value}, where_type) do
    apply_filter(query, token, {field, :eq, value, []}, where_type)
  end

  defp apply_filter(query, token, {field, operator, value}, where_type) do
    apply_filter(query, token, {field, operator, value, []}, where_type)
  end

  defp apply_filter(query, token, {field1, operator, field2, operator_opts}, where_type)
       when is_atom(field2) and field2 not in [nil, false, true] do
    {field1, binding_field1} = find_field_and_binding_from_token(query, token, field1)
    {field2, binding_field2} = find_field_and_binding_from_token(query, token, field2)
    do_where(
      query,
      binding_field1,
      binding_field2,
      {field1, operator, field2, operator_opts},
      where_type
    )
  end

  defp apply_filter(query, token, {field, operator, value, operator_opts}, where_type) do
    {field, binding} = find_field_and_binding_from_token(query, token, field)

    do_where(query, binding, {field, operator, value, operator_opts}, where_type)
  end

  defp do_where(query, binding, {field, :in, values, []}, where_type) when is_list(values) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) in ^values)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) in ^values)
    end
  end

  defp do_where(query, binding, {field, :not_in, values, []}, where_type) when is_list(values) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) not in ^values)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) not in ^values)
    end
  end

  defp do_where(query, binding, {field, :include, value, []}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], ^value in field(x, ^field))
      :or -> Ecto.Query.or_where(query, [{^binding, x}], ^value in field(x, ^field))
    end
  end

  defp do_where(query, binding, {field, :exclude, value, []}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], ^value not in field(x, ^field))
      :or -> Ecto.Query.or_where(query, [{^binding, x}], ^value not in field(x, ^field))
    end
  end

  defp do_where(query, binding, {field, operator, nil, []}, where_type) when operator in [:eq, :equal_to] do
    IO.inspect "A"
    IO.inspect nil

    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], is_nil(field(x, ^field)))
      :or -> Ecto.Query.or_where(query, [{^binding, x}], is_nil(field(x, ^field)))
    end
  end

  defp do_where(query, binding, {field, operator, value, []}, where_type) when operator in [:eq, :equal_to] do
    IO.inspect "B"
    IO.inspect value

    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) == ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) == ^value)
    end
  end

  defp do_where(query, binding, {field, operator, nil, []}, where_type) when operator in [:ne, :other_than] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], not is_nil(field(x, ^field)))
      :or -> Ecto.Query.or_where(query, [{^binding, x}], not is_nil(field(x, ^field)))
    end
  end

  defp do_where(query, binding, {field, operator, value, []}, where_type) when operator in [:ne, :other_than] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) != ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) != ^value)
    end
  end

  defp do_where(query, binding, {field, operator, value, []}, where_type) when operator in [:gt, :greater_than] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) > ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) > ^value)
    end
  end

  defp do_where(query, binding, {field, operator, value, []}, where_type) when operator in [:ge, :greater_than_or_equal_to] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) >= ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) >= ^value)
    end
  end

  defp do_where(query, binding, {field, operator, value, []}, where_type) when operator in [:lt, :less_than] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) < ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) < ^value)
    end
  end

  defp do_where(query, binding, {field, operator, value, []}, where_type) when operator in [:le, :less_than_or_equal_to] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) <= ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) <= ^value)
    end
  end

  defp do_where(query, binding, {field, search_operation, value, operator_opts}, where_type)
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
        case where_type do
          :and -> Ecto.Query.where(query, [{^binding, x}], like(field(x, ^field), ^value))
          :or -> Ecto.Query.or_where(query, [{^binding, x}], like(field(x, ^field), ^value))
        end

      case_sensitivity when case_sensitivity in [:insensitive, :i] ->
        case where_type do
          :and -> Ecto.Query.where(query, [{^binding, x}], ilike(field(x, ^field), ^value))
          :or -> Ecto.Query.or_where(query, [{^binding, x}], ilike(field(x, ^field), ^value))
        end
    end
  end

  defp do_where(query, binding, {field, :like, value, []}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], like(field(x, ^field), ^value))
      :or -> Ecto.Query.or_where(query, [{^binding, x}], like(field(x, ^field), ^value))
    end
  end

  defp do_where(query, binding, {field, :ilike, value, []}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], ilike(field(x, ^field), ^value))
      :or -> Ecto.Query.or_where(query, [{^binding, x}], ilike(field(x, ^field), ^value))
    end
  end

  defp do_where(query, b1, b2, {f1, operator, f2, []}, where_type) when operator in [:eq, :equal_to] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) == field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) == field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, operator, f2, []}, where_type) when operator in [:ne, :other_than] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) != field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) != field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, operator, f2, []}, where_type) when operator in [:gt, :greater_than] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) > field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) > field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, operator, f2, []}, where_type) when operator in [:ge, :greater_than_or_equal_to] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) >= field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) >= field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, operator, f2, []}, where_type) when operator in [:lt, :less_than] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) < field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) < field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, operator, f2, []}, where_type) when operator in [:le, :less_than_or_equal_to] do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) <= field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) <= field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, search_operation, f2, operator_opts}, where_type)
       when search_operation in [:starts_with, :ends_with, :contains] do
    case Keyword.get(operator_opts, :case, :sensitive) do
      :sensitive ->
        case where_type do
          :and ->
            case search_operation do
              :starts_with ->
                Ecto.Query.where(query, [{^b1, x}, {^b2, y}], fragment("? like concat(?, '%')", field(x, ^f1), field(y, ^f2)))
              :ends_with ->
                Ecto.Query.where(query, [{^b1, x}, {^b2, y}], fragment("? like concat('%', ?)", field(x, ^f1), field(y, ^f2)))
              :contains ->
                Ecto.Query.where(query, [{^b1, x}, {^b2, y}], fragment("? like concat('%', ?, '%')", field(x, ^f1), field(y, ^f2)))
            end
          :or ->
            case search_operation do
              :starts_with ->
                Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], fragment("? like concat(?, '%')", field(x, ^f1), field(y, ^f2)))
              :ends_with ->
                Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], fragment("? like concat('%', ?)", field(x, ^f1), field(y, ^f2)))
              :contains ->
                Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], fragment("? like concat('%', ?, '%')", field(x, ^f1), field(y, ^f2)))
            end
        end

      case_sensitivity when case_sensitivity in [:insensitive, :i] ->
        case where_type do
          :and ->
            case search_operation do
              :starts_with ->
                Ecto.Query.where(query, [{^b1, x}, {^b2, y}], fragment("? ilike concat(?, '%')", field(x, ^f1), field(y, ^f2)))
              :ends_with ->
                Ecto.Query.where(query, [{^b1, x}, {^b2, y}], fragment("? ilike concat('%', ?)", field(x, ^f1), field(y, ^f2)))
              :contains ->
                Ecto.Query.where(query, [{^b1, x}, {^b2, y}], fragment("? ilike concat('%', ?, '%')", field(x, ^f1), field(y, ^f2)))
            end
          :or ->
            case search_operation do
              :starts_with ->
                Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], fragment("? ilike concat(?, '%')", field(x, ^f1), field(y, ^f2)))
              :ends_with ->
                Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], fragment("? ilike concat('%', ?)", field(x, ^f1), field(y, ^f2)))
              :contains ->
                Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], fragment("? ilike concat('%', ?, '%')", field(x, ^f1), field(y, ^f2)))
            end
        end
    end
  end
end
