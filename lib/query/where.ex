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
    apply_filter(query, token, {field, :eq, value}, where_type)
  end

  defp apply_filter(query, token, {field1, operator, field2}, where_type) when is_atom(field2) do
    {field1, binding_field1} = find_field_and_binding_from_token(query, token, field1)
    {field2, binding_field2} = find_field_and_binding_from_token(query, token, field2)

    do_where(query, binding_field1, binding_field2, {field1, operator, field2}, where_type)
  end

  defp apply_filter(query, token, {field, operator, value}, where_type) do
    {field, binding} = find_field_and_binding_from_token(query, token, field)

    do_where(query, binding, {field, operator, value}, where_type)
  end

  defp do_where(query, binding, {field, :eq, value}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) == ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) == ^value)
    end
  end

  defp do_where(query, binding, {field, :ne, value}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) != ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) != ^value)
    end
  end

  defp do_where(query, binding, {field, :gt, value}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) > ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) > ^value)
    end
  end

  defp do_where(query, binding, {field, :ge, value}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) >= ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) >= ^value)
    end
  end

  defp do_where(query, binding, {field, :lt, value}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) < ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) < ^value)
    end
  end

  defp do_where(query, binding, {field, :le, value}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^binding, x}], field(x, ^field) <= ^value)
      :or -> Ecto.Query.or_where(query, [{^binding, x}], field(x, ^field) <= ^value)
    end
  end

  defp do_where(query, b1, b2, {f1, :eq, f2}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) == field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) == field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, :ne, f2}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) != field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) != field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, :gt, f2}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) > field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) > field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, :ge, f2}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) >= field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) >= field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, :lt, f2}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) < field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) < field(y, ^f2))
    end
  end

  defp do_where(query, b1, b2, {f1, :le, f2}, where_type) do
    case where_type do
      :and -> Ecto.Query.where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) <= field(y, ^f2))
      :or -> Ecto.Query.or_where(query, [{^b1, x}, {^b2, y}], field(x, ^f1) <= field(y, ^f2))
    end
  end
end
