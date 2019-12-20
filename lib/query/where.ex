defmodule QueryBuilder.Query.Where do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def where(query, assoc_fields, filters) do
    token = QueryBuilder.Token.token(query, assoc_fields)

    {query, token} = QueryBuilder.JoinMaker.make_joins(query, token)

    apply_filters(query, token, List.wrap(filters))
  end

  defp apply_filters(query, _token, []), do: query

  defp apply_filters(query, token, [filter | tail]) do
    query = apply_filter(query, token, filter)
    apply_filters(query, token, tail)
  end

  defp apply_filter(query, token, {field, value}) do
    apply_filter(query, token, {field, :eq, value})
  end

  defp apply_filter(query, token, {field1, operator, field2}) when is_atom(field2) do
    {field1, binding_field1} = find_field_and_binding_from_token(query, token, field1)
    {field2, binding_field2} = find_field_and_binding_from_token(query, token, field2)

    do_where(query, binding_field1, binding_field2, {field1, operator, field2})
  end

  defp apply_filter(query, token, {field, operator, value}) do
    {field, binding} = find_field_and_binding_from_token(query, token, field)

    do_where(query, binding, {field, operator, value})
  end

  defp do_where(query, binding, {field, :eq, value}) do
    Ecto.Query.where(query, [{^binding, x}], field(x, ^field) == ^value)
  end

  defp do_where(query, binding, {field, :ne, value}) do
    Ecto.Query.where(query, [{^binding, x}], field(x, ^field) != ^value)
  end

  defp do_where(query, binding, {field, :gt, value}) do
    Ecto.Query.where(query, [{^binding, x}], field(x, ^field) > ^value)
  end

  defp do_where(query, binding, {field, :ge, value}) do
    Ecto.Query.where(query, [{^binding, x}], field(x, ^field) >= ^value)
  end

  defp do_where(query, binding, {field, :lt, value}) do
    Ecto.Query.where(query, [{^binding, x}], field(x, ^field) < ^value)
  end

  defp do_where(query, binding, {field, :le, value}) do
    Ecto.Query.where(query, [{^binding, x}], field(x, ^field) <= ^value)
  end

  defp do_where(query, binding1, binding2, {field1, :eq, field2}) do
    Ecto.Query.where(
      query,
      [{^binding1, x}, {^binding2, y}],
      field(x, ^field1) == field(y, ^field2)
    )
  end

  defp do_where(query, binding1, binding2, {field1, :ne, field2}) do
    Ecto.Query.where(
      query,
      [{^binding1, x}, {^binding2, y}],
      field(x, ^field1) != field(y, ^field2)
    )
  end

  defp do_where(query, binding1, binding2, {field1, :gt, field2}) do
    Ecto.Query.where(
      query,
      [{^binding1, x}, {^binding2, y}],
      field(x, ^field1) > field(y, ^field2)
    )
  end

  defp do_where(query, binding1, binding2, {field1, :ge, field2}) do
    Ecto.Query.where(
      query,
      [{^binding1, x}, {^binding2, y}],
      field(x, ^field1) >= field(y, ^field2)
    )
  end

  defp do_where(query, binding1, binding2, {field1, :lt, field2}) do
    Ecto.Query.where(
      query,
      [{^binding1, x}, {^binding2, y}],
      field(x, ^field1) < field(y, ^field2)
    )
  end

  defp do_where(query, binding1, binding2, {field1, :le, field2}) do
    Ecto.Query.where(
      query,
      [{^binding1, x}, {^binding2, y}],
      field(x, ^field1) <= field(y, ^field2)
    )
  end
end
