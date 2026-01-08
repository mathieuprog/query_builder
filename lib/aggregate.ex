defmodule QueryBuilder.Aggregate do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  defstruct op: nil, arg: nil, modifier: nil, order_by: nil, filter: nil

  def to_dynamic(assoc_list, %__MODULE__{} = aggregate) do
    to_dynamic_with_resolver(aggregate, &find_field_and_binding_from_token(assoc_list, &1))
  end

  defp to_dynamic_with_resolver(%__MODULE__{op: :count, arg: nil}, _resolve) do
    Ecto.Query.dynamic([], count())
  end

  defp to_dynamic_with_resolver(%__MODULE__{op: :count, arg: token, modifier: :distinct}, resolve)
       when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], count(field(x, ^field), :distinct))
  end

  defp to_dynamic_with_resolver(%__MODULE__{op: :count, arg: token, modifier: nil}, resolve)
       when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], count(field(x, ^field)))
  end

  defp to_dynamic_with_resolver(%__MODULE__{op: :avg, arg: token, modifier: nil}, resolve)
       when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], avg(field(x, ^field)))
  end

  defp to_dynamic_with_resolver(%__MODULE__{op: :sum, arg: token, modifier: nil}, resolve)
       when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], sum(field(x, ^field)))
  end

  defp to_dynamic_with_resolver(%__MODULE__{op: :min, arg: token, modifier: nil}, resolve)
       when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], min(field(x, ^field)))
  end

  defp to_dynamic_with_resolver(%__MODULE__{op: :max, arg: token, modifier: nil}, resolve)
       when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], max(field(x, ^field)))
  end

  defp to_dynamic_with_resolver(
         %__MODULE__{
           op: :array_agg,
           arg: token,
           modifier: modifier,
           order_by: order_by,
           filter: filter
         },
         resolve
       )
       when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    value_dynamic = Ecto.Query.dynamic([{^binding, x}], field(x, ^field))

    order_by = List.wrap(order_by)
    distinct? = modifier == :distinct

    order_terms =
      Enum.map(order_by, fn {direction, expr} ->
        expr_dynamic = order_expr_to_dynamic!(expr, resolve)
        order_term_to_dynamic!(direction, expr_dynamic)
      end)

    agg_dynamic = array_agg_dynamic!(value_dynamic, order_terms, distinct?)

    filter_dynamic = filter_to_dynamic!(filter, resolve)

    case filter_dynamic do
      nil ->
        agg_dynamic

      %Ecto.Query.DynamicExpr{} = filter_dynamic ->
        Ecto.Query.dynamic([], fragment("? FILTER (WHERE ?)", ^agg_dynamic, ^filter_dynamic))
    end
  end

  defp to_dynamic_with_resolver(%__MODULE__{} = aggregate, _resolve) do
    raise ArgumentError, "invalid aggregate expression: #{inspect(aggregate)}"
  end

  defp order_expr_to_dynamic!(%Ecto.Query.DynamicExpr{} = dynamic, _resolve), do: dynamic

  defp order_expr_to_dynamic!(fun, resolve) when is_function(fun, 1) do
    case fun.(resolve) do
      %Ecto.Query.DynamicExpr{} = dynamic ->
        dynamic

      other ->
        raise ArgumentError,
              "array_agg/2 expects order_by functions to return an Ecto dynamic expression, got: #{inspect(other)}"
    end
  end

  defp order_expr_to_dynamic!(token, resolve) when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field))
  end

  defp order_expr_to_dynamic!(other, _resolve) do
    raise ArgumentError,
          "array_agg/2 expects order_by expressions to be tokens (atoms/strings), dynamics, or 1-arity functions; got: #{inspect(other)}"
  end

  defp order_term_to_dynamic!(direction, %Ecto.Query.DynamicExpr{} = expr_dynamic)
       when is_atom(direction) do
    case direction do
      :asc ->
        Ecto.Query.dynamic([], fragment("? ASC", ^expr_dynamic))

      :desc ->
        Ecto.Query.dynamic([], fragment("? DESC", ^expr_dynamic))

      :asc_nulls_first ->
        Ecto.Query.dynamic([], fragment("? ASC NULLS FIRST", ^expr_dynamic))

      :asc_nulls_last ->
        Ecto.Query.dynamic([], fragment("? ASC NULLS LAST", ^expr_dynamic))

      :desc_nulls_first ->
        Ecto.Query.dynamic([], fragment("? DESC NULLS FIRST", ^expr_dynamic))

      :desc_nulls_last ->
        Ecto.Query.dynamic([], fragment("? DESC NULLS LAST", ^expr_dynamic))

      other ->
        raise ArgumentError, "unsupported order_by direction for array_agg/2: #{inspect(other)}"
    end
  end

  defp order_term_to_dynamic!(direction, _expr_dynamic) do
    raise ArgumentError,
          "array_agg/2 expects order_by directions to be atoms, got: #{inspect(direction)}"
  end

  defp filter_to_dynamic!(nil, _resolve), do: nil
  defp filter_to_dynamic!([], _resolve), do: nil

  defp filter_to_dynamic!(%Ecto.Query.DynamicExpr{} = dynamic, _resolve), do: dynamic

  defp filter_to_dynamic!(fun, resolve) when is_function(fun, 1) do
    case fun.(resolve) do
      %Ecto.Query.DynamicExpr{} = dynamic ->
        dynamic

      other ->
        raise ArgumentError,
              "array_agg/2 expects filter functions to return an Ecto dynamic expression, got: #{inspect(other)}"
    end
  end

  defp filter_to_dynamic!(filters, resolve) when is_list(filters) or is_tuple(filters) do
    QueryBuilder.Query.Where.build_dynamic_query_with_resolver(nil, filters, [], resolve)
  end

  defp filter_to_dynamic!(other, _resolve) do
    raise ArgumentError,
          "array_agg/2 expects filter to be a keyword list, a list of filters, a filter tuple, a dynamic, or a 1-arity function; got: #{inspect(other)}"
  end

  defp array_agg_dynamic!(value_dynamic, order_terms, distinct?) do
    case {distinct?, order_terms} do
      {false, []} ->
        Ecto.Query.dynamic([], fragment("array_agg(?)", ^value_dynamic))

      {true, []} ->
        Ecto.Query.dynamic([], fragment("array_agg(DISTINCT ?)", ^value_dynamic))

      {false, [t1]} ->
        Ecto.Query.dynamic([], fragment("array_agg(? ORDER BY ?)", ^value_dynamic, ^t1))

      {true, [t1]} ->
        Ecto.Query.dynamic([], fragment("array_agg(DISTINCT ? ORDER BY ?)", ^value_dynamic, ^t1))

      {false, [t1, t2]} ->
        Ecto.Query.dynamic([], fragment("array_agg(? ORDER BY ?, ?)", ^value_dynamic, ^t1, ^t2))

      {true, [t1, t2]} ->
        Ecto.Query.dynamic(
          [],
          fragment("array_agg(DISTINCT ? ORDER BY ?, ?)", ^value_dynamic, ^t1, ^t2)
        )

      {false, [t1, t2, t3]} ->
        Ecto.Query.dynamic(
          [],
          fragment("array_agg(? ORDER BY ?, ?, ?)", ^value_dynamic, ^t1, ^t2, ^t3)
        )

      {true, [t1, t2, t3]} ->
        Ecto.Query.dynamic(
          [],
          fragment("array_agg(DISTINCT ? ORDER BY ?, ?, ?)", ^value_dynamic, ^t1, ^t2, ^t3)
        )

      {false, [t1, t2, t3, t4]} ->
        Ecto.Query.dynamic(
          [],
          fragment("array_agg(? ORDER BY ?, ?, ?, ?)", ^value_dynamic, ^t1, ^t2, ^t3, ^t4)
        )

      {true, [t1, t2, t3, t4]} ->
        Ecto.Query.dynamic(
          [],
          fragment(
            "array_agg(DISTINCT ? ORDER BY ?, ?, ?, ?)",
            ^value_dynamic,
            ^t1,
            ^t2,
            ^t3,
            ^t4
          )
        )

      {false, [t1, t2, t3, t4, t5]} ->
        Ecto.Query.dynamic(
          [],
          fragment("array_agg(? ORDER BY ?, ?, ?, ?, ?)", ^value_dynamic, ^t1, ^t2, ^t3, ^t4, ^t5)
        )

      {true, [t1, t2, t3, t4, t5]} ->
        Ecto.Query.dynamic(
          [],
          fragment(
            "array_agg(DISTINCT ? ORDER BY ?, ?, ?, ?, ?)",
            ^value_dynamic,
            ^t1,
            ^t2,
            ^t3,
            ^t4,
            ^t5
          )
        )

      {_distinct?, order_terms} ->
        raise ArgumentError,
              "array_agg/2 supports up to 5 order_by terms, got: #{inspect(length(order_terms))}"
    end
  end

  def comparison_fun(%__MODULE__{} = aggregate, operator, value, operator_opts \\ []) do
    fn resolve ->
      aggregate
      |> compare_dynamic(resolve, operator, value, operator_opts)
    end
  end

  def normalize_having_filters(filters) when is_list(filters) do
    Enum.map(filters, fn
      %__MODULE__{} = aggregate ->
        raise ArgumentError,
              "invalid having filter: aggregate expression #{inspect(aggregate)} must be compared; " <>
                "expected `{aggregate, value}` or `{aggregate, operator, value}` (e.g. `{count(:id), :gt, 2}`)"

      {%__MODULE__{} = aggregate, value} ->
        comparison_fun(aggregate, :eq, value)

      {%__MODULE__{} = aggregate, operator, value} when is_atom(operator) ->
        comparison_fun(aggregate, operator, value)

      {%__MODULE__{} = aggregate, operator, _value} ->
        raise ArgumentError,
              "invalid having filter: aggregate expression #{inspect(aggregate)} expects an atom operator; " <>
                "got: #{inspect(operator)}"

      {%__MODULE__{} = aggregate, operator, value, operator_opts} ->
        cond do
          not is_atom(operator) ->
            raise ArgumentError,
                  "invalid having filter: aggregate expression #{inspect(aggregate)} expects an atom operator; " <>
                    "got: #{inspect(operator)}"

          not is_list(operator_opts) ->
            raise ArgumentError,
                  "invalid having filter: aggregate expression #{inspect(aggregate)} expects a list of operator options; " <>
                    "got: #{inspect(operator_opts)}"

          true ->
            comparison_fun(aggregate, operator, value, operator_opts)
        end

      {%__MODULE__{} = aggregate, _a, _b, _c, _d} = tuple ->
        raise ArgumentError,
              "invalid having filter: aggregate expression #{inspect(aggregate)} has an unsupported shape; " <>
                "got: #{inspect(tuple)}"

      other ->
        other
    end)
  end

  def normalize_having_or_filters(or_filters) when is_list(or_filters) do
    Enum.map(or_filters, fn
      {:or, group} when is_list(group) ->
        {:or, normalize_having_filters(group)}

      other ->
        other
    end)
  end

  defp compare_dynamic(%__MODULE__{} = aggregate, resolve, operator, value, operator_opts) do
    if operator_opts != [] do
      raise ArgumentError,
            "aggregate filter does not support operator options, got: #{inspect(operator_opts)}"
    end

    op = normalize_operator(operator)
    agg_dynamic = to_dynamic_with_resolver(aggregate, resolve)

    case {op, value} do
      {:eq, nil} ->
        Ecto.Query.dynamic(is_nil(^agg_dynamic))

      {:ne, nil} ->
        Ecto.Query.dynamic(not is_nil(^agg_dynamic))

      {:eq, _} ->
        Ecto.Query.dynamic(^agg_dynamic == ^value)

      {:ne, _} ->
        Ecto.Query.dynamic(^agg_dynamic != ^value)

      {:gt, _} ->
        Ecto.Query.dynamic(^agg_dynamic > ^value)

      {:ge, _} ->
        Ecto.Query.dynamic(^agg_dynamic >= ^value)

      {:lt, _} ->
        Ecto.Query.dynamic(^agg_dynamic < ^value)

      {:le, _} ->
        Ecto.Query.dynamic(^agg_dynamic <= ^value)

      {:in, _} ->
        Ecto.Query.dynamic(^agg_dynamic in ^value)

      {:not_in, _} ->
        Ecto.Query.dynamic(^agg_dynamic not in ^value)

      {other, _} ->
        raise ArgumentError,
              "unsupported aggregate filter operator #{inspect(other)} for #{inspect(aggregate)}"
    end
  end

  defp normalize_operator(op) when op in [:eq, :equal_to], do: :eq
  defp normalize_operator(op) when op in [:ne, :other_than], do: :ne
  defp normalize_operator(op) when op in [:gt, :greater_than], do: :gt
  defp normalize_operator(op) when op in [:ge, :greater_than_or_equal_to], do: :ge
  defp normalize_operator(op) when op in [:lt, :less_than], do: :lt
  defp normalize_operator(op) when op in [:le, :less_than_or_equal_to], do: :le
  defp normalize_operator(:in), do: :in
  defp normalize_operator(:not_in), do: :not_in
  defp normalize_operator(other), do: other
end
