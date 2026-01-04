defmodule QueryBuilder.Aggregate do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  defstruct [:op, :arg, :modifier]

  def to_dynamic(ecto_query, assoc_list, %__MODULE__{} = aggregate) do
    to_dynamic(aggregate, &find_field_and_binding_from_token(ecto_query, assoc_list, &1))
  end

  def to_dynamic(%__MODULE__{op: :count, arg: nil}, _resolve) do
    Ecto.Query.dynamic([], count())
  end

  def to_dynamic(%__MODULE__{op: :count, arg: token, modifier: :distinct}, resolve)
      when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], count(field(x, ^field), :distinct))
  end

  def to_dynamic(%__MODULE__{op: :count, arg: token, modifier: nil}, resolve)
      when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], count(field(x, ^field)))
  end

  def to_dynamic(%__MODULE__{op: :avg, arg: token, modifier: nil}, resolve)
      when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], avg(field(x, ^field)))
  end

  def to_dynamic(%__MODULE__{op: :sum, arg: token, modifier: nil}, resolve)
      when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], sum(field(x, ^field)))
  end

  def to_dynamic(%__MODULE__{op: :min, arg: token, modifier: nil}, resolve)
      when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], min(field(x, ^field)))
  end

  def to_dynamic(%__MODULE__{op: :max, arg: token, modifier: nil}, resolve)
      when is_atom(token) or is_binary(token) do
    {field, binding} = resolve.(token)
    Ecto.Query.dynamic([{^binding, x}], max(field(x, ^field)))
  end

  def to_dynamic(%__MODULE__{} = aggregate, _resolve) do
    raise ArgumentError, "invalid aggregate expression: #{inspect(aggregate)}"
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
    agg_dynamic = to_dynamic(aggregate, resolve)

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
