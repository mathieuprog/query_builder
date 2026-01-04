defmodule QueryBuilder.Query.Distinct do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def distinct(ecto_query, assoc_list, value) do
    distinct_expr = build_distinct_expr!(ecto_query, assoc_list, value)
    Ecto.Query.distinct(ecto_query, ^distinct_expr)
  end

  defp build_distinct_expr!(_ecto_query, _assoc_list, value) when is_boolean(value), do: value

  defp build_distinct_expr!(_ecto_query, _assoc_list, %Ecto.Query.DynamicExpr{} = dynamic),
    do: dynamic

  defp build_distinct_expr!(ecto_query, assoc_list, value)
       when is_atom(value) or is_binary(value) do
    token_to_dynamic(ecto_query, assoc_list, value)
  end

  defp build_distinct_expr!(ecto_query, assoc_list, {direction, expr})
       when is_atom(direction) do
    build_distinct_expr!(ecto_query, assoc_list, [{direction, expr}])
  end

  defp build_distinct_expr!(ecto_query, assoc_list, values) when is_list(values) do
    values
    |> Enum.map(&build_distinct_order_expr!(ecto_query, assoc_list, &1))
    |> List.flatten()
  end

  defp build_distinct_expr!(_ecto_query, _assoc_list, value) do
    raise ArgumentError,
          "distinct expects a boolean or order_by-like expressions (tokens, dynamics, lists/keyword lists); got: #{inspect(value)}"
  end

  defp build_distinct_order_expr!(ecto_query, assoc_list, {direction, expr})
       when is_atom(direction) do
    resolved_expr =
      case expr do
        %Ecto.Query.DynamicExpr{} = dynamic ->
          dynamic

        fun when is_function(fun, 1) ->
          fun.(&find_field_and_binding_from_token(ecto_query, assoc_list, &1))

        token when is_atom(token) or is_binary(token) ->
          token_to_dynamic(ecto_query, assoc_list, token)

        other ->
          raise ArgumentError,
                "distinct expression #{inspect({direction, other})} is invalid; expected a token, dynamic, or 1-arity function"
      end

    {direction, resolved_expr}
  end

  defp build_distinct_order_expr!(ecto_query, assoc_list, expr) do
    resolved_expr =
      case expr do
        %Ecto.Query.DynamicExpr{} = dynamic ->
          dynamic

        fun when is_function(fun, 1) ->
          fun.(&find_field_and_binding_from_token(ecto_query, assoc_list, &1))

        token when is_atom(token) or is_binary(token) ->
          token_to_dynamic(ecto_query, assoc_list, token)

        other ->
          raise ArgumentError,
                "distinct expression #{inspect(other)} is invalid; expected a token, dynamic, or 1-arity function"
      end

    resolved_expr
  end

  defp token_to_dynamic(ecto_query, assoc_list, token) do
    {field, binding} = find_field_and_binding_from_token(ecto_query, assoc_list, token)
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field))
  end
end
