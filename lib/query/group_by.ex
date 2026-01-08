defmodule QueryBuilder.Query.GroupBy do
  @moduledoc false

  require Ecto.Query
  import QueryBuilder.Utils

  def group_by(ecto_query, assoc_list, expr) do
    group_by_exprs = build_group_by_exprs!(assoc_list, expr)
    Ecto.Query.group_by(ecto_query, ^group_by_exprs)
  end

  defp build_group_by_exprs!(assoc_list, expr) when is_list(expr) do
    if Keyword.keyword?(expr) do
      raise ArgumentError,
            "group_by does not accept a keyword list; " <>
              "pass a token or a list of tokens/expressions, got: #{inspect(expr)}"
    end

    expr
    |> Enum.flat_map(&build_group_by_exprs!(assoc_list, &1))
  end

  defp build_group_by_exprs!(assoc_list, expr) when is_function(expr, 1) do
    expr
    |> call_group_by_fun(assoc_list)
    |> build_group_by_exprs!(assoc_list)
  end

  defp build_group_by_exprs!(_assoc_list, %Ecto.Query.DynamicExpr{} = dynamic),
    do: [dynamic]

  defp build_group_by_exprs!(assoc_list, token) when is_atom(token) or is_binary(token) do
    [token_to_dynamic(assoc_list, token)]
  end

  defp build_group_by_exprs!(_assoc_list, expr) do
    raise ArgumentError,
          "group_by expects a token, a list of tokens/expressions, a dynamic, or a 1-arity function; got: #{inspect(expr)}"
  end

  defp call_group_by_fun(fun, assoc_list) do
    fun.(&find_field_and_binding_from_token(assoc_list, &1))
  end

  defp token_to_dynamic(assoc_list, token) do
    {field, binding} = find_field_and_binding_from_token(assoc_list, token)
    Ecto.Query.dynamic([{^binding, x}], field(x, ^field))
  end
end
