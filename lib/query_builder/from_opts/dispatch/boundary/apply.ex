defmodule QueryBuilder.FromOpts.Dispatch.Boundary.Apply do
  @moduledoc false

  def apply_operation!(query, operation, [arg]) do
    case operation do
      :where ->
        QueryBuilder.where(query, arg)

      :where_any ->
        QueryBuilder.where_any(query, arg)

      :order_by ->
        QueryBuilder.order_by(query, arg)

      :limit ->
        QueryBuilder.limit(query, arg)

      :offset ->
        QueryBuilder.offset(query, arg)

      other ->
        raise ArgumentError, "internal error: unsupported boundary op: #{inspect(other)}"
    end
  end

  def apply_operation!(_query, operation, args) do
    raise ArgumentError,
          "internal error: boundary op #{inspect(operation)} expected one argument, got: #{inspect(args)}"
  end
end
