defmodule QueryBuilder.FromOpts.Dispatch.Boundary do
  @moduledoc false

  alias QueryBuilder.FromOpts.Dispatch.Boundary.Apply
  alias QueryBuilder.FromOpts.Dispatch.Boundary.Validation

  defdelegate apply_operation!(query, operation, args), to: Apply
  defdelegate validate_arguments!(operation, arguments), to: Validation
end
