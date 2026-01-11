defmodule QueryBuilder.ExtensionDelegatesTest do
  use ExUnit.Case, async: true

  defmodule ExtensionModule do
    use QueryBuilder.Extension
  end

  test "extension exposes the full QueryBuilder function surface" do
    excluded? = fn {fun, _arity} ->
      fun == :module_info or String.starts_with?(Atom.to_string(fun), "__")
    end

    query_builder_funs =
      QueryBuilder.__info__(:functions)
      |> Enum.reject(excluded?)
      |> MapSet.new()

    extension_funs =
      ExtensionModule.__info__(:functions)
      |> Enum.reject(excluded?)
      |> MapSet.new()

    missing =
      query_builder_funs
      |> MapSet.difference(extension_funs)
      |> MapSet.to_list()
      |> Enum.sort()

    assert missing == []
  end
end
