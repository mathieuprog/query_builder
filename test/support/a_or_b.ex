defmodule QueryBuilder.AorB do
  @moduledoc false

  use Ecto.Type

  @impl true
  def type, do: :string

  @impl true
  def cast(str) when is_binary(str), do: {:ok, str}
  def cast(atm) when is_atom(atm), do: {:ok, Atom.to_string(atm)}
  def cast(_), do: :error

  @impl true
  def load(str) when is_binary(str), do: {:ok, str}
  def load(_), do: :error

  @impl true
  def dump(str) when is_binary(str), do: {:ok, str}
  def dump(_), do: :error
end
