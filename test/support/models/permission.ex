defmodule QueryBuilder.Permission do
  use QueryBuilder
  use Ecto.Schema

  schema "permissions" do
    field(:name, :string)

    belongs_to(:role, QueryBuilder.Role)

    timestamps(type: :utc_datetime)
  end
end
