defmodule QueryBuilder.Permission do
  use QueryBuilder, assoc_fields: [:role]
  use Ecto.Schema

  schema "permissions" do
    field(:name, :string)

    belongs_to(:role, QueryBuilder.Role)

    timestamps(type: :utc_datetime)
  end
end
