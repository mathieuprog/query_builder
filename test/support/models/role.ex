defmodule QueryBuilder.Role do
  use QueryBuilder
  use Ecto.Schema

  schema "roles" do
    field(:name, :string)

    has_many(:users, QueryBuilder.User)
    has_many(:permissions, QueryBuilder.Permission)

    timestamps(type: :utc_datetime)
  end
end
