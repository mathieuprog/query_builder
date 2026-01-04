defmodule QueryBuilder.CompositeUser do
  use QueryBuilder
  use Ecto.Schema

  @primary_key false
  schema "composite_users" do
    field(:tenant_id, :integer, primary_key: true)
    field(:user_id, :integer, primary_key: true)
    field(:name, :string)

    timestamps(type: :utc_datetime)
  end
end
