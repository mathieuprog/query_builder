defmodule QueryBuilder.CustomPkUser do
  use QueryBuilder
  use Ecto.Schema

  @primary_key {:user_id, :integer, autogenerate: false}
  schema "custom_pk_users" do
    field(:name, :string)
    timestamps(type: :utc_datetime)
  end
end
