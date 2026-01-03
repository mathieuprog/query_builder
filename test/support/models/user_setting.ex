defmodule QueryBuilder.UserSetting do
  use QueryBuilder

  use Ecto.Schema

  schema "user_settings" do
    field(:theme, :string)

    belongs_to(:user, QueryBuilder.User)

    timestamps(type: :utc_datetime)
  end
end
