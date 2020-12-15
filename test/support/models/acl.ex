defmodule QueryBuilder.Acl do
  use QueryBuilder, assoc_fields: [:grantee, :grantor]
  use Ecto.Schema

  schema "acl" do
    belongs_to(:grantee, QueryBuilder.User)
    belongs_to(:grantor, QueryBuilder.User)

    timestamps(type: :utc_datetime)
  end
end
