defmodule QueryBuilder.User do
  use QueryBuilder, assoc_fields: [:role, :authored_articles, :published_articles]
  use Ecto.Schema

  schema "users" do
    field(:name, :string)
    field(:email, :string)
    field(:nickname, :string)
    field(:deleted, :boolean)

    belongs_to(:role, QueryBuilder.Role)
    has_many(:authored_articles, QueryBuilder.Article, foreign_key: :author_id)
    has_many(:published_articles, QueryBuilder.Article, foreign_key: :publisher_id)
    has_many(:acl_grantee, QueryBuilder.Acl, foreign_key: :grantee_id)
    has_many(:acl_grantor, QueryBuilder.Acl, foreign_key: :grantor_id)

    timestamps(type: :utc_datetime)
  end
end
