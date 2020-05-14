defmodule QueryBuilder.Article do
  use QueryBuilder,
    assoc_fields: [:author, :publisher, :comments, :article_likes, :article_stars]

  use Ecto.Schema

  schema "articles" do
    field(:title, :string)
    field(:tags, {:array, :string})

    belongs_to(:author, QueryBuilder.User, foreign_key: :author_id)
    belongs_to(:publisher, QueryBuilder.User, foreign_key: :publisher_id)
    has_many(:comments, QueryBuilder.Comment)
    has_many(:article_likes, QueryBuilder.ArticleLike)
    has_many(:article_stars, QueryBuilder.ArticleStar)

    timestamps(type: :utc_datetime)
  end
end
