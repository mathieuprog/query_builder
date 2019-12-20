defmodule QueryBuilder.ArticleStar do
  use QueryBuilder, assoc_fields: [:article, :user]
  use Ecto.Schema

  schema "article_stars" do
    belongs_to(:article, QueryBuilder.Article)
    belongs_to(:user, QueryBuilder.User)

    timestamps(type: :utc_datetime)
  end
end
