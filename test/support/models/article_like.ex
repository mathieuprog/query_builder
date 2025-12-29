defmodule QueryBuilder.ArticleLike do
  use QueryBuilder
  use Ecto.Schema

  schema "article_likes" do
    belongs_to(:article, QueryBuilder.Article)
    belongs_to(:user, QueryBuilder.User)

    timestamps(type: :utc_datetime)
  end
end
