defmodule QueryBuilder.Comment do
  use QueryBuilder
  use Ecto.Schema

  schema "comments" do
    field(:title, :string)
    field(:body, :string)

    belongs_to(:user, QueryBuilder.User)
    belongs_to(:article, QueryBuilder.Article)
    has_many(:comment_likes, QueryBuilder.CommentLike)
    has_many(:comment_stars, QueryBuilder.CommentStar)

    timestamps(type: :utc_datetime)
  end
end
