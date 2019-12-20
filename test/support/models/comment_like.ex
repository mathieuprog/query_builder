defmodule QueryBuilder.CommentLike do
  use QueryBuilder, assoc_fields: [:article, :comment, :user]
  use Ecto.Schema

  schema "comment_likes" do
    belongs_to(:article, QueryBuilder.Article)
    belongs_to(:comment, QueryBuilder.Comment)
    belongs_to(:user, QueryBuilder.User)

    timestamps(type: :utc_datetime)
  end
end
