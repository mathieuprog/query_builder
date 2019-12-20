defmodule QueryBuilder.Factory do
  use ExMachina.Ecto, repo: QueryBuilder.Repo

  def role_factory do
    %QueryBuilder.Role{
      name: sequence(:name, ["admin", "author", "publisher", "reader"])
    }
  end

  def permission_factory do
    %QueryBuilder.Permission{
      name: sequence(:name, ["read", "write", "publish", "delete"]),
      role: build(:role)
    }
  end

  def user_factory do
    %QueryBuilder.User{
      name: "Jane Smith",
      email: sequence(:email, &"email-#{&1}@example.com"),
      role: build(:role)
    }
  end

  def article_factory do
    title = sequence(:title, &"Article #{&1}")

    %QueryBuilder.Article{
      title: title,
      author: build(:user),
      publisher: build(:user)
    }
  end

  def article_like_factory do
    %QueryBuilder.ArticleLike{
      article: build(:article),
      user: build(:user)
    }
  end

  def article_star_factory do
    %QueryBuilder.ArticleStar{
      article: build(:article),
      user: build(:user)
    }
  end

  def comment_factory do
    %QueryBuilder.Comment{
      title: "It's great!",
      article: build(:article),
      user: build(:user)
    }
  end

  def comment_like_factory do
    %QueryBuilder.CommentLike{
      comment: build(:comment),
      article: build(:article),
      user: build(:user)
    }
  end

  def comment_star_factory do
    %QueryBuilder.CommentStar{
      comment: build(:comment),
      article: build(:article),
      user: build(:user)
    }
  end
end
