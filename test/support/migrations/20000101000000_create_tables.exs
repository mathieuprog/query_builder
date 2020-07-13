defmodule QueryBuilder.CreateTables do
  use Ecto.Migration

  def change do
    create table(:roles) do
      add(:name, :string)

      timestamps()
    end

    create table(:permissions) do
      add(:name, :string)
      add(:role_id, references(:roles))

      timestamps()
    end

    create table(:users) do
      add(:name, :string)
      add(:email, :string)
      add(:nickname, :string)
      add(:deleted, :boolean)
      add(:role_id, references(:roles))

      timestamps()
    end

    create table(:groups) do
      add(:name, :string)

      timestamps()
    end

    create table(:users_groups) do
      add(:name, :string)
      add(:user_id, references(:users))
      add(:group_id, references(:groups))

      timestamps()
    end

    create table(:categories) do
      add(:name, :string)

      timestamps()
    end

    create table(:articles) do
      add(:title, :string)
      add(:tags, {:array, :string})
      add(:author_id, references(:users))
      add(:publisher_id, references(:users))
      add(:category_id, references(:categories))

      timestamps()
    end

    create table(:comments) do
      add(:title, :string)
      add(:body, :string)
      add(:article_id, references(:articles))
      add(:user_id, references(:users))

      timestamps()
    end

    create table(:article_stars) do
      add(:article_id, references(:articles))
      add(:user_id, references(:users))

      timestamps()
    end

    create table(:article_likes) do
      add(:article_id, references(:articles))
      add(:user_id, references(:users))

      timestamps()
    end

    create table(:comment_stars) do
      add(:comment_id, references(:comments))
      add(:article_id, references(:articles))
      add(:user_id, references(:users))

      timestamps()
    end

    create table(:comment_likes) do
      add(:comment_id, references(:comments))
      add(:article_id, references(:articles))
      add(:user_id, references(:users))

      timestamps()
    end
  end
end
