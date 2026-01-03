defmodule QueryBuilder.AddUserSettingsAndEvents do
  use Ecto.Migration

  def change do
    create table(:user_settings) do
      add(:theme, :string)
      add(:user_id, references(:users))

      timestamps()
    end

    create(unique_index(:user_settings, [:user_id]))

    create table(:events) do
      add(:kind, :string)
      add(:article_id, references(:articles))
      add(:comment_id, references(:comments))

      timestamps()
    end
  end
end
