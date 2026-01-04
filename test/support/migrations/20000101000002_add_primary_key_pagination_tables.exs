defmodule QueryBuilder.AddPrimaryKeyPaginationTables do
  use Ecto.Migration

  def change do
    create table(:custom_pk_users, primary_key: false) do
      add(:user_id, :integer, primary_key: true)
      add(:name, :string, null: false)
      timestamps(type: :utc_datetime)
    end

    create table(:composite_users, primary_key: false) do
      add(:tenant_id, :integer, null: false)
      add(:user_id, :integer, null: false)
      add(:name, :string, null: false)
      timestamps(type: :utc_datetime)
    end

    create(unique_index(:composite_users, [:tenant_id, :user_id]))
  end
end
