defmodule QueryBuilder.Event do
  use QueryBuilder

  use Ecto.Schema

  schema "events" do
    field(:kind, :string)

    belongs_to(:article, QueryBuilder.Article)
    belongs_to(:comment, QueryBuilder.Comment)

    timestamps(type: :utc_datetime)
  end
end
