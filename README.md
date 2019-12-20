# Query Builder

Query Builder allows you to build and compose Ecto queries based on data.

Its primary goal is to allow Context functions to receive filters and options to
avoid having to create many different functions for every combination of filters
and options:

```elixir
Blog.list_articles(preload: [:comments], order_by: [title: :asc])
Blog.list_articles(preload: [:category, comments: :user])
```

The calling code (e.g. the Controllers), can now retrieve the list of articles with
different options. In some part of the application, the category is needed; in other
parts it is not, however the articles must be sorted based on their title, etc.

See `QueryBuilder.from_list/2` below.

## Examples

```elixir
User
|> QueryBuilder.where(firstname: "John")
|> QueryBuilder.where([{:age, :gt, 30}, city: "Anytown"])
|> QueryBuilder.order_by(lastname: :asc)
|> QueryBuilder.preload([:role, authored_articles: :comments])
|> Repo.all()
```

Filtering on associations is supported:

```elixir
User
|> QueryBuilder.where(:role, name@role: "admin")
|> Repo.all()
```

```elixir
User
|> QueryBuilder.where([role: :permission], name@permission: "delete")
|> Repo.all()
```

```elixir
Article
|> QueryBuilder.where(:author, id@author: author_id)
|> QueryBuilder.where([:author, :comments], {:logged_at@author, :lt, :inserted_at@comments})
|> QueryBuilder.preload(:comments)
|> Repo.all()
```

## Usage

Add `use QueryBuilder` in your schema:

```elixir
defmodule MyApp.User do
  use Ecto.Schema
  use QueryBuilder

  schema "users" do
    # code
  end

  # code
end
```

You may also specify the schema's associations to `QueryBuilder` in order to remedy
a few limitations when building queries:

```elixir
defmodule MyApp.User do
  use Ecto.Schema
  use QueryBuilder, assoc_fields: [:role, :articles]

  schema "users" do
    # code
    belongs_to :role, MyApp.Role
    has_many :articles, MyApp.Article
  end

  # code
end
```

Currently supported operations are:

`QueryBuilder.where/2`

```elixir
QueryBuilder.where(query, firstname: "John")
```

`QueryBuilder.where/3`

```elixir
QueryBuilder.where(query, [role: :permissions], name@permissions: :write)
```

`QueryBuilder.order_by/2`

```elixir
QueryBuilder.order_by(query, lasstname: :asc, firstname: :asc)
```

`QueryBuilder.order_by/3`

```elixir
QueryBuilder.order_by(query, :articles, title@articles: :asc)
```

`QueryBuilder.preload/2`

```elixir
QueryBuilder.preload(query, [role: :permissions, articles: [:stars, comments: :user]])
```

`QueryBuilder.join/3`

```elixir
QueryBuilder.join(query, :articles, :left)
```

`QueryBuilder.from_list/2`

```elixir
QueryBuilder.from_list(query, [
  where: [name: "John", city: "Anytown"],
  preload: [articles: :comments]
])
```

## Installation

Add `query_builder` for Elixir as a dependency in your `mix.exs` file:

```elixir
def deps do
  [
    {:query_builder, "~> 0.1.0"}
  ]
end
```

## HexDocs

HexDocs documentation can be found at [https://hexdocs.pm/query_builder](https://hexdocs.pm/query_builder).
