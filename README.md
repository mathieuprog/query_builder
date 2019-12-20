# Query Builder

Query Builder allows you to build and compose Ecto queries based on data.

```elixir
User
|> QueryBuilder.where(firstname: "John")
|> QueryBuilder.where([{:age, :gt, 30}, city: "Anytown"])
|> QueryBuilder.order_by(lastname: :asc)
|> QueryBuilder.preload([:role, authored_articles: :comments])
|> Repo.all()
```

This allows writing queries more concisely, without having to deal with bindings
and macros.

Its primary goal is to allow Context functions to receive a set of filters and
options:

```elixir
Blog.list_articles(preload: [:comments], order_by: [title: :asc])
Blog.list_articles(preload: [:category, comments: :user])
```

This avoids having to create many different functions in the Context for every
combination of filters and options, or to create one general function that does
too much to satisfy all the consumers.

The calling code (e.g. the Controllers), can now retrieve the list of articles with
different options. In some part of the application, the category is needed; in other
parts it is not; sometimes the articles must be sorted based on their title;
other times it doesn't matter, etc.

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
some limitations when building queries:

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
QueryBuilder.order_by(query, lastname: :asc, firstname: :asc)
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

The `QueryBuilder.from_list/2` function was the main motivation behind the writing
of this library. As explained above, it allows to add querying options to the
Context functions. Example:

```elixir
defmodule MyApp.Blog do
  alias MyApp.Blog.Article

  def get_article_by_id(id, opts \\ []) do
    QueryBuilder.where(Article, id: id)
    |> QueryBuilder.from_list(opts)
    |> Repo.one!()
  end
end
```

The function get now be called as follows (for instance, from a Controller):

```elixir
Blog.get_article_by_id(id, preload: [:comments])

Blog.get_article_by_id(id, preload: [:likes])

Blog.get_article_by_id(
  id,
  order_by: [:comments, inserted_at@comments: :desc],
  preload: [comments: :user]
)
```

## Installation

Add `query_builder` for Elixir as a dependency in your `mix.exs` file:

```elixir
def deps do
  [
    {:query_builder, "~> 0.2.0"}
  ]
end
```

## HexDocs

HexDocs documentation can be found at [https://hexdocs.pm/query_builder](https://hexdocs.pm/query_builder).