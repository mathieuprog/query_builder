# Query Builder

Query Builder allows you to build and compose Ecto queries based on data.

```elixir
User
|> QueryBuilder.where(firstname: "John")
|> QueryBuilder.where([{:age, :gt, 30}, city: "Anytown"])
|> QueryBuilder.order_by(asc: :lastname)
|> QueryBuilder.preload([:role, authored_articles: :comments])
|> QueryBuilder.offset(20)
|> QueryBuilder.limit(10)
|> Repo.all()
```

This allows writing queries more concisely, without having to deal with bindings
and macros.

Its primary goal is to allow Context functions to receive a set of filters and
options:

```elixir
Blog.list_articles(preload: :comments, order_by: [asc: :title])
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
|> QueryBuilder.order_by(asc: :lastname)
|> QueryBuilder.preload([:role, authored_articles: :comments])
|> QueryBuilder.offset(20)
|> QueryBuilder.limit(10)
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
|> QueryBuilder.where([role: :permissions], name@permissions: "delete")
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

Currently, supported operations are:

`QueryBuilder.where/2`

```elixir
QueryBuilder.where(query, firstname: "John")
```

`QueryBuilder.where/4`

```elixir
QueryBuilder.where(query, [role: :permissions], name@permissions: :write)
```

Above `where` functions support different filter operations, for instance:
```elixir
QueryBuilder.where(query, {:age, :greater_than, 18})
```

Supported filter operations are:
* `:equal_to` (or `:eq`)
* `:other_than` (or `:ne`)
* `:greater_than` (or `:gt`)
* `:greater_than_or_equal_to` (or `:ge`)
* `:less_than` (or `:lt`)
* `:less_than_or_equal_to` (or `:le`)
* `:like`
* `:ilike`
* `:starts_with`
* `:ends_with`
* `:contains`

Array inclusion checking:
* `:in`
* `:not_in`
* `:include`
* `:exclude`

Note that `:starts_with`, `:ends_with` and `:contains` operations can be written using `:like`, but offer a more declarative style and are safer, as they escape the `%` and `_` characters for you. You may also perform case insensitive searchs using these functions. Example:

```elixir
QueryBuilder.where({:name, :starts_with, "jo"})
```

```elixir
QueryBuilder.where({:name, :starts_with, "jo", case: :insensitive}) # `:i` will also work
```

When using `:like` or `:ilike`, make sure to escape `%` and `_` characters properly.

You may also add `OR` clauses through `QueryBuilder.where/4`'s fourth argument:

```elixir
QueryBuilder.where(query, [], [name: "John"], or: [name: "Alice", age: 42], or: [name: "Bob"])
```

`QueryBuilder.maybe_where/3` and `QueryBuilder.maybe_where/5`

```elixir
query
|> QueryBuilder.maybe_where(some_condition, name: "Alice")
```

The above will run `where/2` if the given condition is met.

`QueryBuilder.order_by/2`

```elixir
QueryBuilder.order_by(query, asc: :lastname, asc: :firstname)
```

`QueryBuilder.order_by/3`

```elixir
QueryBuilder.order_by(query, :articles, asc: :title@articles)
```

`QueryBuilder.preload/2`

```elixir
QueryBuilder.preload(query, [role: :permissions, articles: [:stars, comments: :user]])
```

`QueryBuilder.left_join/4`

```elixir
QueryBuilder.left_join(query, :articles, title@articles: "Foo", or: [title@articles: "Bar"])
```

`QueryBuilder.offset/2`

```elixir
QueryBuilder.offset(query, 10)
```

`QueryBuilder.limit/2`

```elixir
QueryBuilder.limit(query, 10)
```

`QueryBuilder.from_list/2`

```elixir
QueryBuilder.from_list(query, [
  where: [name: "John", city: "Anytown"],
  preload: [articles: :comments],
  order_by: {:articles, asc: :title@articles},
  limit: 20,
  offset: 10
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

The function can now be called as follows (for instance, from a Controller):

```elixir
Blog.get_article_by_id(id, preload: [:comments])

Blog.get_article_by_id(id, preload: [:likes])

Blog.get_article_by_id(
  id,
  order_by: {:comments, desc: :inserted_at@comments},
  preload: [comments: :user]
)
```

## Extending Query Functions
In the event that you want to extend QueryBuilder's functionality to include custom app specific query functions, there's the `QueryBuilder.Extension` module to facilitate that.  You can create a module with your app specific query functionality and `use` the `QueryBuilder.Extension` module to inject all `QueryBuilder` functions into your custom module.  Any custom query functions added to your custom module are included in `QueryBuilder.from_list/2`.

For example:

```elixir
defmodule MyApp.QueryBuilder do
  use QueryBuilder.Extension

  defmacro __using__(opts) do
    quote do
      require QueryBuilder
      QueryBuilder.__using__(unquote(opts))
    end
  end

  # Add app specific query functions here...

  def where_initcap(query, field, value) do
    text_equals_condition = fn field, value, get_binding_fun ->
      {field, binding} = get_binding_fun.(field)
      Ecto.Query.dynamic([{^binding, x}], fragment("initcap(?)", ^value) == field(x, ^field))
    end

    query
    |> where(&text_equals_condition.(field, value, &1))
  end
end

defmodule MyApp.Accounts.User do
  use MyApp.QueryBuilder

  schema "users" do
    field :name, :string
    field :active, :boolean
  end
end

defmodule MyApp.Accounts do
  alias MyApp.QueryBuilder, as: QB

  def list_users(opts \\ []) do
    # Query list can include custom query functions as well:
    # [where_initcap: {:name, "john"}, where: {:active, true}]
    MyApp.Accounts.User
    |> QB.from_list(opts)
    |> Repo.all()
  end
end
```

## Special Considerations

With the auto-binding functionality offered by Query Builder, you can specify field comparisons in queries using the special atom value syntax: `:<field_name>@self`. This way Query Builder understands the intent is to compare fields vs a raw value.  For example:

```elixir
users_where_name_matches_nickname =
  User
  |> QueryBuilder.where({:name, :eq, :nickname@self})
  |> Repo.all()
```

would resolve to a query along the following form:

```sql
select * from users where users.name = users.nickname
```


## Installation

Add `query_builder` for Elixir as a dependency in your `mix.exs` file:

```elixir
def deps do
  [
    {:query_builder, "~> 1.0.1"}
  ]
end
```

## HexDocs

HexDocs documentation can be found at [https://hexdocs.pm/query_builder](https://hexdocs.pm/query_builder).
