import Config

config :logger, level: :warning # set to :debug to view SQL queries in logs

config :query_builder,
  ecto_repos: [QueryBuilder.Repo]

config :query_builder, QueryBuilder.Repo,
  port: System.get_env("POSTGRES_PORT", "5432") |> String.to_integer(),
  username: System.get_env("POSTGRES_USER", "postgres"),
  password: System.get_env("POSTGRES_PASSWORD", "postgres"),
  database: System.get_env("POSTGRES_DATABASE", "query_builder_test"),
  hostname: System.get_env("POSTGRES_HOST", "localhost"),
  pool: Ecto.Adapters.SQL.Sandbox,
  priv: "test/support"
