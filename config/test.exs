import Config

config :logger, level: :warn # set to :debug to view SQL queries in logs

config :query_builder,
  ecto_repos: [QueryBuilder.Repo]

config :query_builder, QueryBuilder.Repo,
  username: "postgres",
  password: "postgres",
  database: "query_builder_test",
  hostname: "localhost",
  pool: Ecto.Adapters.SQL.Sandbox,
  priv: "test/support"
