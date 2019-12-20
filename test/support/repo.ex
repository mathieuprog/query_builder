defmodule QueryBuilder.Repo do
  use Ecto.Repo,
    otp_app: :query_builder,
    adapter: Ecto.Adapters.Postgres
end
