{:ok, _} = Application.ensure_all_started(:ex_machina)
{:ok, _} = QueryBuilder.Repo.start_link()

ExUnit.start()

Ecto.Adapters.SQL.Sandbox.mode(QueryBuilder.Repo, :manual)
