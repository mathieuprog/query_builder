defmodule QueryBuilder.Authorizer do
  @moduledoc false

  alias QueryBuilder.{Acl, Article, Query, Repo, User}

  @current_user_id 100

  def reject_unauthorized(%Query{} = query, Article) do
    grantors_for_user =
      Acl
      |> QueryBuilder.where(grantee_id: @current_user_id)
      |> Repo.all()
      |> Enum.map(&(&1.grantor_id))

    grantors_for_user = [@current_user_id | grantors_for_user]

    QueryBuilder.where(
      query,
      [],
      {:author_id, :in, grantors_for_user},
      or: {:publisher_id, :in, grantors_for_user}
    )
  end

  def reject_unauthorized(%Query{} = query, _source) do
    query
  end

  def reject_unauthorized_assoc(User, :authored_articles) do
    grantors_for_user =
      Acl
      |> QueryBuilder.where(grantee_id: @current_user_id)
      |> Repo.all()
      |> Enum.map(&(&1.grantor_id))

    %{
      join: :left,
      on: {:id, :in, grantors_for_user},
      or_on: {:id, @current_user_id}
    }
  end

  def reject_unauthorized_assoc(_source, _assoc) do
    nil
  end
end
