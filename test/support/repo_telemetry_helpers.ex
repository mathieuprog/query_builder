defmodule QueryBuilder.RepoTelemetryHelpers do
  @moduledoc false

  def with_repo_query_count(fun) when is_function(fun, 0) do
    parent = self()
    handler_id = "repo-query-count-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:query_builder, :repo, :query],
      fn _event, _measurements, _metadata, parent ->
        send(parent, {:repo_query, handler_id})
      end,
      parent
    )

    result =
      try do
        fun.()
      after
        :telemetry.detach(handler_id)
      end

    {result, drain_repo_query_messages(handler_id)}
  end

  def with_repo_queries(fun) when is_function(fun, 0) do
    parent = self()
    handler_id = "repo-queries-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:query_builder, :repo, :query],
      fn _event, _measurements, metadata, parent ->
        send(parent, {:repo_query, handler_id, metadata})
      end,
      parent
    )

    result =
      try do
        fun.()
      after
        :telemetry.detach(handler_id)
      end

    {result, drain_repo_query_metadata(handler_id)}
  end

  defp drain_repo_query_messages(handler_id, count \\ 0) do
    receive do
      {:repo_query, ^handler_id} ->
        drain_repo_query_messages(handler_id, count + 1)
    after
      0 ->
        count
    end
  end

  defp drain_repo_query_metadata(handler_id, acc \\ []) do
    receive do
      {:repo_query, ^handler_id, metadata} ->
        drain_repo_query_metadata(handler_id, [metadata | acc])
    after
      0 ->
        Enum.reverse(acc)
    end
  end
end
