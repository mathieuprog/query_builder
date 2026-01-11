defmodule QueryBuilder.Pagination.KeysFirst do
  @moduledoc false

  require Ecto.Query
  alias Ecto.Query

  def load_entries_for_page(_repo, _ecto_query, _source_schema, _pk_fields, []), do: []

  def load_entries_for_page(repo, ecto_query, source_schema, [pk_field], keys)
      when is_list(keys) do
    ecto_query = maybe_strip_for_keys_first_entries_load(ecto_query)

    entries_query =
      ecto_query
      |> Query.exclude([:limit, :offset, :order_by])
      |> Ecto.Query.where([{^source_schema, x}], field(x, ^pk_field) in ^keys)

    entries = repo.all(entries_query)

    entries_by_key =
      Enum.reduce(entries, %{}, fn entry, acc ->
        Map.put_new(acc, Map.fetch!(entry, pk_field), entry)
      end)

    Enum.map(keys, fn key ->
      case Map.fetch(entries_by_key, key) do
        {:ok, entry} ->
          entry

        :error ->
          raise ArgumentError,
                "paginate_cursor/3 and paginate_offset/3 internal error: expected to load an entry with primary key #{inspect(key)}, " <>
                  "but it was missing from the results"
      end
    end)
  end

  def load_entries_for_page(repo, ecto_query, source_schema, pk_fields, keys)
      when is_list(pk_fields) and length(pk_fields) > 1 and is_list(keys) do
    ecto_query = maybe_strip_for_keys_first_entries_load(ecto_query)

    dynamic_keys =
      Enum.map(keys, fn key ->
        key_parts =
          key
          |> Tuple.to_list()
          |> then(&Enum.zip(pk_fields, &1))

        key_parts
        |> Enum.map(fn {field, value} ->
          Ecto.Query.dynamic([{^source_schema, x}], field(x, ^field) == ^value)
        end)
        |> Enum.reduce(&Ecto.Query.dynamic(^&1 and ^&2))
      end)

    [first | rest] = dynamic_keys
    where_dynamic = Enum.reduce(rest, first, &Ecto.Query.dynamic(^&1 or ^&2))

    entries_query =
      ecto_query
      |> Query.exclude([:limit, :offset, :order_by])
      |> Ecto.Query.where(^where_dynamic)

    entries = repo.all(entries_query)

    entries_by_key =
      Enum.reduce(entries, %{}, fn entry, acc ->
        key =
          pk_fields
          |> Enum.map(&Map.fetch!(entry, &1))
          |> List.to_tuple()

        Map.put_new(acc, key, entry)
      end)

    Enum.map(keys, fn key ->
      case Map.fetch(entries_by_key, key) do
        {:ok, entry} ->
          entry

        :error ->
          raise ArgumentError,
                "paginate_cursor/3 and paginate_offset/3 internal error: expected to load an entry with primary key #{inspect(key)}, " <>
                  "but it was missing from the results"
      end
    end)
  end

  defp maybe_strip_for_keys_first_entries_load(%Ecto.Query{assocs: []} = ecto_query) do
    strip_for_keys_first_entries_load(ecto_query)
  end

  defp maybe_strip_for_keys_first_entries_load(%Ecto.Query{} = ecto_query) do
    ecto_query
  end

  defp strip_for_keys_first_entries_load(%Ecto.Query{} = ecto_query) do
    # In keys-first pagination, the keys query already applied all filters/joins needed to
    # decide membership + ordering. When we reload by PK list, we can often drop the join
    # graph (which avoids join multiplication and can be significantly cheaper).
    #
    # This is only safe when the final query does not contain join-preloads (assocs), since
    # those require the join semantics to hydrate associations.
    Query.exclude(ecto_query, [
      :join,
      :where,
      :group_by,
      :having,
      :distinct,
      :windows,
      :select
    ])
  end
end
