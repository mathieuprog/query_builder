defmodule QueryBuilder.Pagination.CursorCodec do
  @moduledoc false

  @max_encoded_cursor_bytes 8_192

  def encode_cursor(cursor_map) when is_map(cursor_map) do
    cursor_map
    |> Jason.encode!()
    |> Base.url_encode64()
  end

  def decode_cursor!(nil), do: %{}

  def decode_cursor!(cursor) when is_binary(cursor) do
    if cursor == "" do
      raise ArgumentError,
            "paginate_cursor/3 cursor cannot be an empty string; omit `cursor:` (or pass `nil`) for the first page"
    end

    cursor_size = byte_size(cursor)

    if cursor_size > @max_encoded_cursor_bytes do
      raise ArgumentError,
            "paginate_cursor/3 cursor is too large (max #{@max_encoded_cursor_bytes} bytes); " <>
              "got #{cursor_size} bytes"
    end

    decoded_string =
      case Base.url_decode64(cursor) do
        {:ok, decoded} ->
          decoded

        :error ->
          case Base.url_decode64(cursor, padding: false) do
            {:ok, decoded} ->
              decoded

            :error ->
              raise ArgumentError,
                    "paginate_cursor/3 invalid cursor; expected base64url-encoded JSON, got: #{inspect(cursor)}"
          end
      end

    decoded_cursor =
      case Jason.decode(decoded_string) do
        {:ok, decoded} ->
          decoded

        {:error, error} ->
          raise ArgumentError,
                "paginate_cursor/3 invalid cursor; expected base64url-encoded JSON, got JSON decode error: #{Exception.message(error)}"
      end

    unless is_map(decoded_cursor) do
      raise ArgumentError,
            "paginate_cursor/3 invalid cursor; expected a JSON object (map), got: #{inspect(decoded_cursor)}"
    end

    decoded_cursor = normalize_cursor_map!(decoded_cursor)

    if decoded_cursor == %{} do
      raise ArgumentError,
            "paginate_cursor/3 invalid cursor; decoded cursor map was empty; omit `cursor:` (or pass `nil`) for the first page"
    end

    decoded_cursor
  end

  def decode_cursor!(cursor) when is_map(cursor) do
    cursor = normalize_cursor_map!(cursor)

    if cursor == %{} do
      raise ArgumentError,
            "paginate_cursor/3 cursor map cannot be empty; omit `cursor:` (or pass `nil`) for the first page"
    end

    cursor
  end

  def decode_cursor!(cursor) do
    raise ArgumentError,
          "paginate_cursor/3 cursor must be a map or a base64url-encoded JSON map (string), got: #{inspect(cursor)}"
  end

  def validate_cursor_matches_order_by!(cursor, order_by_list) when is_map(cursor) do
    expected_keys =
      order_by_list
      |> Enum.map(fn {_direction, field} -> to_string(field) end)
      |> Enum.uniq()

    cursor_keys = Map.keys(cursor)

    expected_set = MapSet.new(expected_keys)
    cursor_set = MapSet.new(cursor_keys)

    missing =
      expected_set
      |> MapSet.difference(cursor_set)
      |> MapSet.to_list()
      |> Enum.sort()

    extra =
      cursor_set
      |> MapSet.difference(expected_set)
      |> MapSet.to_list()
      |> Enum.sort()

    if missing != [] or extra != [] do
      raise ArgumentError,
            "paginate_cursor/3 cursor does not match the query's order_by fields; " <>
              "expected keys: #{inspect(expected_keys)}, " <>
              "missing: #{inspect(missing)}, extra: #{inspect(extra)}. " <>
              "This cursor was likely generated for a different query or the query's order_by changed."
    end
  end

  def cursorable_order_by_field?(field) when is_atom(field) or is_binary(field), do: true
  def cursorable_order_by_field?(_field), do: false

  def supported_cursor_order_direction?(direction)
      when direction in [
             :asc,
             :desc,
             :asc_nulls_first,
             :asc_nulls_last,
             :desc_nulls_first,
             :desc_nulls_last
           ],
      do: true

  def supported_cursor_order_direction?(_direction), do: false

  def normalize_cursor_order_direction(adapter, direction, field) do
    case direction do
      :asc -> {:asc, adapter_default_nulls_position!(adapter, :asc, field)}
      :desc -> {:desc, adapter_default_nulls_position!(adapter, :desc, field)}
      :asc_nulls_first -> {:asc, :first}
      :asc_nulls_last -> {:asc, :last}
      :desc_nulls_first -> {:desc, :first}
      :desc_nulls_last -> {:desc, :last}
    end
  end

  def reverse_order_direction(direction, field) do
    case direction do
      :asc ->
        :desc

      :desc ->
        :asc

      :asc_nulls_first ->
        :desc_nulls_last

      :asc_nulls_last ->
        :desc_nulls_first

      :desc_nulls_first ->
        :asc_nulls_last

      :desc_nulls_last ->
        :asc_nulls_first

      other ->
        raise ArgumentError,
              "paginate_cursor/3 can't reverse order direction #{inspect(other)} for field #{inspect(field)} " <>
                "(supported: :asc, :desc, :asc_nulls_first, :asc_nulls_last, :desc_nulls_first, :desc_nulls_last)"
    end
  end

  defp normalize_cursor_map!(cursor) when is_map(cursor) do
    normalized_pairs =
      Enum.map(cursor, fn {key, value} ->
        {normalize_cursor_key!(key), value}
      end)

    normalized_keys = Enum.map(normalized_pairs, &elem(&1, 0))

    if length(normalized_keys) != length(Enum.uniq(normalized_keys)) do
      raise ArgumentError,
            "paginate_cursor/3 cursor map has duplicate keys after normalization: #{inspect(normalized_keys)}"
    end

    Map.new(normalized_pairs)
  end

  defp normalize_cursor_key!(key) when is_binary(key) do
    if key == "" do
      raise ArgumentError, "paginate_cursor/3 cursor map has an empty key"
    end

    key
  end

  defp normalize_cursor_key!(key) when is_atom(key) do
    Atom.to_string(key)
  end

  defp normalize_cursor_key!(key) do
    raise ArgumentError,
          "paginate_cursor/3 cursor map keys must be strings or atoms; got: #{inspect(key)}"
  end

  defp adapter_default_nulls_position!(adapter, dir, field) when dir in [:asc, :desc] do
    case {adapter, dir} do
      {Ecto.Adapters.Postgres, :asc} ->
        :last

      {Ecto.Adapters.Postgres, :desc} ->
        :first

      {Ecto.Adapters.MyXQL, :asc} ->
        :first

      {Ecto.Adapters.MyXQL, :desc} ->
        :last

      {Ecto.Adapters.SQLite3, :asc} ->
        :first

      {Ecto.Adapters.SQLite3, :desc} ->
        :last

      {other, _} ->
        raise ArgumentError,
              "paginate_cursor/3 cannot infer the default NULL ordering for adapter #{inspect(other)} " <>
                "when using #{inspect(dir)} for #{inspect(field)}; " <>
                "supported adapters: Ecto.Adapters.Postgres, Ecto.Adapters.MyXQL, Ecto.Adapters.SQLite3. " <>
                "Use explicit *_nulls_* order directions if supported by your adapter."
    end
  end
end
