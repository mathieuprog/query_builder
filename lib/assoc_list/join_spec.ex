defmodule QueryBuilder.AssocList.JoinSpec do
  @moduledoc false

  @type qualifier :: :any | :left | :inner
  @type join_filter_group :: {filters :: list(), or_filters :: list()}
  @type join_filters :: [join_filter_group()]

  @type t :: %__MODULE__{
          required?: boolean(),
          qualifier: qualifier(),
          filters: join_filters()
        }

  @enforce_keys [:required?, :qualifier, :filters]
  defstruct required?: false,
            qualifier: :any,
            filters: []

  @spec new(boolean(), qualifier(), join_filters()) :: t()
  def new(required? \\ false, qualifier \\ :any, filters \\ [])

  def new(required?, qualifier, filters)
      when is_boolean(required?) and qualifier in [:any, :left, :inner] and is_list(filters) do
    normalized_filters =
      case filters do
        [] ->
          []

        _ ->
          Enum.uniq(filters)
      end

    if normalized_filters != [] do
      Enum.each(normalized_filters, fn
        {filters, or_filters} when is_list(filters) and is_list(or_filters) ->
          :ok

        other ->
          raise ArgumentError,
                "invalid join spec: expected join filters to be `{filters, or_filters}` pairs, got: #{inspect(other)}"
      end)
    end

    if not required? and qualifier != :any do
      raise ArgumentError,
            "invalid join spec: join qualifier requires the association to be joined"
    end

    if normalized_filters != [] and not required? do
      raise ArgumentError,
            "invalid join spec: join filters require the association to be joined"
    end

    %__MODULE__{
      required?: required?,
      qualifier: qualifier,
      filters: normalized_filters
    }
  end

  def new(required?, qualifier, filters) do
    raise ArgumentError,
          "invalid join spec: expected required? to be a boolean, qualifier to be :any/:left/:inner, " <>
            "and filters to be a list; got: required?=#{inspect(required?)}, qualifier=#{inspect(qualifier)}, filters=#{inspect(filters)}"
  end

  @spec merge!(t(), t(), module(), atom()) :: t()
  def merge!(%__MODULE__{} = a, %__MODULE__{} = b, source_schema, assoc_field) do
    required? = a.required? || b.required?

    qualifier =
      merge_qualifiers!(a.qualifier, b.qualifier, source_schema, assoc_field)

    new(required?, qualifier, a.filters ++ b.filters)
  end

  @spec merge_qualifiers!(qualifier(), qualifier(), module(), atom()) :: qualifier()
  def merge_qualifiers!(left, right, source_schema, assoc_field) do
    allowed = [:left, :inner, :any]

    if left not in allowed do
      raise ArgumentError,
            "invalid join qualifier #{inspect(left)} for #{inspect(source_schema)}.#{inspect(assoc_field)}; " <>
              "expected :left, :inner, or :any"
    end

    if right not in allowed do
      raise ArgumentError,
            "invalid join qualifier #{inspect(right)} for #{inspect(source_schema)}.#{inspect(assoc_field)}; " <>
              "expected :left, :inner, or :any"
    end

    case {left, right} do
      {:any, join_type} ->
        join_type

      {join_type, :any} ->
        join_type

      {join_type, join_type} ->
        join_type

      {a, b} ->
        raise ArgumentError,
              "conflicting join requirements for #{inspect(source_schema)}.#{inspect(assoc_field)}: " <>
                "cannot mix #{inspect(a)} and #{inspect(b)}"
    end
  end
end
