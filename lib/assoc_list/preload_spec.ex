defmodule QueryBuilder.AssocList.PreloadSpec do
  @moduledoc false

  @type strategy :: :separate | :through_join
  @type t :: %__MODULE__{strategy: strategy(), query_opts: keyword() | nil}

  @enforce_keys [:strategy]
  defstruct strategy: :separate,
            query_opts: nil

  @spec new(strategy(), keyword() | nil) :: t()
  def new(strategy \\ :separate, query_opts \\ nil)

  def new(strategy, query_opts) when strategy in [:separate, :through_join] do
    if query_opts != nil and not Keyword.keyword?(query_opts) do
      raise ArgumentError,
            "invalid preload query opts; expected a keyword list or nil, got: #{inspect(query_opts)}"
    end

    if query_opts != nil and strategy != :separate do
      raise ArgumentError,
            "invalid preload spec: scoped preload queries require separate preload; " <>
              "got strategy #{inspect(strategy)} with query opts #{inspect(query_opts)}"
    end

    %__MODULE__{strategy: strategy, query_opts: query_opts}
  end

  def new(strategy, query_opts) do
    raise ArgumentError,
          "invalid preload spec; expected strategy to be :separate or :through_join, " <>
            "got: #{inspect(strategy)} with query opts #{inspect(query_opts)}"
  end

  @spec merge!(t() | nil, t() | nil, module(), atom()) :: t() | nil
  def merge!(nil, nil, _source_schema, _assoc_field), do: nil
  def merge!(%__MODULE__{} = spec, nil, _source_schema, _assoc_field), do: spec
  def merge!(nil, %__MODULE__{} = spec, _source_schema, _assoc_field), do: spec

  def merge!(%__MODULE__{} = a, %__MODULE__{} = b, source_schema, assoc_field) do
    strategy = merge_strategy!(a.strategy, b.strategy)
    query_opts = merge_query_opts!(a.query_opts, b.query_opts, source_schema, assoc_field)

    if query_opts != nil and strategy == :through_join do
      raise ArgumentError,
            "conflicting preload requirements for #{inspect(source_schema)}.#{inspect(assoc_field)}: " <>
              "cannot combine a scoped separate preload with `preload_through_join`"
    end

    %__MODULE__{strategy: strategy, query_opts: query_opts}
  end

  defp merge_strategy!(:through_join, _other), do: :through_join
  defp merge_strategy!(_other, :through_join), do: :through_join
  defp merge_strategy!(:separate, _other), do: :separate
  defp merge_strategy!(_other, :separate), do: :separate

  defp merge_query_opts!(a, b, source_schema, assoc_field) do
    case {a, b} do
      {nil, nil} ->
        nil

      {opts, nil} ->
        opts

      {nil, opts} ->
        opts

      {opts, opts} ->
        opts

      {a, b} ->
        raise ArgumentError,
              "conflicting scoped preload queries for #{inspect(source_schema)}.#{inspect(assoc_field)}: " <>
                "cannot combine #{inspect(a)} and #{inspect(b)}"
    end
  end
end
