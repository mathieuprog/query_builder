defmodule QueryBuilder.FromOpts.Options do
  @moduledoc false

  def validate_from_opts_options!(opts) when is_list(opts) do
    unless Keyword.keyword?(opts) do
      raise ArgumentError,
            "from_opts/3 expects options to be a keyword list like `[mode: :boundary]`, got: #{inspect(opts)}"
    end

    mode = Keyword.get(opts, :mode, :boundary)

    includes_allowlist =
      QueryBuilder.FromOpts.Includes.normalize_from_opts_includes_allowlist!(
        Keyword.get(opts, :includes, %{})
      )

    unless mode in [:boundary, :full] do
      raise ArgumentError,
            "from_opts/3 expects `mode:` to be :boundary or :full, got: #{inspect(mode)}"
    end

    case Keyword.keys(opts) -- [:mode, :includes] do
      [] ->
        :ok

      unknown ->
        raise ArgumentError,
              "from_opts/3 got unknown options #{inspect(unknown)}; supported options: [:mode, :includes]"
    end

    [mode: mode, includes: includes_allowlist]
  end

  def validate_from_opts_options!(opts) do
    raise ArgumentError,
          "from_opts/3 expects options to be a keyword list like `[mode: :boundary]`, got: #{inspect(opts)}"
  end
end
