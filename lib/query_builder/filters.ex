defmodule QueryBuilder.Filters do
  @moduledoc false

  def normalize_or_groups!(or_groups, opt_key, context) do
    cond do
      is_nil(or_groups) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` to be a list of filter groups; got nil"

      Keyword.keyword?(or_groups) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` to be a list of filter groups like `[[...], [...]]`; " <>
                "got a keyword list. Wrap it in a list if you intended a single group."

      not is_list(or_groups) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` to be a list of filter groups like `[[...], [...]]`; got: #{inspect(or_groups)}"

      Enum.any?(or_groups, &(not is_list(&1))) ->
        raise ArgumentError,
              "#{context} expects `#{opt_key}:` groups to be lists (e.g. `[[title: \"A\"], [title: \"B\"]]`); got: #{inspect(or_groups)}"

      true ->
        or_groups
    end
  end
end
