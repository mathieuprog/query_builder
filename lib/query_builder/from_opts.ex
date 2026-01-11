defmodule QueryBuilder.FromOpts do
  @moduledoc false

  @from_opts_supported_operations_boundary [
    :where,
    :where_any,
    :order_by,
    :limit,
    :offset
  ]

  @from_opts_supported_operations_full [
    :distinct,
    :distinct_roots,
    :group_by,
    :having,
    :having_any,
    :first_per,
    :inner_join,
    :left_join,
    :left_join_leaf,
    :left_join_latest,
    :left_join_top_n,
    :left_join_path,
    :limit,
    :maybe_order_by,
    :maybe_where,
    :offset,
    :order_by,
    :preload_separate,
    :preload_separate_scoped,
    :preload_through_join,
    :select,
    :select_merge,
    :top_n_per,
    :where,
    :where_any,
    :where_has,
    :where_exists,
    :where_exists_subquery,
    :where_missing,
    :where_not_exists,
    :where_not_exists_subquery
  ]

  @from_opts_supported_operations_boundary_string Enum.map_join(
                                                    @from_opts_supported_operations_boundary,
                                                    ", ",
                                                    &inspect/1
                                                  )

  @from_opts_supported_operations_full_string Enum.map_join(
                                                @from_opts_supported_operations_full,
                                                ", ",
                                                &inspect/1
                                              )

  def supported_operations(:boundary), do: @from_opts_supported_operations_boundary
  def supported_operations(:full), do: @from_opts_supported_operations_full

  def supported_operations_string(:boundary), do: @from_opts_supported_operations_boundary_string
  def supported_operations_string(:full), do: @from_opts_supported_operations_full_string

  def apply(query, opts, apply_module, from_opts_opts) do
    from_opts_opts = QueryBuilder.FromOpts.Options.validate_from_opts_options!(from_opts_opts)
    mode = Keyword.fetch!(from_opts_opts, :mode)
    includes_allowlist = Keyword.fetch!(from_opts_opts, :includes)

    {requested_includes, from_opts_ops} =
      QueryBuilder.FromOpts.Includes.extract_requested_includes_from_opts!(opts)

    if requested_includes != [] and map_size(includes_allowlist) == 0 do
      raise ArgumentError,
            "from_opts/2 got `include:` but no `includes:` allowlist was provided to from_opts/3. " <>
              "Define a context-owned allowlist (e.g. `includes: [role: :role]`) and pass include keys " <>
              "via opts (e.g. `include: [:role]`). If you handle `include` yourself, remove it from opts " <>
              "before calling `from_opts` (e.g. `{include, qb_opts} = Keyword.pop(opts, :include, [])`)."
    end

    extension_config =
      if apply_module == QueryBuilder do
        nil
      else
        QueryBuilder.FromOpts.Extension.extension_from_opts_config!(apply_module)
      end

    query
    |> QueryBuilder.FromOpts.Dispatch.apply(from_opts_ops, apply_module, mode, extension_config)
    |> QueryBuilder.FromOpts.Includes.apply_includes_allowlist!(
      requested_includes,
      includes_allowlist
    )
  end
end
