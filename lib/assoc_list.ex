defmodule QueryBuilder.AssocList do
  @moduledoc false

  defmodule State do
    @moduledoc false

    # AssocList is built recursively and needs to carry a handful of values between
    # recursive calls. This struct keeps the recursive function signatures readable.

    defstruct source_binding: nil,
              source_schema: nil,
              lock_left?: false
  end

  defmodule PreloadSpec do
    @moduledoc false

    @type strategy :: :auto | :separate | :through_join
    @type t :: %__MODULE__{strategy: strategy(), query_opts: keyword() | nil}

    @enforce_keys [:strategy]
    defstruct strategy: :auto,
              query_opts: nil

    @spec new(strategy(), keyword() | nil) :: t()
    def new(strategy \\ :auto, query_opts \\ nil)

    def new(strategy, query_opts) when strategy in [:auto, :separate, :through_join] do
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
            "invalid preload spec; expected strategy to be :auto, :separate, or :through_join, " <>
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

      if query_opts != nil and strategy != :separate do
        raise ArgumentError,
              "invalid preload spec merge for #{inspect(source_schema)}.#{inspect(assoc_field)}: " <>
                "scoped preload queries require separate preload"
      end

      %__MODULE__{strategy: strategy, query_opts: query_opts}
    end

    defp merge_strategy!(:through_join, _other), do: :through_join
    defp merge_strategy!(_other, :through_join), do: :through_join
    defp merge_strategy!(:separate, _other), do: :separate
    defp merge_strategy!(_other, :separate), do: :separate
    defp merge_strategy!(:auto, :auto), do: :auto

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

  defmodule JoinSpec do
    @moduledoc false

    @type qualifier :: :any | :left | :inner
    @type join_filter_group :: {filters :: list(), or_filters :: list()}
    @type join_filters :: [join_filter_group()]

    @type t :: %__MODULE__{
            required?: boolean(),
            qualifier: qualifier(),
            filters: join_filters(),
            joined?: boolean()
          }

    @enforce_keys [:required?, :qualifier, :filters, :joined?]
    defstruct required?: false,
              qualifier: :any,
              filters: [],
              joined?: false

    @spec new(boolean(), qualifier(), join_filters(), boolean()) :: t()
    def new(required? \\ false, qualifier \\ :any, filters \\ [], joined? \\ false)

    def new(required?, qualifier, filters, joined?)
        when is_boolean(required?) and qualifier in [:any, :left, :inner] and is_list(filters) and
               is_boolean(joined?) do
      normalized_filters =
        filters
        |> Enum.uniq()

      Enum.each(normalized_filters, fn
        {filters, or_filters} when is_list(filters) and is_list(or_filters) ->
          :ok

        other ->
          raise ArgumentError,
                "invalid join spec: expected join filters to be `{filters, or_filters}` pairs, got: #{inspect(other)}"
      end)

      if normalized_filters != [] and not required? do
        raise ArgumentError,
              "invalid join spec: join filters require the association to be joined"
      end

      if joined? and not required? do
        raise ArgumentError,
              "invalid join spec: joined? implies required? (internal join state invariant)"
      end

      %__MODULE__{
        required?: required?,
        qualifier: qualifier,
        filters: normalized_filters,
        joined?: joined?
      }
    end

    def new(required?, qualifier, filters, joined?) do
      raise ArgumentError,
            "invalid join spec: expected required? and joined? to be booleans, " <>
              "qualifier to be :any/:left/:inner, and filters to be a list; " <>
              "got: required?=#{inspect(required?)}, qualifier=#{inspect(qualifier)}, filters=#{inspect(filters)}, joined?=#{inspect(joined?)}"
    end

    @spec merge!(t(), t(), module(), atom()) :: t()
    def merge!(%__MODULE__{} = a, %__MODULE__{} = b, source_schema, assoc_field) do
      required? = a.required? || b.required?

      qualifier =
        merge_qualifiers!(a.qualifier, b.qualifier, source_schema, assoc_field)

      filters = a.filters ++ b.filters

      # `joined?` is runtime state (set by JoinMaker), not a "requirement".
      # During assoc_list merge it should always be false, but keep the merge rule explicit.
      joined? = a.joined? || b.joined?

      new(required?, qualifier, filters, joined?)
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

  @doc ~S"""
  Builds (and merges) an association tree data structure.

  It receives an association tree expressed as nested lists/keyword lists of
  association fields (atoms). For example:
  ```
  [
    {:authored_articles,
     [
       :article_likes,
       :article_stars,
       {:comments, [:comment_stars, comment_likes: :user]}
     ]},
    :published_articles
  ]
  ```

  For each association field, a map will be created with the following keys and values:

    * `:assoc_binding`: *named binding* to be used (atom)
    * `:assoc_field`: field name (atom)
    * `:assoc_schema`: module name of the schema (atom)
    * `:cardinality`: cardinality (atom `:one` or `:many`)
    * `:join_spec`: `%QueryBuilder.AssocList.JoinSpec{}` describing whether this association
    must be joined (`required?`), the join qualifier requirement (`qualifier`), optional join
    `on:` filters (`filters`), and whether QueryBuilder has joined/validated the join (`joined?`)
    * `:nested_assocs`: the nested associations (list)
    * `:source_binding`: *named binding* of the source schema (atom)
    * `:source_schema`: module name of the source schema (atom)
    * `preload_spec`: `nil` or `%QueryBuilder.AssocList.PreloadSpec{}` representing preload intent and strategy
      (auto/separate/through-join) and optional scoped separate-preload query options (`query_opts`)

  This information allows the exposed functions such as `QueryBuilder.where/3` to join
  associations, refer to associations, etc.
  """
  def build(source_schema, assoc_list, assoc_fields, opts \\ []) do
    state = %State{
      # the name of the binding of the query's root schema is the schema itself
      source_binding: source_schema,
      source_schema: source_schema
    }

    assoc_list
    |> do_build(List.wrap(assoc_fields), state, opts)
    |> merge_assoc_data()
  end

  defp merge_assoc_data(assoc_list) do
    Enum.reduce(assoc_list, [], fn assoc_data, new_assoc_list ->
      new_assoc_list
      |> Enum.with_index()
      |> Enum.find(fn {acc_assoc_data, _index} ->
        acc_assoc_data.assoc_binding == assoc_data.assoc_binding
      end)
      |> case do
        {acc_assoc_data, index} ->
          if acc_assoc_data.assoc_field != assoc_data.assoc_field or
               acc_assoc_data.source_schema != assoc_data.source_schema or
               acc_assoc_data.assoc_schema != assoc_data.assoc_schema do
            raise ArgumentError,
                  "association binding collision for #{inspect(assoc_data.assoc_binding)}; " <>
                    "it was generated for both #{inspect(acc_assoc_data.source_schema)}.#{inspect(acc_assoc_data.assoc_field)} " <>
                    "and #{inspect(assoc_data.source_schema)}.#{inspect(assoc_data.assoc_field)}. " <>
                    "This should not happen; please report a bug."
          end

          nested_assocs =
            merge_assoc_data(acc_assoc_data.nested_assocs ++ assoc_data.nested_assocs)

          join_spec =
            JoinSpec.merge!(
              acc_assoc_data.join_spec,
              assoc_data.join_spec,
              acc_assoc_data.source_schema,
              acc_assoc_data.assoc_field
            )

          preload_spec =
            PreloadSpec.merge!(
              acc_assoc_data.preload_spec,
              assoc_data.preload_spec,
              acc_assoc_data.source_schema,
              acc_assoc_data.assoc_field
            )

          new_assoc_data =
            acc_assoc_data
            |> Map.put(:nested_assocs, nested_assocs)
            |> Map.put(:join_spec, join_spec)
            |> Map.put(:preload_spec, preload_spec)

          List.replace_at(new_assoc_list, index, new_assoc_data)

        nil ->
          [assoc_data | new_assoc_list]
      end
    end)
  end

  defp do_build(assoc_list, [], _, _), do: assoc_list

  defp do_build(assoc_list, [{assoc_field, nested_assoc_fields} | tail], state, opts) do
    %{
      source_binding: source_binding,
      source_schema: source_schema
    } = state

    {assoc_field, join_type_marker} = normalize_assoc_field_and_join_type!(assoc_field)

    {join_required?, join_qualifier, join_filter_group} =
      case Keyword.get(opts, :join, :any) do
        :none ->
          {false, :any, []}

        :left ->
          left_join_mode = Keyword.get(opts, :left_join_mode, :leaf)

          if nested_assoc_fields == [] do
            join_filter_group =
              Keyword.get(opts, :join_filters, [])

            {true, :left, List.wrap(join_filter_group)}
          else
            case left_join_mode do
              :path ->
                {true, :left, []}

              :leaf ->
                {true, :inner, []}

              other ->
                raise ArgumentError,
                      "invalid left_join_mode #{inspect(other)}; expected :leaf or :path"
            end
          end

        join_qualifier when join_qualifier in [:inner, :left, :any] ->
          {true, join_qualifier, []}

        other ->
          raise ArgumentError,
                "invalid join option #{inspect(other)}; expected :none, :left, :inner, or :any"
      end

    join_type_marker =
      if join_required? do
        if state.lock_left? do
          if join_type_marker == :inner do
            raise ArgumentError,
                  "invalid assoc_fields: cannot require an inner join (`!`) under an optional association path (`?`) " <>
                    "for #{inspect(source_schema)}.#{inspect(assoc_field)}"
          end

          :left
        else
          join_type_marker
        end
      else
        # Join qualifiers are irrelevant when the operation does not request joins (e.g. preload),
        # but we still accept `:assoc?` / `:assoc!` to strip markers and keep assoc_fields uniform.
        :any
      end

    join_qualifier =
      JoinSpec.merge_qualifiers!(join_qualifier, join_type_marker, source_schema, assoc_field)

    join_filters =
      case join_filter_group do
        [] ->
          []

        [filters, or_filters] when is_list(filters) and is_list(or_filters) ->
          [{filters, or_filters}]

        other ->
          raise ArgumentError,
                "invalid join filters for #{inspect(source_schema)}.#{inspect(assoc_field)}: " <>
                  "expected `[filters, or_filters]` (as set by left_join*/*), got: #{inspect(other)}"
      end

    join_spec =
      JoinSpec.new(
        join_required?,
        join_qualifier,
        join_filters
      )

    preload_spec = Keyword.get(opts, :preload_spec, nil)

    preload_spec =
      case preload_spec do
        nil -> nil
        %PreloadSpec{} = preload_spec -> preload_spec
        other -> raise ArgumentError, "invalid preload spec: #{inspect(other)}"
      end

    assoc_data =
      assoc_data(
        source_binding,
        source_schema,
        assoc_field,
        join_spec,
        preload_spec
      )

    %{
      assoc_binding: assoc_binding,
      assoc_schema: assoc_schema
    } = assoc_data

    assoc_data =
      %{
        assoc_data
        | nested_assocs:
            do_build(
              [],
              List.wrap(nested_assoc_fields),
              %{
                state
                | source_binding: assoc_binding,
                  source_schema: assoc_schema,
                  lock_left?: state.lock_left? || join_qualifier == :left
              },
              opts
            )
      }

    do_build([assoc_data | assoc_list], tail, state, opts)
  end

  defp do_build(assoc_list, [assoc_field | tail], state, opts) do
    do_build(assoc_list, [{assoc_field, []} | tail], state, opts)
  end

  defp normalize_assoc_field_and_join_type!(assoc_field) when is_atom(assoc_field) do
    assoc_field_string = Atom.to_string(assoc_field)

    cond do
      String.ends_with?(assoc_field_string, "?") ->
        base = String.trim_trailing(assoc_field_string, "?")

        if base == "" do
          raise ArgumentError,
                "invalid assoc field #{inspect(assoc_field)} (cannot be only a marker)"
        end

        assoc_atom =
          try do
            String.to_existing_atom(base)
          rescue
            ArgumentError ->
              raise ArgumentError,
                    "invalid assoc field #{inspect(assoc_field)}; expected an existing association atom (like :role?), got unknown association #{inspect(base)}"
          end

        {assoc_atom, :left}

      String.ends_with?(assoc_field_string, "!") ->
        base = String.trim_trailing(assoc_field_string, "!")

        if base == "" do
          raise ArgumentError,
                "invalid assoc field #{inspect(assoc_field)} (cannot be only a marker)"
        end

        assoc_atom =
          try do
            String.to_existing_atom(base)
          rescue
            ArgumentError ->
              raise ArgumentError,
                    "invalid assoc field #{inspect(assoc_field)}; expected an existing association atom (like :role!), got unknown association #{inspect(base)}"
          end

        {assoc_atom, :inner}

      true ->
        {assoc_field, :any}
    end
  end

  defp normalize_assoc_field_and_join_type!(assoc_field) do
    raise ArgumentError,
          "invalid assoc field #{inspect(assoc_field)}; expected an association atom (optionally suffixed with ? or !)"
  end

  defp assoc_data(
         source_binding,
         source_schema,
         assoc_field,
         join_spec,
         preload_spec
       ) do
    assoc_schema = source_schema._assoc_schema(assoc_field)
    cardinality = source_schema._assoc_cardinality(assoc_field)
    assoc_binding = source_schema._binding(assoc_field)

    %{
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema,
      cardinality: cardinality,
      join_spec: join_spec,
      preload_spec: preload_spec,
      nested_assocs: [],
      source_binding: source_binding,
      source_schema: source_schema
    }
  end
end
