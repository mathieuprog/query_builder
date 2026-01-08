defmodule QueryBuilder.AssocList do
  @moduledoc false

  @type assoc_path :: [atom()]

  @type t :: %__MODULE__{
          id: pos_integer(),
          revision: non_neg_integer(),
          root_schema: module(),
          roots: %{optional(atom()) => Node.t()},
          by_path: %{optional(assoc_path()) => atom()},
          by_name: %{optional(atom()) => [%{binding: atom(), path: assoc_path()}]},
          by_binding: %{
            optional(atom()) => %{
              source_binding: atom(),
              source_schema: module(),
              assoc_field: atom(),
              path: assoc_path()
            }
          }
        }

  defstruct id: 0,
            revision: 0,
            root_schema: nil,
            roots: %{},
            by_path: %{},
            by_name: %{},
            by_binding: %{}

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

  defmodule JoinSpec do
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
            filters
            |> Enum.uniq()
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

  defmodule Node do
    @moduledoc false

    @type t :: %__MODULE__{
            assoc_binding: atom(),
            assoc_field: atom(),
            assoc_schema: module(),
            cardinality: :one | :many,
            join_spec: JoinSpec.t(),
            preload_spec: PreloadSpec.t() | nil,
            nested_assocs: %{optional(atom()) => t()},
            source_binding: atom(),
            source_schema: module()
          }

    @enforce_keys [
      :assoc_binding,
      :assoc_field,
      :assoc_schema,
      :cardinality,
      :join_spec,
      :nested_assocs,
      :source_binding,
      :source_schema
    ]

    defstruct assoc_binding: nil,
              assoc_field: nil,
              assoc_schema: nil,
              cardinality: :one,
              join_spec: nil,
              preload_spec: nil,
              nested_assocs: %{},
              source_binding: nil,
              source_schema: nil
  end

  @spec new(module()) :: t()
  def new(root_schema) when is_atom(root_schema) do
    %__MODULE__{
      id: System.unique_integer([:positive]),
      revision: 0,
      root_schema: root_schema
    }
  end

  def new(other) do
    raise ArgumentError, "AssocList.new/1 expects a schema module, got: #{inspect(other)}"
  end

  @spec root_assoc(t(), atom()) :: Node.t() | nil
  def root_assoc(%__MODULE__{} = assoc_list, assoc_field) when is_atom(assoc_field) do
    Map.get(assoc_list.roots, assoc_field)
  end

  @spec binding_from_assoc_name(t(), atom()) ::
          {:ok, atom()} | {:error, :not_found} | {:error, {:ambiguous, list()}}
  def binding_from_assoc_name(%__MODULE__{} = assoc_list, assoc_field)
      when is_atom(assoc_field) do
    case Map.get(assoc_list.by_name, assoc_field, []) do
      [] ->
        {:error, :not_found}

      [%{binding: binding}] ->
        {:ok, binding}

      matches ->
        {:error, {:ambiguous, matches}}
    end
  end

  @spec binding_from_assoc_path(t(), assoc_path()) :: {:ok, atom()} | {:error, :not_found}
  def binding_from_assoc_path(%__MODULE__{} = assoc_list, assoc_path) when is_list(assoc_path) do
    case Map.fetch(assoc_list.by_path, assoc_path) do
      {:ok, binding} -> {:ok, binding}
      :error -> {:error, :not_found}
    end
  end

  @spec any?(t(), (Node.t() -> as_boolean(term()))) :: boolean()
  def any?(%__MODULE__{} = assoc_list, fun) when is_function(fun, 1) do
    do_any?(assoc_list.roots, fun)
  end

  defp do_any?(nodes_map, fun) when is_map(nodes_map) do
    Enum.any?(nodes_map, fn {_key, node} ->
      fun.(node) or do_any?(node.nested_assocs, fun)
    end)
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
    must be joined (`required?`), the join qualifier requirement (`qualifier`), and optional join
    `on:` filters (`filters`)
    * `:nested_assocs`: the nested associations (map)
    * `:source_binding`: *named binding* of the source schema (atom)
    * `:source_schema`: module name of the source schema (atom)
    * `:preload_spec`: `nil` or `%QueryBuilder.AssocList.PreloadSpec{}` representing preload intent and strategy
      (separate/through-join) and optional scoped separate-preload query options (`query_opts`)

  This information allows the exposed functions such as `QueryBuilder.where/3` to join
  associations, refer to associations, etc.
  """
  def build(source_schema, assoc_list, assoc_fields, opts \\ [])

  def build(source_schema, %__MODULE__{} = assoc_list, assoc_fields, opts)
      when is_atom(source_schema) do
    if assoc_list.root_schema != source_schema do
      raise ArgumentError,
            "AssocList.build/4 expected root schema #{inspect(assoc_list.root_schema)}, " <>
              "got: #{inspect(source_schema)}"
    end

    state = %State{
      # the name of the binding of the query's root schema is the schema itself
      source_binding: source_schema,
      source_schema: source_schema
    }

    {roots, assoc_list} =
      do_build_nodes(assoc_list, assoc_list.roots, List.wrap(assoc_fields), state, opts, [])

    %{assoc_list | roots: roots}
  end

  def build(source_schema, other_assoc_list, _assoc_fields, _opts) do
    raise ArgumentError,
          "AssocList.build/4 expects the assoc_list argument to be a %QueryBuilder.AssocList{}, " <>
            "got: #{inspect(other_assoc_list)} for root schema #{inspect(source_schema)}"
  end

  defp do_build_nodes(%__MODULE__{} = assoc_list, nodes_map, [], _state, _opts, _prefix_rev)
       when is_map(nodes_map) do
    {nodes_map, assoc_list}
  end

  defp do_build_nodes(
         %__MODULE__{} = assoc_list,
         nodes_map,
         [{assoc_field, nested_assoc_fields} | tail],
         state,
         opts,
         prefix_rev
       ) do
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
            join_filter_group = Keyword.get(opts, :join_filters, [])
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

    preload_spec =
      case Keyword.get(opts, :preload_spec, nil) do
        nil -> nil
        %PreloadSpec{} = preload_spec -> preload_spec
        other -> raise ArgumentError, "invalid preload spec: #{inspect(other)}"
      end

    {nodes_map, assoc_list, assoc_data} =
      upsert_current_assoc_data(
        assoc_list,
        nodes_map,
        prefix_rev,
        assoc_data(source_binding, source_schema, assoc_field, join_spec, preload_spec)
      )

    {nested_assocs, assoc_list} =
      do_build_nodes(
        assoc_list,
        assoc_data.nested_assocs,
        List.wrap(nested_assoc_fields),
        %{
          state
          | source_binding: assoc_data.assoc_binding,
            source_schema: assoc_data.assoc_schema,
            lock_left?: state.lock_left? || join_qualifier == :left
        },
        opts,
        [assoc_field | prefix_rev]
      )

    assoc_data = %{assoc_data | nested_assocs: nested_assocs}
    nodes_map = Map.put(nodes_map, assoc_field, assoc_data)

    do_build_nodes(assoc_list, nodes_map, tail, state, opts, prefix_rev)
  end

  defp do_build_nodes(
         %__MODULE__{} = assoc_list,
         nodes_map,
         [assoc_field | tail],
         state,
         opts,
         prefix_rev
       ) do
    do_build_nodes(assoc_list, nodes_map, [{assoc_field, []} | tail], state, opts, prefix_rev)
  end

  defp upsert_current_assoc_data(
         %__MODULE__{} = assoc_list,
         nodes_map,
         prefix_rev,
         %Node{} = new_node
       )
       when is_map(nodes_map) and is_list(prefix_rev) do
    {nodes_map, {node, inserted?}} =
      case Map.get(nodes_map, new_node.assoc_field) do
        nil ->
          {Map.put(nodes_map, new_node.assoc_field, new_node), {new_node, true}}

        %Node{} = existing ->
          if existing.assoc_binding != new_node.assoc_binding or
               existing.source_binding != new_node.source_binding or
               existing.source_schema != new_node.source_schema or
               existing.assoc_schema != new_node.assoc_schema do
            raise ArgumentError,
                  "association tree conflict for #{inspect(existing.source_schema)}.#{inspect(existing.assoc_field)}; " <>
                    "this is likely a QueryBuilder bug. Please report it."
          end

          join_spec =
            JoinSpec.merge!(
              existing.join_spec,
              new_node.join_spec,
              existing.source_schema,
              existing.assoc_field
            )

          preload_spec =
            PreloadSpec.merge!(
              existing.preload_spec,
              new_node.preload_spec,
              existing.source_schema,
              existing.assoc_field
            )

          merged = %{existing | join_spec: join_spec, preload_spec: preload_spec}

          {Map.put(nodes_map, merged.assoc_field, merged), {merged, false}}
      end

    assoc_list =
      if inserted? do
        path = Enum.reverse([new_node.assoc_field | prefix_rev])

        assoc_list
        |> then(&%{&1 | revision: &1.revision + 1})
        |> register_path!(path, node.assoc_binding)
        |> register_name!(node.assoc_field, node.assoc_binding, path)
        |> register_binding!(node, path)
      else
        assoc_list
      end

    {nodes_map, assoc_list, node}
  end

  defp register_path!(%__MODULE__{} = assoc_list, path, binding) do
    case Map.get(assoc_list.by_path, path) do
      nil ->
        %{assoc_list | by_path: Map.put(assoc_list.by_path, path, binding)}

      ^binding ->
        assoc_list

      other ->
        raise ArgumentError,
              "association path collision for #{inspect(path)}; it resolves to both " <>
                "#{inspect(binding)} and #{inspect(other)}. This is likely a QueryBuilder bug; please report it."
    end
  end

  defp register_name!(%__MODULE__{} = assoc_list, assoc_field, binding, path) do
    match = %{binding: binding, path: path}
    matches = Map.get(assoc_list.by_name, assoc_field, [])
    %{assoc_list | by_name: Map.put(assoc_list.by_name, assoc_field, [match | matches])}
  end

  defp register_binding!(%__MODULE__{} = assoc_list, %Node{} = node, path) do
    descriptor = %{
      source_binding: node.source_binding,
      source_schema: node.source_schema,
      assoc_field: node.assoc_field,
      path: path
    }

    case Map.get(assoc_list.by_binding, node.assoc_binding) do
      nil ->
        %{assoc_list | by_binding: Map.put(assoc_list.by_binding, node.assoc_binding, descriptor)}

      %{source_binding: same_source, source_schema: same_schema, assoc_field: same_field} =
          existing
      when same_source == node.source_binding and same_schema == node.source_schema and
             same_field == node.assoc_field ->
        if existing.path != path do
          raise ArgumentError,
                "association binding collision for #{inspect(node.assoc_binding)}; " <>
                  "it is used for multiple association paths (#{inspect(existing.path)} and #{inspect(path)}). " <>
                  "QueryBuilder's binding naming scheme cannot represent the same association join multiple times " <>
                  "under different parent joins. Use explicit Ecto joins with distinct `as:` bindings for this case."
        end

        assoc_list

      _other ->
        raise ArgumentError,
              "association binding collision for #{inspect(node.assoc_binding)}; " <>
                "QueryBuilder attempted to use it for #{inspect(node.source_schema)}.#{inspect(node.assoc_field)} " <>
                "at path #{inspect(path)}, but it is already used elsewhere in the association tree. " <>
                "QueryBuilder cannot safely represent multiple distinct joins under the same binding. " <>
                "Use explicit Ecto joins with distinct `as:` bindings for this case."
    end
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

    %Node{
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema,
      cardinality: cardinality,
      join_spec: join_spec,
      preload_spec: preload_spec,
      nested_assocs: %{},
      source_binding: source_binding,
      source_schema: source_schema
    }
  end
end
