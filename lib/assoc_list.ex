defmodule QueryBuilder.AssocList do
  @moduledoc false

  alias QueryBuilder.AssocList.Builder
  alias QueryBuilder.AssocList.Node

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

  For each association field, a `%QueryBuilder.AssocList.Node{}` will be created with the following fields:

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

  def build(source_schema, assoc_list, assoc_fields, opts) do
    Builder.build(source_schema, assoc_list, assoc_fields, opts)
  end
end
