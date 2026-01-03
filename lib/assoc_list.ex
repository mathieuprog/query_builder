defmodule QueryBuilder.AssocList do
  @moduledoc false

  defmodule State do
    @moduledoc false

    # AssocList is built recursively and needs to carry a handful of values between
    # recursive calls. This struct keeps the recursive function signatures readable.

    defstruct source_binding: nil,
              source_schema: nil,
              # `bindings` allows to keep track of all the binding names in order to
              # detect a binding name that is going to be used twice when joining
              # associations.
              bindings: []
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
    * `:has_joined`: indicating whether the association has already been joined or not
    with Ecto query (boolean)
    * `:nested_assocs`: the nested associations (list)
    * `:source_binding`: *named binding* of the source schema (atom)
    * `:source_schema`: module name of the source schema (atom)
    * `join?`: whether this association should be joined (boolean)
    * `join_type`: `:left` or `:inner` (atom; only meaningful when `join?` is true)
    * `join_filters`: only in case of a left join, clauses for the `:on` option (list of
    two keyword lists – and/or clauses)
    * `preload`: is to be preloaded or not (boolean)
    * `preload_strategy`: `:separate` or `:through_join` (atom; only meaningful when `preload` is true)

  This information allows the exposed functions such as `QueryBuilder.where/3` to join
  associations, refer to associations, etc.
  """
  def build(source_schema, assoc_list, assoc_fields, opts \\ []) do
    state = %State{
      # the name of the binding of the query's root schema is the schema itself
      source_binding: source_schema,
      source_schema: source_schema,
      bindings: [source_schema]
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

          join? = acc_assoc_data.join? || assoc_data.join?

          join_type =
            if acc_assoc_data.join_type == :left || assoc_data.join_type == :left do
              :left
            else
              :inner
            end

          preload = acc_assoc_data.preload || assoc_data.preload

          preload_strategy =
            cond do
              acc_assoc_data.preload_strategy == :through_join ||
                  assoc_data.preload_strategy == :through_join ->
                :through_join

              acc_assoc_data.preload_strategy == :separate ||
                  assoc_data.preload_strategy == :separate ->
                :separate

              true ->
                nil
            end

          join_filters =
            (acc_assoc_data.join_filters ++ assoc_data.join_filters)
            |> Enum.uniq()
            |> Enum.reject(&(&1 == []))

          new_assoc_data =
            acc_assoc_data
            |> Map.put(:nested_assocs, nested_assocs)
            |> Map.put(:join?, join?)
            |> Map.put(:join_type, join_type)
            |> Map.put(:join_filters, join_filters)
            |> Map.put(:preload, preload)
            |> Map.put(:preload_strategy, preload_strategy)

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
      source_schema: source_schema,
      bindings: bindings
    } = state

    {join?, join_type, join_filters} =
      case Keyword.get(opts, :join, :inner) do
        :none ->
          {false, :inner, []}

        :left ->
          if nested_assoc_fields == [] do
            join_filters =
              case Keyword.get(opts, :join_filters, []) do
                [[], []] -> []
                join_filters -> join_filters
              end

            {true, :left, List.wrap(join_filters)}
          else
            {true, :inner, []}
          end

        join_type when join_type in [:inner, :left] ->
          {true, join_type, []}
      end

    preload = Keyword.get(opts, :preload, false)
    preload_strategy = Keyword.get(opts, :preload_strategy, nil)
    authorizer = Keyword.get(opts, :authorizer, nil)

    assoc_data =
      assoc_data(
        source_binding,
        source_schema,
        assoc_field,
        join?,
        join_type,
        preload,
        preload_strategy,
        join_filters,
        authorizer
      )

    %{
      assoc_binding: assoc_binding,
      assoc_schema: assoc_schema
    } = assoc_data

    state = %{state | bindings: [assoc_binding | bindings]}

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
                  source_schema: assoc_schema
              },
              opts
            )
      }

    do_build([assoc_data | assoc_list], tail, state, opts)
  end

  defp do_build(assoc_list, [assoc_field | tail], state, opts) do
    do_build(assoc_list, [{assoc_field, []} | tail], state, opts)
  end

  defp assoc_data(
         source_binding,
         source_schema,
         assoc_field,
         join?,
         join_type,
         preload,
         preload_strategy,
         join_filters,
         authorizer
       ) do
    assoc_schema = source_schema._assoc_schema(assoc_field)
    cardinality = source_schema._assoc_cardinality(assoc_field)
    assoc_binding = source_schema._binding(assoc_field)

    {join?, join_type, auth_z_join_filters} =
      case authorizer &&
             authorizer.reject_unauthorized_assoc(source_schema, {assoc_field, assoc_schema}) do
        %{join: join, on: on, or_on: or_on} ->
          join_type =
            if join == :left || join_type == :left do
              :left
            else
              :inner
            end

          {true, join_type, [List.wrap(on), [or: List.wrap(or_on)]]}

        %{join: join, on: on} ->
          join_type =
            if join == :left || join_type == :left do
              :left
            else
              :inner
            end

          {true, join_type, [List.wrap(on), [or: []]]}

        nil ->
          {join?, join_type, []}
      end

    join_filters =
      ([join_filters] ++ [auth_z_join_filters])
      |> Enum.reject(&(&1 == []))

    %{
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema,
      cardinality: cardinality,
      has_joined: false,
      join?: join?,
      join_type: join_type,
      join_filters: join_filters,
      preload: preload,
      preload_strategy: preload_strategy,
      nested_assocs: [],
      source_binding: source_binding,
      source_schema: source_schema
    }
  end
end
