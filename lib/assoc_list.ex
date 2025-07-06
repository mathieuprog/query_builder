defmodule QueryBuilder.AssocList do
  @moduledoc false

  defmodule State do
    @moduledoc false

    # The `token` function below received way too many arguments (which made the code
    # harder to read and led `mix format` to split the arguments over multiple lines).
    #
    # The purpose of this struct is to reduce the number of arguments and to maintain
    # state between `token`'s recursive calls, hence its name.

    defstruct source_binding: nil,
              source_schema: nil,
              # `bindings` allows to keep track of all the binding names in order to
              # detect a binding name that is going to be used twice when joining
              # associations; in such case, the `token` function raises an error.
              bindings: []
  end

  @doc ~S"""
  The purpose of the `token/2` function is to generate a data structure containing
  information about given association tree.

  It receives a query and a list (with nested lists) of association fields (atoms).
  For example:
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
    * `join_type`: `:left` or `inner` (atom)
    * `join_filters`: only in case of a left join, clauses for the `:on` option (list of
    two keyword lists – and/or clauses)
    * `preload`: is to be preloaded or not (boolean)

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
          nested_assocs =
            merge_assoc_data(acc_assoc_data.nested_assocs ++ assoc_data.nested_assocs)

          join_type =
            cond do
              acc_assoc_data.join_type == assoc_data.join_type ->
                assoc_data.join_type

              acc_assoc_data.join_type == :left || assoc_data.join_type == :left ->
                :left

              true ->
                :inner
            end

          preload = acc_assoc_data.preload || assoc_data.preload

          join_filters =
            (acc_assoc_data.join_filters ++ assoc_data.join_filters)
            |> Enum.uniq()
            |> Enum.reject(&(&1 == []))

          new_assoc_data =
            acc_assoc_data
            |> Map.put(:nested_assocs, nested_assocs)
            |> Map.put(:join_type, join_type)
            |> Map.put(:join_filters, join_filters)
            |> Map.put(:preload, preload)

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

    # Convert string association fields to atoms
    assoc_field =
      if is_binary(assoc_field), do: String.to_existing_atom(assoc_field), else: assoc_field

    {join_type, join_filters} =
      case Keyword.get(opts, :join, :inner) do
        :left ->
          if nested_assoc_fields == [] do
            join_filters =
              case Keyword.get(opts, :join_filters, []) do
                [[], []] -> []
                join_filters -> join_filters
              end

            {:left, List.wrap(join_filters)}
          else
            {:inner, []}
          end

        join_type ->
          {join_type, []}
      end

    preload = Keyword.get(opts, :preload, false)
    authorizer = Keyword.get(opts, :authorizer, nil)

    assoc_data =
      assoc_data(
        source_binding,
        source_schema,
        assoc_field,
        join_type,
        preload,
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
    # Convert string to atom if needed before delegating
    assoc_field =
      if is_binary(assoc_field), do: String.to_existing_atom(assoc_field), else: assoc_field

    do_build(assoc_list, [{assoc_field, []} | tail], state, opts)
  end

  defp assoc_data(
         source_binding,
         source_schema,
         assoc_field,
         join_type,
         preload,
         join_filters,
         authorizer
       ) do
    assoc_schema = assoc_schema(source_schema, assoc_field)
    cardinality = assoc_cardinality(source_schema, assoc_field)

    assoc_binding =
      with assoc_binding when not is_nil(assoc_binding) <- source_schema._binding(assoc_field) do
        assoc_binding
      else
        _ -> assoc_schema._binding()
      end

    {join_type, auth_z_join_filters} =
      case authorizer &&
             authorizer.reject_unauthorized_assoc(source_schema, {assoc_field, assoc_schema}) do
        %{join: join, on: on, or_on: or_on} ->
          {cond do
             join == :left || join_type == :left -> :left
             true -> :inner
           end, [List.wrap(on), [or: List.wrap(or_on)]]}

        %{join: join, on: on} ->
          {cond do
             join == :left || join_type == :left -> :left
             true -> :inner
           end, [List.wrap(on), [or: []]]}

        nil ->
          {join_type, []}
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
      join_type: join_type,
      join_filters: join_filters,
      preload: preload,
      nested_assocs: [],
      source_binding: source_binding,
      source_schema: source_schema
    }
  end

  defp assoc_schema(source_schema, assoc_field) do
    assoc_data = source_schema.__schema__(:association, assoc_field)

    if assoc_data do
      assoc_data.queryable
    else
      raise "association :" <>
              to_string(assoc_field) <> " not found in " <> to_string(source_schema)
    end
  end

  defp assoc_cardinality(source_schema, assoc_field) do
    source_schema.__schema__(:association, assoc_field).cardinality
  end
end
