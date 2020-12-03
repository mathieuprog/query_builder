defmodule QueryBuilder.Token do
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

  This information allows the exposed functions such as `QueryBuilder.where/3` to join
  associations, refer to associations, etc.
  """
  def token(query, nil, value) do
    token(query, %{list_assoc_data: [], preload: []}, value)
  end

  def token(query, token, value) do
    source_schema = QueryBuilder.Utils.root_schema(query)

    state = %State{
      # the name of the binding of the query's root schema is the schema itself
      source_binding: source_schema,
      source_schema: source_schema,
      bindings: [source_schema]
    }

    list_assoc_data =
      token.list_assoc_data
      |> token(query, List.wrap(value), state)
      |> merge_assoc_data_in_token()

    %{
      list_assoc_data: list_assoc_data,
      preload: token.preload
    }
  end

  defp merge_assoc_data_in_token(list_assoc_data) do
    Enum.reduce(list_assoc_data, [], fn assoc_data, new_list_assoc_data ->
      acc_assoc_data_with_index =
        new_list_assoc_data
        |> Enum.with_index()
        |> Enum.find(fn {acc_assoc_data, _index} -> acc_assoc_data.assoc_binding == assoc_data.assoc_binding end)

      if acc_assoc_data_with_index do
        {acc_assoc_data, index} = acc_assoc_data_with_index

        nested_assocs = merge_assoc_data_in_token(acc_assoc_data.nested_assocs ++ assoc_data.nested_assocs)

        List.replace_at(new_list_assoc_data, index, Map.put(acc_assoc_data, :nested_assocs, nested_assocs))
      else
        [assoc_data | new_list_assoc_data]
      end
    end)
  end

  defp token(token, _, [], _), do: token

  defp token(token, query, [assoc_field | tail], state)
       when is_atom(assoc_field) do

    %{
      source_binding: source_binding,
      source_schema: source_schema,
      bindings: bindings
    } = state

    assoc_data = assoc_data(query, source_binding, source_schema, assoc_field)

    %{
      assoc_binding: assoc_binding,
      assoc_schema: assoc_schema,
      has_joined: has_joined
    } = assoc_data

    if has_joined do
      raise_if_already_bound(bindings, assoc_schema, assoc_binding)
    end

    state = %{state | bindings: [assoc_binding | bindings]}

    token([assoc_data | token], query, tail, state)
  end

  defp token(token, query, [{assoc_field, nested_assoc_fields} | tail], state) do
    %{
      source_binding: source_binding,
      source_schema: source_schema,
      bindings: bindings
    } = state

    assoc_data = assoc_data(query, source_binding, source_schema, assoc_field)

    %{
      assoc_binding: assoc_binding,
      assoc_schema: assoc_schema,
      has_joined: has_joined
    } = assoc_data

    if has_joined do
      raise_if_already_bound(bindings, assoc_schema, assoc_binding)
    end

    state = %{state | bindings: [assoc_binding | bindings]}

    assoc_data = %{
      assoc_data
      | nested_assocs:
          token([], query, List.wrap(nested_assoc_fields), %{
            state
            | source_binding: assoc_binding,
              source_schema: assoc_schema
          })
    }

    if !assoc_data.has_joined do
      raise_if_any_nested_assoc_has_joined(assoc_data.assoc_schema, assoc_data.nested_assocs)
    end

    token([assoc_data | token], query, tail, state)
  end

  defp assoc_data(query, source_binding, source_schema, assoc_field) do
    assoc_schema = assoc_schema(source_schema, assoc_field)
    cardinality = assoc_cardinality(source_schema, assoc_field)

    assoc_binding =
      with assoc_binding when not is_nil(assoc_binding) <- source_schema._binding(assoc_field) do
        assoc_binding
      else
        _ -> assoc_schema._binding()
      end

    has_joined = Ecto.Query.has_named_binding?(query, assoc_binding)

    %{
      assoc_binding: assoc_binding,
      assoc_field: assoc_field,
      assoc_schema: assoc_schema,
      cardinality: cardinality,
      has_joined: has_joined,
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

  defp raise_if_already_bound(bindings, schema, binding) do
    if Enum.member?(bindings, binding) do
      raise "trying to bind #{schema} multiple times with same binding name"
    end
  end

  defp raise_if_any_nested_assoc_has_joined(_, []), do: nil

  defp raise_if_any_nested_assoc_has_joined(schema, [
         %{has_joined: true, assoc_schema: nested_schema} | _tail
       ]) do
    raise "#{schema} has not been joined while nested #{nested_schema} has joined}"
  end

  defp raise_if_any_nested_assoc_has_joined(schema, [
         %{has_joined: false, nested_assocs: nested_assocs} | tail
       ]) do
    raise_if_any_nested_assoc_has_joined(schema, nested_assocs)
    raise_if_any_nested_assoc_has_joined(schema, tail)
  end
end
