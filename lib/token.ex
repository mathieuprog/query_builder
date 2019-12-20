defmodule QueryBuilder.Token do
  @moduledoc false

  defmodule State do
    defstruct source_binding: nil,
              source_schema: nil,
              bindings: []
  end

  def token(query, value) do
    source_schema = QueryBuilder.Utils.root_schema(query)

    state = %State{
      source_binding: source_schema,
      source_schema: source_schema
    }

    token([], query, List.wrap(value), state)
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
      with assoc_binding when not is_nil(assoc_binding) <- source_schema.binding(assoc_field) do
        assoc_binding
      else
        _ -> assoc_schema.binding()
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
