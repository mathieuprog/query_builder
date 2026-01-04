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
    * `join_type`: `:left`, `:inner`, or `:any` (atom; only meaningful when `join?` is true).
    `:any` means “no guarantee”: if the join already exists, accept `:left` or `:inner`;
    if QueryBuilder needs to emit the join, it defaults to `:left`.
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

          join? = acc_assoc_data.join? || assoc_data.join?

          join_type =
            merge_join_types!(
              acc_assoc_data.join_type,
              assoc_data.join_type,
              acc_assoc_data.source_schema,
              acc_assoc_data.assoc_field
            )

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
      source_schema: source_schema
    } = state

    {assoc_field, join_type_marker} = normalize_assoc_field_and_join_type!(assoc_field)

    {join?, join_type, join_filters} =
      case Keyword.get(opts, :join, :any) do
        :none ->
          {false, :any, []}

        :left ->
          left_join_mode = Keyword.get(opts, :left_join_mode, :leaf)

          if nested_assoc_fields == [] do
            join_filters =
              case Keyword.get(opts, :join_filters, []) do
                [[], []] -> []
                join_filters -> join_filters
              end

            {true, :left, List.wrap(join_filters)}
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

        join_type when join_type in [:inner, :left, :any] ->
          {true, join_type, []}

        other ->
          raise ArgumentError,
                "invalid join option #{inspect(other)}; expected :none, :left, :inner, or :any"
      end

    join_type_marker =
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

    join_type =
      merge_join_types!(join_type, join_type_marker, source_schema, assoc_field)

    preload = Keyword.get(opts, :preload, false)
    preload_strategy = Keyword.get(opts, :preload_strategy, nil)

    assoc_data =
      assoc_data(
        source_binding,
        source_schema,
        assoc_field,
        join?,
        join_type,
        preload,
        preload_strategy,
        join_filters
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
                  lock_left?: state.lock_left? || join_type == :left
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

        {String.to_existing_atom(base), :left}

      String.ends_with?(assoc_field_string, "!") ->
        base = String.trim_trailing(assoc_field_string, "!")

        if base == "" do
          raise ArgumentError,
                "invalid assoc field #{inspect(assoc_field)} (cannot be only a marker)"
        end

        {String.to_existing_atom(base), :inner}

      true ->
        {assoc_field, :any}
    end
  end

  defp normalize_assoc_field_and_join_type!(assoc_field) do
    raise ArgumentError,
          "invalid assoc field #{inspect(assoc_field)}; expected an association atom (optionally suffixed with ? or !)"
  end

  defp merge_join_types!(left, right, source_schema, assoc_field) do
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

  defp assoc_data(
         source_binding,
         source_schema,
         assoc_field,
         join?,
         join_type,
         preload,
         preload_strategy,
         join_filters
       ) do
    assoc_schema = source_schema._assoc_schema(assoc_field)
    cardinality = source_schema._assoc_cardinality(assoc_field)
    assoc_binding = source_schema._binding(assoc_field)

    join_filters = if join_filters == [], do: [], else: [join_filters]

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
