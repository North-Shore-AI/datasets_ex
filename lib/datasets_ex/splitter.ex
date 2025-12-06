defmodule DatasetsEx.Splitter do
  @moduledoc """
  Dataset splitting for train/validation/test sets.
  """

  alias DatasetsEx.Dataset

  @doc """
  Splits a dataset into train and test sets.

  ## Options

    * `:ratio` - Train/test ratio (default: 0.8)
    * `:seed` - Random seed for reproducibility
    * `:shuffle` - Whether to shuffle before splitting (default: true)

  ## Examples

      {train, test} = DatasetsEx.Splitter.split(dataset, ratio: 0.8, seed: 42)
  """
  def split(dataset, opts \\ [])

  def split(%Dataset{data: data} = dataset, opts) when is_list(data) do
    ratio = Keyword.get(opts, :ratio, 0.8)
    seed = Keyword.get(opts, :seed)
    shuffle = Keyword.get(opts, :shuffle, true)

    data = if shuffle, do: shuffle_data(data, seed), else: data
    split_point = round(length(data) * ratio)

    {train_data, test_data} = Enum.split(data, split_point)

    train = %{dataset | data: train_data, splits: %{}}
    test = %{dataset | data: test_data, splits: %{}}

    {train, test}
  end

  def split(%Dataset{splits: splits} = dataset, opts) when map_size(splits) > 0 do
    # If dataset already has splits, combine them first
    all_data = splits |> Map.values() |> List.flatten()
    split(%{dataset | data: all_data, splits: %{}}, opts)
  end

  @doc """
  Splits a dataset into train, validation, and test sets.

  ## Options

    * `:ratios` - List of ratios [train, val, test] (default: [0.7, 0.15, 0.15])
    * `:seed` - Random seed for reproducibility
    * `:shuffle` - Whether to shuffle before splitting (default: true)

  ## Examples

      {train, val, test} = DatasetsEx.Splitter.split_three(dataset, ratios: [0.7, 0.15, 0.15])
  """
  def split_three(dataset, opts \\ [])

  def split_three(%Dataset{data: data} = dataset, opts) when is_list(data) do
    ratios = Keyword.get(opts, :ratios, [0.7, 0.15, 0.15])
    seed = Keyword.get(opts, :seed)
    shuffle = Keyword.get(opts, :shuffle, true)

    unless Enum.sum(ratios) == 1.0 do
      raise ArgumentError, "Ratios must sum to 1.0"
    end

    [train_ratio, val_ratio, _test_ratio] = ratios

    data = if shuffle, do: shuffle_data(data, seed), else: data
    total = length(data)

    train_size = round(total * train_ratio)
    val_size = round(total * val_ratio)

    {train_data, rest} = Enum.split(data, train_size)
    {val_data, test_data} = Enum.split(rest, val_size)

    train = %{dataset | data: train_data, splits: %{}}
    val = %{dataset | data: val_data, splits: %{}}
    test = %{dataset | data: test_data, splits: %{}}

    {train, val, test}
  end

  def split_three(%Dataset{splits: splits} = dataset, opts) when map_size(splits) > 0 do
    # If dataset already has splits, combine them first
    all_data = splits |> Map.values() |> List.flatten()
    split_three(%{dataset | data: all_data, splits: %{}}, opts)
  end

  @doc """
  Creates k-fold cross-validation splits.

  Returns a list of {train, test} tuples.

  ## Examples

      folds = DatasetsEx.Splitter.k_fold(dataset, k: 5, seed: 42)
  """
  def k_fold(dataset, opts \\ [])

  def k_fold(%Dataset{data: data} = dataset, opts) when is_list(data) do
    k = Keyword.get(opts, :k, 5)
    seed = Keyword.get(opts, :seed)
    shuffle = Keyword.get(opts, :shuffle, true)

    data = if shuffle, do: shuffle_data(data, seed), else: data
    fold_size = div(length(data), k)

    for i <- 0..(k - 1) do
      start_idx = i * fold_size
      end_idx = if i == k - 1, do: length(data), else: (i + 1) * fold_size

      test_data = Enum.slice(data, start_idx, end_idx - start_idx)
      train_data = Enum.slice(data, 0, start_idx) ++ Enum.slice(data, end_idx, length(data))

      train = %{dataset | data: train_data, splits: %{}}
      test = %{dataset | data: test_data, splits: %{}}

      {train, test}
    end
  end

  def k_fold(%Dataset{splits: splits} = dataset, opts) when map_size(splits) > 0 do
    all_data = splits |> Map.values() |> List.flatten()
    k_fold(%{dataset | data: all_data, splits: %{}}, opts)
  end

  @doc """
  Stratified split that maintains class distribution.

  ## Options

    * `:ratio` - Train/test ratio (default: 0.8)
    * `:label_key` - Key to extract labels from (default: :label)
    * `:seed` - Random seed for reproducibility

  ## Examples

      {train, test} = DatasetsEx.Splitter.stratified_split(dataset, label_key: :label, ratio: 0.8)
  """
  def stratified_split(%Dataset{data: data} = dataset, opts \\ []) when is_list(data) do
    ratio = Keyword.get(opts, :ratio, 0.8)
    label_key = Keyword.get(opts, :label_key, :label)
    seed = Keyword.get(opts, :seed)

    # Group by label
    grouped = Enum.group_by(data, &Map.get(&1, label_key))

    # Split each group
    {train_groups, test_groups} =
      grouped
      |> Enum.map(fn {label, examples} ->
        shuffled = shuffle_data(examples, seed)
        split_point = round(length(shuffled) * ratio)
        {train, test} = Enum.split(shuffled, split_point)
        {{label, train}, {label, test}}
      end)
      |> Enum.unzip()

    train_data = train_groups |> Enum.flat_map(fn {_label, examples} -> examples end)
    test_data = test_groups |> Enum.flat_map(fn {_label, examples} -> examples end)

    train = %{dataset | data: train_data, splits: %{}}
    test = %{dataset | data: test_data, splits: %{}}

    {train, test}
  end

  # Private Functions

  defp shuffle_data(data, nil) do
    Enum.shuffle(data)
  end

  defp shuffle_data(data, seed) do
    :rand.seed(:exsss, seed)
    Enum.shuffle(data)
  end
end
