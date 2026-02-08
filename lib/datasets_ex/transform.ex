defmodule DatasetsEx.Transform do
  @moduledoc """
  Data transformation pipeline for preprocessing datasets.

  Provides text preprocessing, filtering, mapping, and augmentation
  operations that can be chained together.
  """

  alias DatasetsEx.Dataset

  @doc """
  Applies a transformation function to all items in a dataset.

  ## Examples

      dataset
      |> DatasetsEx.Transform.map(fn item ->
        %{item | text: String.downcase(item.text)}
      end)
  """
  @spec map(Dataset.t(), (map() -> map())) :: Dataset.t()
  def map(%Dataset{data: data} = dataset, fun) when is_list(data) do
    %{dataset | data: Enum.map(data, fun)}
  end

  def map(%Dataset{splits: splits} = dataset, fun) when map_size(splits) > 0 do
    transformed_splits =
      Map.new(splits, fn {split_name, split_data} ->
        {split_name, Enum.map(split_data, fun)}
      end)

    %{dataset | splits: transformed_splits}
  end

  @doc """
  Filters dataset items based on a predicate function.

  ## Examples

      dataset
      |> DatasetsEx.Transform.filter(fn item ->
        String.length(item.text) > 10
      end)
  """
  @spec filter(Dataset.t(), (map() -> boolean())) :: Dataset.t()
  def filter(%Dataset{data: data} = dataset, predicate) when is_list(data) do
    %{dataset | data: Enum.filter(data, predicate)}
  end

  def filter(%Dataset{splits: splits} = dataset, predicate) when map_size(splits) > 0 do
    filtered_splits =
      Map.new(splits, fn {split_name, split_data} ->
        {split_name, Enum.filter(split_data, predicate)}
      end)

    %{dataset | splits: filtered_splits}
  end

  @doc """
  Normalizes text by lowercasing, removing extra whitespace, etc.

  ## Options

    * `:lowercase` - Convert to lowercase (default: true)
    * `:trim` - Trim whitespace (default: true)
    * `:normalize_whitespace` - Replace multiple spaces with single space (default: true)
    * `:text_key` - Key containing text to normalize (default: :text)

  ## Examples

      dataset |> DatasetsEx.Transform.normalize_text(text_key: :content)
  """
  @spec normalize_text(Dataset.t(), keyword()) :: Dataset.t()
  def normalize_text(dataset, opts \\ []) do
    text_key = Keyword.get(opts, :text_key, :text)
    lowercase = Keyword.get(opts, :lowercase, true)
    trim = Keyword.get(opts, :trim, true)
    normalize_ws = Keyword.get(opts, :normalize_whitespace, true)

    map(dataset, fn item ->
      text = Map.get(item, text_key, "")

      normalized =
        text
        |> maybe_downcase(lowercase)
        |> maybe_trim(trim)
        |> maybe_normalize_whitespace(normalize_ws)

      Map.put(item, text_key, normalized)
    end)
  end

  @doc """
  Removes duplicate items from the dataset based on a key.

  ## Examples

      dataset |> DatasetsEx.Transform.deduplicate(:text)
  """
  @spec deduplicate(Dataset.t(), atom()) :: Dataset.t()
  def deduplicate(%Dataset{data: data} = dataset, key) when is_list(data) do
    deduplicated =
      data
      |> Enum.uniq_by(&Map.get(&1, key))

    %{dataset | data: deduplicated}
  end

  def deduplicate(%Dataset{splits: splits} = dataset, key) when map_size(splits) > 0 do
    dedup_splits =
      Map.new(splits, fn {split_name, split_data} ->
        {split_name, Enum.uniq_by(split_data, &Map.get(&1, key))}
      end)

    %{dataset | splits: dedup_splits}
  end

  @doc """
  Samples a random subset of the dataset.

  ## Examples

      dataset |> DatasetsEx.Transform.sample(100, seed: 42)
  """
  @spec sample(Dataset.t(), non_neg_integer(), keyword()) :: Dataset.t()
  def sample(dataset, n, opts \\ [])

  def sample(%Dataset{data: data} = dataset, n, opts) when is_list(data) do
    seed = Keyword.get(opts, :seed)

    sampled =
      if seed do
        :rand.seed(:exsss, seed)
        Enum.take_random(data, n)
      else
        Enum.take_random(data, n)
      end

    %{dataset | data: sampled}
  end

  def sample(%Dataset{splits: splits} = dataset, n, opts) when map_size(splits) > 0 do
    seed = Keyword.get(opts, :seed)

    if seed, do: :rand.seed(:exsss, seed)

    sampled_splits =
      Map.new(splits, fn {split_name, split_data} ->
        {split_name, Enum.take_random(split_data, min(n, length(split_data)))}
      end)

    %{dataset | splits: sampled_splits}
  end

  @doc """
  Adds noise to text for data augmentation.

  ## Options

    * `:text_key` - Key containing text (default: :text)
    * `:char_noise_prob` - Probability of character-level noise (default: 0.05)
    * `:word_drop_prob` - Probability of dropping a word (default: 0.1)
    * `:seed` - Random seed

  ## Examples

      dataset |> DatasetsEx.Transform.add_text_noise(char_noise_prob: 0.1)
  """
  @spec add_text_noise(Dataset.t(), keyword()) :: Dataset.t()
  def add_text_noise(dataset, opts \\ []) do
    text_key = Keyword.get(opts, :text_key, :text)
    char_noise_prob = Keyword.get(opts, :char_noise_prob, 0.05)
    word_drop_prob = Keyword.get(opts, :word_drop_prob, 0.1)
    seed = Keyword.get(opts, :seed)

    if seed, do: :rand.seed(:exsss, seed)

    map(dataset, fn item ->
      text = Map.get(item, text_key, "")
      noisy_text = apply_text_noise(text, char_noise_prob, word_drop_prob)
      Map.put(item, text_key, noisy_text)
    end)
  end

  @doc """
  Balances dataset classes by undersampling or oversampling.

  ## Options

    * `:label_key` - Key containing labels (default: :label)
    * `:strategy` - :undersample or :oversample (default: :undersample)
    * `:seed` - Random seed

  ## Examples

      dataset |> DatasetsEx.Transform.balance_classes(strategy: :oversample)
  """
  @spec balance_classes(Dataset.t(), keyword()) :: Dataset.t()
  def balance_classes(dataset, opts \\ [])

  def balance_classes(%Dataset{data: data} = dataset, opts) when is_list(data) do
    label_key = Keyword.get(opts, :label_key, :label)
    strategy = Keyword.get(opts, :strategy, :undersample)
    seed = Keyword.get(opts, :seed)

    if seed, do: :rand.seed(:exsss, seed)

    grouped = Enum.group_by(data, &Map.get(&1, label_key))

    balanced =
      case strategy do
        :undersample ->
          undersample_groups(grouped)

        :oversample ->
          oversample_groups(grouped)
      end

    %{dataset | data: balanced}
  end

  def balance_classes(%Dataset{splits: splits} = dataset, opts) when map_size(splits) > 0 do
    # Balance each split independently
    balanced_splits =
      Map.new(splits, fn {split_name, split_data} ->
        temp_dataset = %{dataset | data: split_data, splits: %{}}
        balanced = balance_classes(temp_dataset, opts)
        {split_name, balanced.data}
      end)

    %{dataset | splits: balanced_splits}
  end

  # Private Functions

  defp apply_text_noise(text, char_noise_prob, word_drop_prob) do
    text
    |> String.split()
    |> Enum.reject(fn _word -> :rand.uniform() < word_drop_prob end)
    |> Enum.map_join(" ", fn word ->
      if :rand.uniform() < char_noise_prob do
        apply_char_noise(word)
      else
        word
      end
    end)
  end

  defp apply_char_noise(word) do
    chars = String.graphemes(word)
    noise_type = :rand.uniform(3)

    case noise_type do
      1 -> swap_adjacent(chars, word)
      2 -> delete_char(chars, word)
      3 -> duplicate_char(chars)
    end
  end

  defp maybe_downcase(text, true), do: String.downcase(text)
  defp maybe_downcase(text, false), do: text

  defp maybe_trim(text, true), do: String.trim(text)
  defp maybe_trim(text, false), do: text

  defp maybe_normalize_whitespace(text, true), do: String.replace(text, ~r/\s+/, " ")
  defp maybe_normalize_whitespace(text, false), do: text

  defp swap_adjacent(chars, original) do
    if length(chars) > 1 do
      idx = :rand.uniform(length(chars) - 1) - 1

      chars
      |> List.update_at(idx, fn c -> Enum.at(chars, idx + 1, c) end)
      |> List.update_at(idx + 1, fn _ -> Enum.at(chars, idx) end)
      |> Enum.join()
    else
      original
    end
  end

  defp delete_char(chars, original) do
    if length(chars) > 1 do
      idx = :rand.uniform(length(chars)) - 1
      List.delete_at(chars, idx) |> Enum.join()
    else
      original
    end
  end

  defp duplicate_char(chars) do
    idx = :rand.uniform(length(chars)) - 1
    char = Enum.at(chars, idx)
    List.insert_at(chars, idx, char) |> Enum.join()
  end

  defp undersample_groups(grouped) do
    min_size = grouped |> Map.values() |> Enum.map(&length/1) |> Enum.min()

    Enum.flat_map(grouped, fn {_label, items} ->
      Enum.take_random(items, min_size)
    end)
  end

  defp oversample_groups(grouped) do
    max_size = grouped |> Map.values() |> Enum.map(&length/1) |> Enum.max()

    Enum.flat_map(grouped, fn {_label, items} ->
      oversample_items(items, max_size)
    end)
  end

  defp oversample_items(items, max_size) do
    if length(items) < max_size do
      needed = max_size - length(items)
      items ++ Enum.take_random(items, needed)
    else
      items
    end
  end
end
