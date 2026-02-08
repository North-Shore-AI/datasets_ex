defmodule DatasetsEx.Quality do
  @moduledoc """
  Data quality checks and validation for datasets.

  Provides schema validation, duplicate detection, distribution analysis,
  and data profiling capabilities.
  """

  alias DatasetsEx.Dataset

  @doc """
  Validates dataset schema compliance.

  ## Options

    * `:required_keys` - List of required keys in each item
    * `:allowed_keys` - List of allowed keys (if provided, extra keys are errors)
    * `:type_checks` - Map of key => type_check_function

  ## Examples

      DatasetsEx.Quality.validate_schema(dataset,
        required_keys: [:text, :label],
        type_checks: %{text: &is_binary/1, label: &is_atom/1}
      )
  """
  @spec validate_schema(Dataset.t(), keyword()) ::
          {:ok, map()} | {:error, list(String.t())}
  def validate_schema(dataset, opts \\ []) do
    required_keys = Keyword.get(opts, :required_keys, [])
    allowed_keys = Keyword.get(opts, :allowed_keys)
    type_checks = Keyword.get(opts, :type_checks, %{})

    data = get_all_data(dataset)
    errors = []

    errors =
      Enum.reduce(Enum.with_index(data), errors, fn {item, idx}, acc ->
        item_errors =
          []
          |> check_required_keys(item, required_keys, idx)
          |> check_allowed_keys(item, allowed_keys, idx)
          |> check_types(item, type_checks, idx)

        acc ++ item_errors
      end)

    if Enum.empty?(errors) do
      {:ok, %{valid: true, total_items: length(data)}}
    else
      {:error, errors}
    end
  end

  @doc """
  Detects duplicate items in the dataset.

  ## Options

    * `:key` - Key to check for duplicates (default: checks entire item)
    * `:ignore_case` - Ignore case when comparing strings (default: false)

  ## Examples

      DatasetsEx.Quality.detect_duplicates(dataset, key: :text)
  """
  @spec detect_duplicates(Dataset.t(), keyword()) :: map()
  def detect_duplicates(dataset, opts \\ []) do
    key = Keyword.get(opts, :key)
    ignore_case = Keyword.get(opts, :ignore_case, false)

    data = get_all_data(dataset)
    total_items = length(data)

    {duplicates, total} =
      data
      |> Enum.with_index()
      |> Enum.group_by(fn {item, _idx} -> duplicate_key(item, key, ignore_case) end)
      |> Enum.reduce({[], 0}, &accumulate_duplicates/2)

    %{
      total_items: total_items,
      duplicate_groups: length(duplicates),
      duplicate_items: total,
      duplicate_rate: if(total_items > 0, do: total / total_items, else: 0.0),
      duplicates: Enum.take(duplicates, 10)
    }
  end

  @doc """
  Analyzes label distribution in the dataset.

  ## Options

    * `:label_key` - Key containing labels (default: :label)

  ## Examples

      DatasetsEx.Quality.label_distribution(dataset)
  """
  @spec label_distribution(Dataset.t(), keyword()) :: map()
  def label_distribution(dataset, opts \\ []) do
    label_key = Keyword.get(opts, :label_key, :label)
    data = get_all_data(dataset)

    total = length(data)

    distribution =
      data
      |> Enum.group_by(&Map.get(&1, label_key))
      |> Map.new(fn {label, items} ->
        count = length(items)

        {label,
         %{
           count: count,
           percentage: if(total > 0, do: count / total * 100, else: 0.0)
         }}
      end)

    %{
      total_items: total,
      num_classes: map_size(distribution),
      distribution: distribution,
      is_balanced: check_balance(distribution)
    }
  end

  @doc """
  Profiles dataset characteristics.

  Returns statistics about text length, vocabulary, missing values, etc.

  ## Options

    * `:text_key` - Key containing text to analyze (default: :text)
    * `:compute_vocab` - Compute vocabulary stats (default: true)

  ## Examples

      DatasetsEx.Quality.profile(dataset)
  """
  @spec profile(Dataset.t(), keyword()) :: map()
  def profile(dataset, opts \\ []) do
    text_key = Keyword.get(opts, :text_key, :text)
    compute_vocab = Keyword.get(opts, :compute_vocab, true)

    data = get_all_data(dataset)
    total = length(data)

    # Basic stats
    basic_stats = %{
      total_items: total,
      splits: if(map_size(dataset.splits) > 0, do: Map.keys(dataset.splits), else: [:unsplit])
    }

    # Text length stats
    text_lengths =
      data
      |> Enum.map(fn item ->
        text = Map.get(item, text_key, "")
        String.length(text)
      end)

    text_stats =
      if Enum.any?(text_lengths) do
        %{
          min_length: Enum.min(text_lengths),
          max_length: Enum.max(text_lengths),
          mean_length: Enum.sum(text_lengths) / length(text_lengths),
          median_length: median(text_lengths)
        }
      else
        %{}
      end

    # Vocabulary stats
    vocab_stats =
      if compute_vocab do
        tokens =
          data
          |> Enum.flat_map(fn item ->
            text = Map.get(item, text_key, "")
            String.split(text)
          end)

        unique_tokens = Enum.uniq(tokens)

        %{
          total_tokens: length(tokens),
          unique_tokens: length(unique_tokens),
          vocabulary_size: length(unique_tokens)
        }
      else
        %{}
      end

    # Missing values
    all_keys =
      data
      |> Enum.flat_map(&Map.keys/1)
      |> Enum.uniq()

    missing_stats =
      Map.new(all_keys, fn key ->
        missing_count =
          Enum.count(data, fn item ->
            is_nil(Map.get(item, key)) or Map.get(item, key) == ""
          end)

        {key,
         %{
           missing: missing_count,
           present: total - missing_count,
           missing_rate: if(total > 0, do: missing_count / total, else: 0.0)
         }}
      end)

    Map.merge(basic_stats, %{
      text_stats: text_stats,
      vocabulary: vocab_stats,
      missing_values: missing_stats
    })
  end

  @doc """
  Checks for outliers in numeric fields.

  ## Options

    * `:field` - Numeric field to check
    * `:method` - :iqr (interquartile range) or :zscore (default: :iqr)
    * `:threshold` - Threshold for outlier detection (default: 1.5 for IQR, 3 for z-score)

  ## Examples

      DatasetsEx.Quality.detect_outliers(dataset, field: :score)
  """
  @spec detect_outliers(Dataset.t(), keyword()) :: map()
  def detect_outliers(dataset, opts \\ []) do
    field = Keyword.fetch!(opts, :field)
    method = Keyword.get(opts, :method, :iqr)
    threshold = Keyword.get(opts, :threshold, if(method == :iqr, do: 1.5, else: 3))

    data = get_all_data(dataset)

    values =
      data
      |> Enum.map(&Map.get(&1, field))
      |> Enum.reject(&is_nil/1)
      |> Enum.sort()

    outlier_indices =
      case method do
        :iqr ->
          detect_outliers_iqr(data, field, values, threshold)

        :zscore ->
          detect_outliers_zscore(data, field, values, threshold)
      end

    %{
      total_items: length(data),
      outlier_count: length(outlier_indices),
      outlier_rate: length(outlier_indices) / length(data),
      outlier_indices: Enum.take(outlier_indices, 20)
    }
  end

  # Private Functions

  defp get_all_data(%Dataset{data: data}) when is_list(data), do: data

  defp get_all_data(%Dataset{splits: splits}) when map_size(splits) > 0 do
    splits |> Map.values() |> List.flatten()
  end

  defp get_all_data(_), do: []

  defp check_required_keys(errors, item, required_keys, idx) do
    missing_keys = required_keys -- Map.keys(item)

    if Enum.any?(missing_keys) do
      ["Item #{idx}: Missing required keys: #{inspect(missing_keys)}" | errors]
    else
      errors
    end
  end

  defp check_allowed_keys(errors, _item, nil, _idx), do: errors

  defp check_allowed_keys(errors, item, allowed_keys, idx) do
    extra_keys = Map.keys(item) -- allowed_keys

    if Enum.any?(extra_keys) do
      ["Item #{idx}: Contains disallowed keys: #{inspect(extra_keys)}" | errors]
    else
      errors
    end
  end

  defp check_types(errors, item, type_checks, idx) do
    Enum.reduce(type_checks, errors, fn {key, check_fn}, acc ->
      value = Map.get(item, key)

      if value && !check_fn.(value) do
        ["Item #{idx}: Type check failed for key '#{key}'" | acc]
      else
        acc
      end
    end)
  end

  defp duplicate_key(item, key, ignore_case) do
    value = if key, do: Map.get(item, key), else: item

    if ignore_case and is_binary(value) do
      String.downcase(value)
    else
      value
    end
  end

  defp accumulate_duplicates({_value, items}, {dupes, count}) do
    case duplicate_entry(items) do
      nil ->
        {dupes, count}

      %{count: item_count} = entry ->
        {[entry | dupes], count + item_count}
    end
  end

  defp duplicate_entry(items) do
    item_count = length(items)

    if item_count > 1 do
      indices = Enum.map(items, fn {_item, idx} -> idx end)
      %{count: item_count, indices: indices}
    end
  end

  defp check_balance(distribution) do
    counts = distribution |> Map.values() |> Enum.map(& &1.count)

    if Enum.empty?(counts) do
      true
    else
      min_count = Enum.min(counts)
      max_count = Enum.max(counts)
      # Consider balanced if max is at most 1.5x min
      max_count / min_count <= 1.5
    end
  end

  defp median(list) when list == [], do: 0

  defp median(list) do
    sorted = Enum.sort(list)
    mid = div(length(sorted), 2)

    if rem(length(sorted), 2) == 0 do
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
    else
      Enum.at(sorted, mid)
    end
  end

  defp detect_outliers_iqr(data, field, values, threshold) do
    q1_idx = div(length(values), 4)
    q3_idx = div(3 * length(values), 4)

    q1 = Enum.at(values, q1_idx, 0)
    q3 = Enum.at(values, q3_idx, 0)
    iqr = q3 - q1

    lower_bound = q1 - threshold * iqr
    upper_bound = q3 + threshold * iqr

    data
    |> Enum.with_index()
    |> Enum.filter(fn {item, _idx} ->
      value = Map.get(item, field)
      value && (value < lower_bound || value > upper_bound)
    end)
    |> Enum.map(fn {_item, idx} -> idx end)
  end

  defp detect_outliers_zscore(data, field, values, threshold) do
    mean = Enum.sum(values) / length(values)

    std_dev =
      values
      |> Enum.map(fn v -> :math.pow(v - mean, 2) end)
      |> Enum.sum()
      |> then(fn sum -> :math.sqrt(sum / length(values)) end)

    data
    |> Enum.with_index()
    |> Enum.filter(fn {item, _idx} ->
      value = Map.get(item, field)

      if value && std_dev > 0 do
        z_score = abs((value - mean) / std_dev)
        z_score > threshold
      else
        false
      end
    end)
    |> Enum.map(fn {_item, idx} -> idx end)
  end
end
