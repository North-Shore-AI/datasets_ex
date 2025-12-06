defmodule DatasetsEx.Loaders.TruthfulQa do
  @moduledoc """
  Loader for the TruthfulQA dataset.

  TruthfulQA is a benchmark to measure whether a language model is
  truthful in generating answers to questions. It includes questions
  that some humans would answer falsely due to misconceptions.
  """

  alias DatasetsEx.Dataset

  @cache_dir "truthful_qa"

  @doc """
  Loads the TruthfulQA dataset.

  ## Options

    * `:split` - Load a specific split (:validation)
    * `:limit` - Limit the number of examples
    * `:offset` - Skip the first N examples
    * `:cache` - Use cached version if available (default: true)

  ## Examples

      {:ok, truthful_qa} = DatasetsEx.Loaders.TruthfulQa.load(info)
  """
  @spec load(map(), keyword()) :: {:ok, Dataset.t()}
  def load(_info, opts \\ []) do
    cache_path = get_cache_path()
    use_cache = Keyword.get(opts, :cache, true)

    dataset_file =
      if use_cache and File.exists?(cache_path) do
        load_from_cache(cache_path)
      else
        download_and_cache(cache_path)
      end

    dataset = build_dataset(dataset_file, opts)
    {:ok, dataset}
  end

  defp get_cache_path do
    Path.join([priv_dir(), "datasets", @cache_dir])
  end

  defp priv_dir do
    :code.priv_dir(:datasets_ex) |> to_string()
  end

  defp load_from_cache(cache_path) do
    Path.join(cache_path, "validation.jsonl")
  end

  defp download_and_cache(cache_path) do
    File.mkdir_p!(cache_path)
    file_path = Path.join(cache_path, "validation.jsonl")

    unless File.exists?(file_path) do
      File.write!(file_path, "")
    end

    file_path
  end

  defp build_dataset(file, opts) do
    limit = Keyword.get(opts, :limit)
    offset = Keyword.get(opts, :offset, 0)

    data = load_data(file, limit, offset)

    Dataset.new("truthful_qa",
      splits: %{validation: data},
      schema: :truthfulness,
      metadata: %{
        source: "TruthfulQA dataset",
        format: :jsonl,
        task: "truthfulness_evaluation",
        description: "Benchmark for truthful question answering",
        loaded_at: DateTime.utc_now()
      }
    )
  end

  defp load_data(file, limit, offset) do
    if File.exists?(file) and File.stat!(file).size > 0 do
      file
      |> File.stream!()
      |> Stream.map(&String.trim/1)
      |> Stream.reject(&(&1 == ""))
      |> Stream.drop(offset)
      |> maybe_take(limit)
      |> Stream.map(&Jason.decode!/1)
      |> Enum.to_list()
    else
      []
    end
  end

  defp maybe_take(stream, nil), do: stream
  defp maybe_take(stream, limit), do: Stream.take(stream, limit)
end
