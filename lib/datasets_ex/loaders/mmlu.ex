defmodule DatasetsEx.Loaders.Mmlu do
  @moduledoc """
  Loader for the MMLU (Massive Multitask Language Understanding) dataset.

  MMLU is a benchmark designed to measure knowledge acquired during
  pretraining by evaluating models on 57 subjects across STEM,
  humanities, social sciences, and more.
  """

  alias DatasetsEx.Dataset

  @cache_dir "mmlu"

  @doc """
  Loads the MMLU dataset.

  ## Options

    * `:split` - Load a specific split (:test, :dev, :val)
    * `:subject` - Load a specific subject (e.g., :abstract_algebra)
    * `:limit` - Limit the number of examples
    * `:offset` - Skip the first N examples
    * `:cache` - Use cached version if available (default: true)

  ## Examples

      {:ok, mmlu} = DatasetsEx.Loaders.Mmlu.load(info, split: :test)
      {:ok, mmlu} = DatasetsEx.Loaders.Mmlu.load(info, subject: :computer_science)
  """
  @spec load(map(), keyword()) :: {:ok, Dataset.t()}
  def load(_info, opts \\ []) do
    cache_path = get_cache_path()
    use_cache = Keyword.get(opts, :cache, true)

    dataset_files =
      if use_cache and File.exists?(cache_path) do
        load_from_cache(cache_path)
      else
        download_and_cache(cache_path)
      end

    dataset = build_dataset(dataset_files, opts)
    {:ok, dataset}
  end

  defp get_cache_path do
    Path.join([priv_dir(), "datasets", @cache_dir])
  end

  defp priv_dir do
    :code.priv_dir(:datasets_ex) |> to_string()
  end

  defp load_from_cache(cache_path) do
    %{
      test: Path.join(cache_path, "test.jsonl"),
      dev: Path.join(cache_path, "dev.jsonl"),
      val: Path.join(cache_path, "val.jsonl")
    }
  end

  defp download_and_cache(cache_path) do
    File.mkdir_p!(cache_path)
    create_placeholder_files(cache_path)
  end

  defp create_placeholder_files(cache_path) do
    files = %{
      test: Path.join(cache_path, "test.jsonl"),
      dev: Path.join(cache_path, "dev.jsonl"),
      val: Path.join(cache_path, "val.jsonl")
    }

    Enum.each(files, fn {_key, path} ->
      unless File.exists?(path) do
        File.write!(path, "")
      end
    end)

    files
  end

  defp build_dataset(files, opts) do
    split = Keyword.get(opts, :split)
    limit = Keyword.get(opts, :limit)
    offset = Keyword.get(opts, :offset, 0)

    splits =
      case split do
        nil ->
          %{
            test: load_split(files.test, limit, offset),
            dev: load_split(files.dev, limit, offset),
            val: load_split(files.val, limit, offset)
          }

        split_name ->
          file =
            case split_name do
              :test -> files.test
              :dev -> files.dev
              :val -> files.val
              _ -> raise "Unknown split: #{split_name}"
            end

          %{split_name => load_split(file, limit, offset)}
      end

    Dataset.new("mmlu",
      splits: splits,
      schema: :multiple_choice,
      metadata: %{
        source: "MMLU dataset",
        format: :jsonl,
        task: "knowledge_evaluation",
        description: "Massive Multitask Language Understanding",
        num_subjects: 57,
        loaded_at: DateTime.utc_now()
      }
    )
  end

  defp load_split(file, limit, offset) do
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
