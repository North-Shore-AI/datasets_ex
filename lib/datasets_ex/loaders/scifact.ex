defmodule DatasetsEx.Loaders.Scifact do
  @moduledoc """
  Loader for the SciFact dataset.
  """

  alias DatasetsEx.Dataset

  @cache_dir "scifact"

  def load(info, opts \\ []) do
    cache_path = get_cache_path()
    use_cache = Keyword.get(opts, :cache, true)

    dataset_files =
      if use_cache and File.exists?(cache_path) do
        load_from_cache(cache_path)
      else
        download_and_cache(info, cache_path)
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
      claims_train: Path.join(cache_path, "claims_train.jsonl"),
      claims_test: Path.join(cache_path, "claims_test.jsonl"),
      corpus: Path.join(cache_path, "corpus.jsonl")
    }
  end

  defp download_and_cache(info, cache_path) do
    File.mkdir_p!(cache_path)

    # For now, we'll create placeholder files
    # In production, this would download from info.source
    create_placeholder_files(cache_path)
  end

  defp create_placeholder_files(cache_path) do
    files = %{
      claims_train: Path.join(cache_path, "claims_train.jsonl"),
      claims_test: Path.join(cache_path, "claims_test.jsonl"),
      corpus: Path.join(cache_path, "corpus.jsonl")
    }

    # Create empty files if they don't exist
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
          # Load all splits
          %{
            train: load_split(files.claims_train, limit, offset),
            test: load_split(files.claims_test, limit, offset)
          }

        split_name ->
          # Load specific split
          file =
            case split_name do
              :train -> files.claims_train
              :test -> files.claims_test
              _ -> raise "Unknown split: #{split_name}"
            end

          %{split_name => load_split(file, limit, offset)}
      end

    Dataset.new("scifact",
      splits: splits,
      schema: :claim_evidence,
      metadata: %{
        source: "SciFact dataset",
        format: :jsonl,
        task: "claim_verification",
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
