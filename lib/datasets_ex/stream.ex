defmodule DatasetsEx.Stream do
  @moduledoc """
  Streaming support for large datasets using GenStage.

  Provides memory-efficient iteration over datasets without loading
  everything into memory at once.
  """

  alias DatasetsEx.Dataset

  @doc """
  Creates a lazy stream from a dataset.

  Returns a Stream that can be processed without loading the entire
  dataset into memory.

  ## Examples

      dataset
      |> DatasetsEx.Stream.lazy()
      |> Stream.take(100)
      |> Enum.to_list()
  """
  @spec lazy(Dataset.t()) :: Enumerable.t()
  def lazy(%Dataset{data: data}) when is_list(data) do
    Stream.each(data, & &1)
  end

  def lazy(%Dataset{splits: splits}) when map_size(splits) > 0 do
    splits
    |> Map.values()
    |> Stream.concat()
  end

  @doc """
  Creates a batched stream from a dataset.

  ## Options

    * `:batch_size` - Number of items per batch (default: 32)
    * `:drop_remainder` - Drop incomplete final batch (default: false)

  ## Examples

      dataset
      |> DatasetsEx.Stream.batch(batch_size: 32)
      |> Enum.each(fn batch ->
        # Process batch
      end)
  """
  @spec batch(Dataset.t(), keyword()) :: Enumerable.t()
  def batch(dataset, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 32)
    drop_remainder = Keyword.get(opts, :drop_remainder, false)

    stream = lazy(dataset)

    if drop_remainder do
      stream
      |> Stream.chunk_every(batch_size, batch_size, :discard)
    else
      stream
      |> Stream.chunk_every(batch_size)
    end
  end

  @doc """
  Streams data from a file without loading it all into memory.

  ## Options

    * `:format` - File format (:jsonl, :csv)
    * `:batch_size` - Number of items per batch

  ## Examples

      DatasetsEx.Stream.from_file("large_dataset.jsonl", format: :jsonl)
      |> Stream.take(1000)
      |> Enum.to_list()
  """
  @spec from_file(String.t(), keyword()) :: Enumerable.t()
  def from_file(path, opts \\ []) do
    format = Keyword.get(opts, :format, detect_format(path))

    case format do
      :jsonl -> stream_jsonl(path)
      :csv -> stream_csv(path, opts)
      _ -> raise "Unsupported format for streaming: #{format}"
    end
  end

  @doc """
  Applies a transformation to each item in the stream.

  ## Examples

      dataset
      |> DatasetsEx.Stream.lazy()
      |> DatasetsEx.Stream.map_stream(fn item ->
        %{item | text: String.downcase(item.text)}
      end)
  """
  @spec map_stream(Enumerable.t(), (term() -> term())) :: Enumerable.t()
  def map_stream(stream, fun) do
    Stream.map(stream, fun)
  end

  @doc """
  Filters items in the stream based on a predicate.

  ## Examples

      dataset
      |> DatasetsEx.Stream.lazy()
      |> DatasetsEx.Stream.filter_stream(fn item ->
        String.length(item.text) > 10
      end)
  """
  @spec filter_stream(Enumerable.t(), (term() -> boolean())) :: Enumerable.t()
  def filter_stream(stream, predicate) do
    Stream.filter(stream, predicate)
  end

  @doc """
  Parallelizes stream processing across multiple tasks.

  ## Options

    * `:max_concurrency` - Maximum concurrent tasks (default: System.schedulers_online())
    * `:ordered` - Maintain order of results (default: false)

  ## Examples

      dataset
      |> DatasetsEx.Stream.lazy()
      |> DatasetsEx.Stream.parallel_map(fn item ->
        # Expensive operation
        process(item)
      end, max_concurrency: 4)
  """
  @spec parallel_map(Enumerable.t(), (term() -> term()), keyword()) :: Enumerable.t()
  def parallel_map(stream, fun, opts \\ []) do
    max_concurrency = Keyword.get(opts, :max_concurrency, System.schedulers_online())
    ordered = Keyword.get(opts, :ordered, false)

    stream
    |> Task.async_stream(fun,
      max_concurrency: max_concurrency,
      ordered: ordered
    )
    |> Stream.map(fn {:ok, result} -> result end)
  end

  # Private Functions

  defp detect_format(path) do
    case Path.extname(path) do
      ".jsonl" -> :jsonl
      ".json" -> :json
      ".csv" -> :csv
      _ -> :unknown
    end
  end

  defp stream_jsonl(path) do
    path
    |> File.stream!()
    |> Stream.map(&String.trim/1)
    |> Stream.reject(&(&1 == ""))
    |> Stream.map(&Jason.decode!/1)
  end

  defp stream_csv(path, opts) do
    headers = Keyword.get(opts, :headers, true)

    base_stream =
      path
      |> File.stream!()
      |> NimbleCSV.RFC4180.parse_stream(skip_headers: false)

    if headers do
      base_stream
      |> Stream.transform(nil, fn row, header_row ->
        case header_row do
          nil ->
            # First row is headers
            {[], row}

          headers ->
            # Subsequent rows are data
            item =
              headers
              |> Enum.zip(row)
              |> Map.new()

            {[item], headers}
        end
      end)
    else
      base_stream
    end
  end
end
