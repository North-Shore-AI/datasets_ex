defmodule DatasetsEx.Loader do
  @moduledoc """
  Dataset loader for various formats and sources.
  """

  alias DatasetsEx.{Dataset, Registry}

  @doc """
  Loads a dataset by name.

  ## Options

    * `:split` - Load a specific split (e.g., :train, :test)
    * `:limit` - Limit the number of examples
    * `:offset` - Skip the first N examples
    * `:version` - Load a specific version
    * `:cache` - Use cached version if available (default: true)

  ## Examples

      {:ok, dataset} = DatasetsEx.Loader.load(:scifact)
      {:ok, dataset} = DatasetsEx.Loader.load(:fever, split: :train, limit: 1000)
  """
  def load(name, opts \\ []) do
    with {:ok, info} <- get_dataset_info(name),
         {:ok, loader_module} <- get_loader_module(name),
         {:ok, dataset} <- loader_module.load(info, opts) do
      {:ok, dataset}
    end
  end

  @doc """
  Loads a dataset from a local file.

  ## Options

    * `:format` - File format (:jsonl, :csv, :json)
    * `:schema` - Dataset schema
  """
  def load_file(path, opts \\ []) do
    format = Keyword.get(opts, :format, detect_format(path))

    case format do
      :jsonl -> load_jsonl(path, opts)
      :json -> load_json(path, opts)
      :csv -> load_csv(path, opts)
      _ -> {:error, :unsupported_format}
    end
  end

  @doc """
  Loads JSONL file.
  """
  def load_jsonl(path, opts \\ []) do
    name = Keyword.get(opts, :name, Path.basename(path, ".jsonl"))

    data =
      path
      |> File.stream!()
      |> Stream.map(&String.trim/1)
      |> Stream.reject(&(&1 == ""))
      |> Stream.map(&Jason.decode!/1)
      |> maybe_limit(opts)
      |> Enum.to_list()

    dataset =
      Dataset.new(name,
        data: data,
        schema: Keyword.get(opts, :schema),
        metadata: %{
          source: path,
          format: :jsonl,
          loaded_at: DateTime.utc_now()
        }
      )

    {:ok, dataset}
  rescue
    error -> {:error, error}
  end

  @doc """
  Loads JSON file.
  """
  def load_json(path, opts \\ []) do
    name = Keyword.get(opts, :name, Path.basename(path, ".json"))

    data =
      path
      |> File.read!()
      |> Jason.decode!()
      |> ensure_list()
      |> maybe_limit(opts)

    dataset =
      Dataset.new(name,
        data: data,
        schema: Keyword.get(opts, :schema),
        metadata: %{
          source: path,
          format: :json,
          loaded_at: DateTime.utc_now()
        }
      )

    {:ok, dataset}
  rescue
    error -> {:error, error}
  end

  @doc """
  Loads CSV file.
  """
  def load_csv(path, opts \\ []) do
    name = Keyword.get(opts, :name, Path.basename(path, ".csv"))
    headers = Keyword.get(opts, :headers, true)

    # Parse all rows first
    all_rows =
      path
      |> File.stream!()
      |> NimbleCSV.RFC4180.parse_stream(skip_headers: false)
      |> Enum.to_list()

    data =
      if headers and length(all_rows) > 0 do
        [header_row | data_rows] = all_rows

        data_rows
        |> maybe_limit_list(opts)
        |> Enum.map(fn row ->
          header_row
          |> Enum.zip(row)
          |> Map.new()
        end)
      else
        all_rows
        |> maybe_limit_list(opts)
      end

    dataset =
      Dataset.new(name,
        data: data,
        schema: Keyword.get(opts, :schema),
        metadata: %{
          source: path,
          format: :csv,
          loaded_at: DateTime.utc_now()
        }
      )

    {:ok, dataset}
  rescue
    error -> {:error, error}
  end

  # Private Functions

  defp get_dataset_info(name) do
    case Registry.info(name) do
      nil -> {:error, {:unknown_dataset, name}}
      info -> {:ok, info}
    end
  end

  defp get_loader_module(name) do
    module = Module.concat([DatasetsEx.Loaders, Macro.camelize(to_string(name))])

    if Code.ensure_loaded?(module) do
      {:ok, module}
    else
      # Fall back to generic JSONL loader
      {:ok, DatasetsEx.Loaders.Jsonl}
    end
  end

  defp detect_format(path) do
    case Path.extname(path) do
      ".jsonl" -> :jsonl
      ".json" -> :json
      ".csv" -> :csv
      _ -> :unknown
    end
  end

  defp ensure_list(data) when is_list(data), do: data
  defp ensure_list(data), do: [data]

  defp maybe_limit(stream, opts) do
    stream
    |> maybe_offset(opts)
    |> maybe_take(opts)
  end

  defp maybe_offset(stream, opts) do
    case Keyword.get(opts, :offset) do
      nil -> stream
      offset -> Stream.drop(stream, offset)
    end
  end

  defp maybe_take(stream, opts) do
    case Keyword.get(opts, :limit) do
      nil -> stream
      limit -> Stream.take(stream, limit)
    end
  end

  defp maybe_limit_list(list, opts) do
    limit = Keyword.get(opts, :limit)
    offset = Keyword.get(opts, :offset, 0)

    list
    |> Enum.drop(offset)
    |> then(fn l -> if limit, do: Enum.take(l, limit), else: l end)
  end
end
