defmodule DatasetsEx.Export do
  @moduledoc """
  Dataset export to various formats.
  """

  alias DatasetsEx.Dataset

  @doc """
  Exports a dataset to the specified format.

  ## Supported Formats

    * `:jsonl` - JSON Lines format
    * `:json` - JSON format
    * `:csv` - CSV format

  ## Options

    * `:path` - Output file path (required)
    * `:split` - Export a specific split (optional)
    * `:pretty` - Pretty print JSON (default: false)

  ## Examples

      DatasetsEx.Export.export(dataset, format: :jsonl, path: "data.jsonl")
      DatasetsEx.Export.export(dataset, format: :csv, path: "data.csv", split: :train)
  """
  def export(%Dataset{} = dataset, opts) do
    format = Keyword.fetch!(opts, :format)
    path = Keyword.fetch!(opts, :path)

    data = get_export_data(dataset, opts)

    case format do
      :jsonl -> export_jsonl(data, path, opts)
      :json -> export_json(data, path, opts)
      :csv -> export_csv(data, path, opts)
      _ -> {:error, :unsupported_format}
    end
  end

  @doc """
  Exports dataset to JSONL format.
  """
  def export_jsonl(data, path, _opts \\ []) do
    File.mkdir_p!(Path.dirname(path))

    file = File.open!(path, [:write, :utf8])

    try do
      Enum.each(data, fn item ->
        IO.write(file, Jason.encode!(item))
        IO.write(file, "\n")
      end)

      {:ok, path}
    after
      File.close(file)
    end
  rescue
    error -> {:error, error}
  end

  @doc """
  Exports dataset to JSON format.
  """
  def export_json(data, path, opts \\ []) do
    File.mkdir_p!(Path.dirname(path))
    pretty = Keyword.get(opts, :pretty, false)

    json =
      if pretty do
        Jason.encode!(data, pretty: true)
      else
        Jason.encode!(data)
      end

    case File.write(path, json) do
      :ok -> {:ok, path}
      {:error, reason} -> {:error, reason}
    end
  rescue
    error -> {:error, error}
  end

  @doc """
  Exports dataset to CSV format.
  """
  def export_csv(data, path, opts \\ []) do
    File.mkdir_p!(Path.dirname(path))

    if Enum.empty?(data) do
      {:error, :empty_dataset}
    else
      headers = get_headers(data, opts)
      rows = convert_to_rows(data, headers)

      file = File.open!(path, [:write, :utf8])

      try do
        # Write headers
        write_csv_row(file, headers)

        # Write data rows
        Enum.each(rows, fn row ->
          write_csv_row(file, row)
        end)

        {:ok, path}
      after
        File.close(file)
      end
    end
  rescue
    error -> {:error, error}
  end

  # Private Functions

  defp get_export_data(%Dataset{data: data}, opts) when is_list(data) do
    maybe_limit(data, opts)
  end

  defp get_export_data(%Dataset{splits: splits}, opts) when map_size(splits) > 0 do
    case Keyword.get(opts, :split) do
      nil ->
        # Export all splits combined
        splits
        |> Map.values()
        |> List.flatten()
        |> maybe_limit(opts)

      split_name ->
        # Export specific split
        splits
        |> Map.get(split_name, [])
        |> maybe_limit(opts)
    end
  end

  defp get_export_data(_dataset, _opts), do: []

  defp maybe_limit(data, opts) do
    case Keyword.get(opts, :limit) do
      nil -> data
      limit -> Enum.take(data, limit)
    end
  end

  defp get_headers(data, opts) do
    case Keyword.get(opts, :headers) do
      nil ->
        # Infer headers from first item
        data
        |> List.first()
        |> Map.keys()
        |> Enum.map(&to_string/1)

      headers when is_list(headers) ->
        headers
    end
  end

  defp convert_to_rows(data, headers) do
    Enum.map(data, fn item ->
      Enum.map(headers, fn header ->
        value = Map.get(item, header) || Map.get(item, String.to_atom(header))
        format_csv_value(value)
      end)
    end)
  end

  defp write_csv_row(file, row) do
    row
    |> Enum.map_join(",", &escape_csv_value/1)
    |> then(&IO.write(file, &1 <> "\n"))
  end

  defp escape_csv_value(value) do
    value_str = to_string(value)

    if String.contains?(value_str, [",", "\"", "\n"]) do
      "\"#{String.replace(value_str, "\"", "\"\"")}\""
    else
      value_str
    end
  end

  defp format_csv_value(value) when is_binary(value), do: value
  defp format_csv_value(value) when is_number(value), do: to_string(value)
  defp format_csv_value(value) when is_boolean(value), do: to_string(value)
  defp format_csv_value(value) when is_atom(value), do: to_string(value)
  defp format_csv_value(value) when is_list(value), do: Jason.encode!(value)
  defp format_csv_value(value) when is_map(value), do: Jason.encode!(value)
  defp format_csv_value(nil), do: ""
  defp format_csv_value(value), do: inspect(value)
end
