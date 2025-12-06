defmodule DatasetsEx.Loaders.Csv do
  @moduledoc """
  Generic CSV loader for custom datasets.

  Provides flexible CSV loading with header detection,
  custom column mapping, and type inference.
  """

  alias DatasetsEx.{Dataset, Loader}

  @doc """
  Loads a CSV dataset.

  ## Options

    * `:headers` - Use first row as headers (default: true)
    * `:column_mapping` - Map column names to schema fields
    * `:delimiter` - CSV delimiter (default: ",")
    * `:schema` - Dataset schema
    * `:limit` - Limit the number of examples
    * `:offset` - Skip the first N examples

  ## Examples

      {:ok, dataset} = DatasetsEx.Loaders.Csv.load(
        %{name: "my_data", path: "data.csv"},
        headers: true,
        schema: :text_classification
      )
  """
  @spec load(map(), keyword()) :: {:ok, Dataset.t()} | {:error, term()}
  def load(info, opts \\ []) do
    path = Map.get(info, :path) || get_cache_path(info.name)

    if File.exists?(path) do
      Loader.load_csv(path, Keyword.merge([name: info.name], opts))
    else
      {:error, :dataset_not_found}
    end
  end

  defp get_cache_path(name) do
    Path.join([priv_dir(), "datasets", to_string(name), "data.csv"])
  end

  defp priv_dir do
    :code.priv_dir(:datasets_ex) |> to_string()
  end
end
