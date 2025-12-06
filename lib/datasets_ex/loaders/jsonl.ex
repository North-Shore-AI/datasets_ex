defmodule DatasetsEx.Loaders.Jsonl do
  @moduledoc """
  Generic JSONL loader for custom datasets.
  """

  alias DatasetsEx.Loader

  def load(info, opts \\ []) do
    # For generic datasets, assume they're already downloaded
    cache_path = get_cache_path(info.name)

    if File.exists?(cache_path) do
      Loader.load_jsonl(cache_path, Keyword.merge([name: info.name], opts))
    else
      {:error, :dataset_not_found}
    end
  end

  defp get_cache_path(name) do
    Path.join([priv_dir(), "datasets", to_string(name), "data.jsonl"])
  end

  defp priv_dir do
    :code.priv_dir(:datasets_ex) |> to_string()
  end
end
