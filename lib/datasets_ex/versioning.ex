defmodule DatasetsEx.Versioning do
  @moduledoc """
  Dataset versioning and lineage tracking.
  """

  alias DatasetsEx.Dataset

  @versions_file "versions.json"

  @doc """
  Creates a new version of a dataset.

  ## Examples

      DatasetsEx.Versioning.create(dataset, "v1.0.0")
  """
  def create(%Dataset{} = dataset, version) do
    dataset = Dataset.ensure_artifact_id(dataset)
    dataset_with_hash = Dataset.with_hash(dataset)
    versioned_dataset = %{dataset_with_hash | version: version}

    # Save version metadata
    metadata = %{
      version: version,
      hash: versioned_dataset.hash,
      created_at: DateTime.utc_now(),
      size: Dataset.size(versioned_dataset),
      metadata: versioned_dataset.metadata
    }

    save_version_metadata(dataset.name, metadata)

    # Save dataset to versioned path
    save_versioned_dataset(versioned_dataset)

    {:ok, versioned_dataset}
  end

  @doc """
  Loads a specific version of a dataset.
  """
  def load(name, version) do
    path = versioned_dataset_path(name, version)

    if File.exists?(path) do
      dataset =
        path
        |> File.read!()
        |> :erlang.binary_to_term()
        |> Dataset.ensure_artifact_id()

      {:ok, dataset}
    else
      {:error, :version_not_found}
    end
  end

  @doc """
  Lists all versions of a dataset.
  """
  def list_versions(name) do
    case get_version_history(name) do
      [] -> []
      versions -> Enum.map(versions, & &1.version)
    end
  end

  @doc """
  Gets the lineage (version history) of a dataset.
  """
  def lineage(name) do
    get_version_history(name)
  end

  @doc """
  Gets information about a specific version.
  """
  def version_info(name, version) do
    name
    |> get_version_history()
    |> Enum.find(&(&1.version == version))
  end

  @doc """
  Compares two versions of a dataset.
  """
  def diff(name, version1, version2) do
    with {:ok, v1_info} <- {:ok, version_info(name, version1)},
         {:ok, v2_info} <- {:ok, version_info(name, version2)} do
      %{
        version1: version1,
        version2: version2,
        hash_changed: v1_info.hash != v2_info.hash,
        size_diff: v2_info.size - v1_info.size,
        time_diff: DateTime.diff(v2_info.created_at, v1_info.created_at),
        metadata_changes: map_diff(v1_info.metadata, v2_info.metadata)
      }
    else
      _ -> {:error, :version_not_found}
    end
  end

  @doc """
  Tags a version with a label.
  """
  def tag(name, version, tag_name) do
    case version_info(name, version) do
      nil ->
        {:error, :version_not_found}

      info ->
        updated_info = Map.put(info, :tags, [tag_name | Map.get(info, :tags, [])])
        update_version_metadata(name, version, updated_info)
        :ok
    end
  end

  @doc """
  Gets the latest version of a dataset.
  """
  def latest(name) do
    name
    |> get_version_history()
    |> List.first()
  end

  # Private Functions

  defp versions_path(name) do
    Path.join([priv_dir(), "datasets", to_string(name), @versions_file])
  end

  defp versioned_dataset_path(name, version) do
    Path.join([priv_dir(), "datasets", to_string(name), "versions", "#{version}.etf"])
  end

  defp priv_dir do
    :code.priv_dir(:datasets_ex) |> to_string()
  end

  defp get_version_history(name) do
    path = versions_path(name)

    if File.exists?(path) do
      path
      |> File.read!()
      |> Jason.decode!(keys: :atoms)
      |> Enum.map(&atomize_keys/1)
      |> Enum.sort_by(& &1.created_at, {:desc, DateTime})
    else
      []
    end
  rescue
    _ -> []
  end

  defp save_version_metadata(name, metadata) do
    path = versions_path(name)
    File.mkdir_p!(Path.dirname(path))

    versions =
      if File.exists?(path) do
        path
        |> File.read!()
        |> Jason.decode!()
      else
        []
      end

    updated_versions = [metadata | versions]

    updated_versions
    |> Jason.encode!(pretty: true)
    |> then(&File.write!(path, &1))
  end

  defp update_version_metadata(name, version, updated_info) do
    path = versions_path(name)

    versions =
      path
      |> File.read!()
      |> Jason.decode!()
      |> Enum.map(fn v ->
        if v["version"] == version, do: updated_info, else: v
      end)

    versions
    |> Jason.encode!(pretty: true)
    |> then(&File.write!(path, &1))
  end

  defp save_versioned_dataset(dataset) do
    path = versioned_dataset_path(dataset.name, dataset.version)
    File.mkdir_p!(Path.dirname(path))

    binary = :erlang.term_to_binary(dataset)
    File.write!(path, binary)
  end

  defp atomize_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) -> {String.to_atom(k), atomize_keys(v)}
      {k, v} -> {k, atomize_keys(v)}
    end)
  end

  defp atomize_keys(list) when is_list(list), do: Enum.map(list, &atomize_keys/1)
  defp atomize_keys(value), do: value

  defp map_diff(map1, map2) do
    all_keys = MapSet.union(MapSet.new(Map.keys(map1)), MapSet.new(Map.keys(map2)))

    Enum.reduce(all_keys, %{added: [], removed: [], changed: []}, fn key, acc ->
      cond do
        not Map.has_key?(map1, key) ->
          %{acc | added: [{key, Map.get(map2, key)} | acc.added]}

        not Map.has_key?(map2, key) ->
          %{acc | removed: [{key, Map.get(map1, key)} | acc.removed]}

        Map.get(map1, key) != Map.get(map2, key) ->
          %{acc | changed: [{key, Map.get(map1, key), Map.get(map2, key)} | acc.changed]}

        true ->
          acc
      end
    end)
  end
end
