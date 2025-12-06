defmodule DatasetsEx.Registry do
  @moduledoc """
  Dataset registry for tracking available datasets and their metadata.
  """

  use GenServer
  alias DatasetsEx.Dataset

  @registry_file "registry.json"

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Lists all registered datasets.
  """
  def list do
    GenServer.call(__MODULE__, :list)
  end

  @doc """
  Gets information about a specific dataset.
  """
  def info(name) do
    GenServer.call(__MODULE__, {:info, name})
  end

  @doc """
  Registers a new dataset.
  """
  def register(name, metadata \\ %{}) do
    GenServer.call(__MODULE__, {:register, name, metadata})
  end

  @doc """
  Unregisters a dataset.
  """
  def unregister(name) do
    GenServer.call(__MODULE__, {:unregister, name})
  end

  @doc """
  Updates dataset metadata.
  """
  def update_metadata(name, metadata) do
    GenServer.call(__MODULE__, {:update_metadata, name, metadata})
  end

  # Server Callbacks

  @impl true
  def init(_opts) do
    registry = load_registry()
    {:ok, registry}
  end

  @impl true
  def handle_call(:list, _from, registry) do
    names = Map.keys(registry)
    {:reply, names, registry}
  end

  @impl true
  def handle_call({:info, name}, _from, registry) do
    info = Map.get(registry, name)
    {:reply, info, registry}
  end

  @impl true
  def handle_call({:register, name, metadata}, _from, registry) do
    entry = %{
      name: name,
      registered_at: DateTime.utc_now(),
      metadata: metadata
    }

    new_registry = Map.put(registry, name, entry)
    save_registry(new_registry)
    {:reply, :ok, new_registry}
  end

  @impl true
  def handle_call({:unregister, name}, _from, registry) do
    new_registry = Map.delete(registry, name)
    save_registry(new_registry)
    {:reply, :ok, new_registry}
  end

  @impl true
  def handle_call({:update_metadata, name, metadata}, _from, registry) do
    case Map.get(registry, name) do
      nil ->
        {:reply, {:error, :not_found}, registry}

      entry ->
        updated_entry = %{entry | metadata: Map.merge(entry.metadata, metadata)}
        new_registry = Map.put(registry, name, updated_entry)
        save_registry(new_registry)
        {:reply, :ok, new_registry}
    end
  end

  # Private Functions

  defp registry_path do
    Path.join([priv_dir(), "datasets", @registry_file])
  end

  defp priv_dir do
    :code.priv_dir(:datasets_ex) |> to_string()
  end

  defp load_registry do
    path = registry_path()

    if File.exists?(path) do
      path
      |> File.read!()
      |> Jason.decode!(keys: :atoms)
      |> Map.new(fn {k, v} ->
        {k, atomize_keys(v)}
      end)
    else
      # Return default datasets
      default_registry()
    end
  rescue
    _ -> default_registry()
  end

  defp save_registry(registry) do
    path = registry_path()
    File.mkdir_p!(Path.dirname(path))

    registry
    |> Jason.encode!(pretty: true)
    |> then(&File.write!(path, &1))
  end

  defp atomize_keys(map) when is_map(map) do
    Map.new(map, fn
      {k, v} when is_binary(k) -> {String.to_atom(k), atomize_keys(v)}
      {k, v} -> {k, atomize_keys(v)}
    end)
  end

  defp atomize_keys(list) when is_list(list), do: Enum.map(list, &atomize_keys/1)
  defp atomize_keys(value), do: value

  defp default_registry do
    %{
      scifact: %{
        name: :scifact,
        description: "Scientific claim verification dataset",
        size: 5183,
        splits: [:train, :test],
        schema: :claim_evidence,
        source: "https://scifact.s3.us-west-2.amazonaws.com/release/latest/data.tar.gz",
        metadata: %{
          format: :jsonl,
          task: "claim_verification"
        }
      },
      fever: %{
        name: :fever,
        description: "Fact Extraction and VERification dataset",
        size: 185_445,
        splits: [:train, :dev, :test],
        schema: :claim_evidence,
        source: "https://fever.ai/download/fever/train.jsonl",
        metadata: %{
          format: :jsonl,
          task: "fact_verification"
        }
      }
    }
  end
end
