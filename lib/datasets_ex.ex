defmodule DatasetsEx do
  @moduledoc """
  Dataset management library for ML experiments.

  DatasetsEx provides functionality for:
  - Loading standard datasets (SciFact, FEVER, etc.)
  - Creating custom datasets
  - Versioning and lineage tracking
  - Train/test splitting
  - Export to various formats
  - Emitting LineageIR artifact references and provenance edges

  ## Examples

      # Load a standard dataset
      {:ok, scifact} = DatasetsEx.load(:scifact)

      # Load with options
      {:ok, fever} = DatasetsEx.load(:fever, split: :train, limit: 1000)

      # Create a custom dataset
      {:ok, dataset} = DatasetsEx.create("my_dataset", %{
        data: [%{claim: "...", evidence: "..."}],
        schema: :claim_evidence
      })

      # Split dataset
      {train, test} = DatasetsEx.split(dataset, ratio: 0.8, seed: 42)

      # Version dataset
      {:ok, versioned} = DatasetsEx.version(dataset, "v1.0.0")

      # Export dataset
      DatasetsEx.export(dataset, format: :jsonl, path: "data.jsonl")
  """

  alias DatasetsEx.{
    Dataset,
    Export,
    Lineage,
    Loader,
    Registry,
    Splitter,
    Versioning
  }

  # Dataset Loading

  @doc """
  Loads a dataset by name.

  ## Options

    * `:split` - Load a specific split (e.g., :train, :test)
    * `:limit` - Limit the number of examples
    * `:offset` - Skip the first N examples
    * `:version` - Load a specific version
    * `:cache` - Use cached version if available (default: true)
  """
  defdelegate load(name, opts \\ []), to: Loader

  @doc """
  Loads a dataset from a local file.

  ## Options

    * `:format` - File format (:jsonl, :csv, :json)
    * `:schema` - Dataset schema
  """
  defdelegate load_file(path, opts \\ []), to: Loader

  # Dataset Registry

  @doc """
  Lists all registered datasets.
  """
  defdelegate list(), to: Registry

  @doc """
  Gets information about a specific dataset.
  """
  defdelegate info(name), to: Registry

  # Dataset Creation

  @doc """
  Creates a new dataset.

  ## Options

    * `:data` - Dataset items (list of maps)
    * `:splits` - Pre-defined splits (map of split_name => data)
    * `:schema` - Dataset schema
    * `:metadata` - Additional metadata

  ## Examples

      DatasetsEx.create("my_dataset", %{
        data: [%{text: "hello", label: "greeting"}],
        schema: :text_classification,
        metadata: %{source: "manual"}
      })
  """
  def create(name, opts) when is_map(opts) do
    dataset =
      Dataset.new(name,
        data: Map.get(opts, :data),
        splits: Map.get(opts, :splits, %{}),
        schema: Map.get(opts, :schema),
        metadata:
          Map.get(opts, :metadata, %{
            created_at: DateTime.utc_now(),
            source: "custom"
          })
      )

    # Register the dataset
    Registry.register(name, %{
      schema: dataset.schema,
      size: Dataset.size(dataset),
      splits: Dataset.list_splits(dataset),
      metadata: dataset.metadata
    })

    {:ok, dataset}
  end

  # Dataset Splitting

  @doc """
  Splits a dataset into train and test sets.

  ## Options

    * `:ratio` - Train/test ratio (default: 0.8)
    * `:seed` - Random seed for reproducibility
    * `:shuffle` - Whether to shuffle before splitting (default: true)
  """
  defdelegate split(dataset, opts \\ []), to: Splitter

  @doc """
  Splits a dataset into train, validation, and test sets.

  ## Options

    * `:ratios` - List of ratios [train, val, test] (default: [0.7, 0.15, 0.15])
    * `:seed` - Random seed for reproducibility
  """
  defdelegate split_three(dataset, opts \\ []), to: Splitter

  @doc """
  Creates k-fold cross-validation splits.
  """
  defdelegate k_fold(dataset, opts \\ []), to: Splitter

  @doc """
  Stratified split that maintains class distribution.
  """
  defdelegate stratified_split(dataset, opts \\ []), to: Splitter

  # Dataset Versioning

  @doc """
  Creates a new version of a dataset.
  """
  def version(%Dataset{} = dataset, version) do
    Versioning.create(dataset, version)
  end

  @doc """
  Loads a specific version of a dataset.
  """
  def load_version(name, version) do
    Versioning.load(name, version)
  end

  @doc """
  Gets the lineage (version history) of a dataset.
  """
  defdelegate lineage(name), to: Versioning

  @doc """
  Lists all versions of a dataset.
  """
  defdelegate list_versions(name), to: Versioning

  # Lineage

  @doc """
  Builds a LineageIR.ArtifactRef for a dataset.
  """
  defdelegate artifact_ref(dataset, opts \\ []), to: Lineage

  @doc """
  Builds a LineageIR.ProvenanceEdge between datasets or artifact refs.
  """
  defdelegate lineage_edge(source, target, opts \\ []), to: Lineage, as: :edge

  # Dataset Export

  @doc """
  Exports a dataset to the specified format.

  ## Options

    * `:format` - Export format (:jsonl, :json, :csv)
    * `:path` - Output file path
    * `:split` - Export a specific split (optional)
    * `:pretty` - Pretty print JSON (default: false)
  """
  defdelegate export(dataset, opts), to: Export

  # Utility Functions

  @doc """
  Returns dataset size (total number of examples).
  """
  defdelegate size(dataset), to: Dataset

  @doc """
  Gets a specific split from a dataset.
  """
  defdelegate get_split(dataset, split), to: Dataset

  @doc """
  Lists available splits in a dataset.
  """
  defdelegate list_splits(dataset), to: Dataset
end
