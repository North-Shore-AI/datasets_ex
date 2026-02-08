defmodule DatasetsEx.Lineage do
  @moduledoc """
  Lineage helpers for dataset artifact references and provenance edges.
  """

  alias DatasetsEx.Dataset
  alias LineageIR.{ArtifactRef, ProvenanceEdge}

  @dataset_type "dataset"
  @default_relationship "derived_from"

  @doc """
  Builds a LineageIR.ArtifactRef for a dataset.
  """
  @spec artifact_ref(Dataset.t(), keyword()) :: ArtifactRef.t()
  def artifact_ref(%Dataset{} = dataset, opts \\ []) do
    dataset = Dataset.ensure_artifact_id(dataset)

    checksum =
      Keyword.get(opts, :checksum, dataset.hash || Dataset.compute_hash(dataset))

    %ArtifactRef{
      artifact_id: dataset.artifact_id,
      type: Keyword.get(opts, :type, @dataset_type),
      uri: Keyword.get(opts, :uri, dataset_uri(dataset)),
      checksum: checksum,
      metadata: build_metadata(dataset, Keyword.get(opts, :metadata, %{}))
    }
  end

  @doc """
  Builds a provenance edge between two artifact refs or datasets.
  """
  @spec edge(ArtifactRef.t() | Dataset.t(), ArtifactRef.t() | Dataset.t(), keyword()) ::
          ProvenanceEdge.t()
  def edge(source, target, opts \\ [])

  def edge(%Dataset{} = source, %Dataset{} = target, opts) do
    edge(artifact_ref(source), artifact_ref(target), opts)
  end

  def edge(%ArtifactRef{} = source, %ArtifactRef{} = target, opts) do
    %ProvenanceEdge{
      id: Keyword.get(opts, :id, Ecto.UUID.generate()),
      trace_id: Keyword.get(opts, :trace_id),
      source_type: Keyword.get(opts, :source_type, "artifact"),
      source_id: source.artifact_id,
      target_type: Keyword.get(opts, :target_type, "artifact"),
      target_id: target.artifact_id,
      relationship: Keyword.get(opts, :relationship, @default_relationship),
      metadata: Keyword.get(opts, :metadata, %{})
    }
  end

  defp dataset_uri(%Dataset{name: name, version: version}) do
    base = "datasets_ex://#{to_string(name)}"

    if is_binary(version) do
      base <> "/versions/" <> version
    else
      base
    end
  end

  defp build_metadata(dataset, overrides) when is_map(overrides) do
    base = %{
      name: to_string(dataset.name),
      version: dataset.version,
      schema: dataset.schema,
      size: Dataset.size(dataset),
      splits: Dataset.list_splits(dataset),
      dataset_metadata: dataset.metadata
    }

    Map.merge(base, overrides)
  end

  defp build_metadata(dataset, _overrides) do
    build_metadata(dataset, %{})
  end
end
