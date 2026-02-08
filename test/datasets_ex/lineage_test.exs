defmodule DatasetsEx.LineageTest do
  use ExUnit.Case, async: true

  alias DatasetsEx.{Dataset, Lineage}

  describe "artifact_ref/2" do
    test "builds a dataset artifact ref with metadata" do
      dataset =
        Dataset.new("lineage_dataset",
          data: [%{text: "hello"}, %{text: "world"}],
          schema: :text_classification,
          version: "v1.0.0",
          metadata: %{source: "unit_test"}
        )

      ref = Lineage.artifact_ref(dataset)

      assert %LineageIR.ArtifactRef{} = ref
      assert ref.artifact_id == dataset.artifact_id
      assert ref.type == "dataset"
      assert ref.uri == "datasets_ex://lineage_dataset/versions/v1.0.0"
      assert ref.checksum == Dataset.compute_hash(dataset)
      assert ref.metadata.name == "lineage_dataset"
      assert ref.metadata.schema == :text_classification
      assert ref.metadata.size == 2
      assert ref.metadata.splits == []
      assert ref.metadata.dataset_metadata.source == "unit_test"
    end

    test "allows overrides for uri and metadata" do
      dataset = Dataset.new("override_dataset", data: [%{x: 1}])

      ref =
        Lineage.artifact_ref(dataset,
          uri: "datasets_ex://override_dataset/custom",
          metadata: %{tag: "gold"}
        )

      assert ref.uri == "datasets_ex://override_dataset/custom"
      assert ref.metadata.tag == "gold"
      assert ref.metadata.name == "override_dataset"
    end
  end

  describe "edge/3" do
    test "builds provenance edges between artifact refs" do
      source =
        Dataset.new("source_dataset", data: [%{x: 1}])
        |> Lineage.artifact_ref()

      target =
        Dataset.new("target_dataset", data: [%{x: 2}])
        |> Lineage.artifact_ref()

      edge =
        Lineage.edge(source, target,
          relationship: "derived_from",
          metadata: %{operation: "split"}
        )

      assert %LineageIR.ProvenanceEdge{} = edge
      assert edge.source_type == "artifact"
      assert edge.target_type == "artifact"
      assert edge.source_id == source.artifact_id
      assert edge.target_id == target.artifact_id
      assert edge.relationship == "derived_from"
      assert edge.metadata.operation == "split"
    end
  end
end
