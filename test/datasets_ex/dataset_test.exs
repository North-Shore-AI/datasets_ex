defmodule DatasetsEx.DatasetTest do
  use ExUnit.Case, async: true
  alias DatasetsEx.Dataset

  describe "new/2" do
    test "creates a new dataset with data" do
      data = [%{text: "hello"}, %{text: "world"}]
      dataset = Dataset.new("test", data: data)

      assert dataset.name == "test"
      assert dataset.data == data
      assert dataset.splits == %{}
    end

    test "creates a new dataset with splits" do
      splits = %{train: [%{x: 1}], test: [%{x: 2}]}
      dataset = Dataset.new("test", splits: splits)

      assert dataset.name == "test"
      assert dataset.splits == splits
    end

    test "creates a new dataset with metadata" do
      metadata = %{source: "test", version: "1.0"}
      dataset = Dataset.new("test", metadata: metadata)

      assert dataset.metadata == metadata
    end
  end

  describe "size/1" do
    test "returns size of dataset with data" do
      dataset = Dataset.new("test", data: [%{x: 1}, %{x: 2}, %{x: 3}])
      assert Dataset.size(dataset) == 3
    end

    test "returns size of dataset with splits" do
      dataset =
        Dataset.new("test",
          splits: %{
            train: [%{x: 1}, %{x: 2}],
            test: [%{x: 3}]
          }
        )

      assert Dataset.size(dataset) == 3
    end

    test "returns 0 for empty dataset" do
      dataset = Dataset.new("test")
      assert Dataset.size(dataset) == 0
    end
  end

  describe "get_split/2" do
    test "gets a specific split" do
      splits = %{train: [%{x: 1}], test: [%{x: 2}]}
      dataset = Dataset.new("test", splits: splits)

      assert Dataset.get_split(dataset, :train) == [%{x: 1}]
      assert Dataset.get_split(dataset, :test) == [%{x: 2}]
    end

    test "returns nil for non-existent split" do
      dataset = Dataset.new("test", splits: %{train: [%{x: 1}]})
      assert Dataset.get_split(dataset, :validation) == nil
    end
  end

  describe "list_splits/1" do
    test "lists available splits" do
      dataset = Dataset.new("test", splits: %{train: [], test: [], validation: []})
      splits = Dataset.list_splits(dataset)

      assert :train in splits
      assert :test in splits
      assert :validation in splits
    end

    test "returns empty list for dataset without splits" do
      dataset = Dataset.new("test", data: [%{x: 1}])
      assert Dataset.list_splits(dataset) == []
    end
  end

  describe "put_split/3" do
    test "adds a new split" do
      dataset = Dataset.new("test")
      updated = Dataset.put_split(dataset, :train, [%{x: 1}])

      assert Dataset.get_split(updated, :train) == [%{x: 1}]
    end

    test "updates an existing split" do
      dataset = Dataset.new("test", splits: %{train: [%{x: 1}]})
      updated = Dataset.put_split(dataset, :train, [%{x: 2}])

      assert Dataset.get_split(updated, :train) == [%{x: 2}]
    end
  end

  describe "compute_hash/1" do
    test "computes hash for dataset with data" do
      dataset = Dataset.new("test", data: [%{x: 1}, %{x: 2}])
      hash = Dataset.compute_hash(dataset)

      assert is_binary(hash)
      # SHA-256 hex length
      assert String.length(hash) == 64
    end

    test "same data produces same hash" do
      data = [%{x: 1}, %{x: 2}]
      dataset1 = Dataset.new("test1", data: data)
      dataset2 = Dataset.new("test2", data: data)

      assert Dataset.compute_hash(dataset1) == Dataset.compute_hash(dataset2)
    end

    test "different data produces different hash" do
      dataset1 = Dataset.new("test", data: [%{x: 1}])
      dataset2 = Dataset.new("test", data: [%{x: 2}])

      assert Dataset.compute_hash(dataset1) != Dataset.compute_hash(dataset2)
    end
  end

  describe "with_hash/1" do
    test "adds hash to dataset" do
      dataset = Dataset.new("test", data: [%{x: 1}])
      hashed = Dataset.with_hash(dataset)

      assert hashed.hash != nil
      assert is_binary(hashed.hash)
    end
  end
end
