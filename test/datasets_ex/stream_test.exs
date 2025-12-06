defmodule DatasetsEx.StreamTest do
  use ExUnit.Case, async: true

  alias DatasetsEx.{Dataset, Stream}

  setup do
    dataset =
      Dataset.new("test_dataset",
        data: [
          %{text: "item1", value: 1},
          %{text: "item2", value: 2},
          %{text: "item3", value: 3},
          %{text: "item4", value: 4},
          %{text: "item5", value: 5}
        ]
      )

    {:ok, dataset: dataset}
  end

  describe "lazy/1" do
    test "creates lazy stream from dataset", %{dataset: dataset} do
      stream = Stream.lazy(dataset)

      items = Enum.take(stream, 3)
      assert length(items) == 3
    end

    test "streams all items", %{dataset: dataset} do
      items = dataset |> Stream.lazy() |> Enum.to_list()
      assert length(items) == 5
    end
  end

  describe "batch/2" do
    test "creates batches of specified size", %{dataset: dataset} do
      batches = dataset |> Stream.batch(batch_size: 2) |> Enum.to_list()
      assert length(batches) == 3
      assert length(hd(batches)) == 2
      assert length(List.last(batches)) == 1
    end

    test "drops remainder when specified", %{dataset: dataset} do
      batches = dataset |> Stream.batch(batch_size: 2, drop_remainder: true) |> Enum.to_list()
      assert length(batches) == 2
      assert Enum.all?(batches, fn batch -> length(batch) == 2 end)
    end
  end

  describe "map_stream/2" do
    test "transforms items in stream", %{dataset: dataset} do
      transformed =
        dataset
        |> Stream.lazy()
        |> Stream.map_stream(fn item -> %{item | value: item.value * 2} end)
        |> Enum.to_list()

      assert hd(transformed).value == 2
      assert Enum.at(transformed, 1).value == 4
    end
  end

  describe "filter_stream/2" do
    test "filters items in stream", %{dataset: dataset} do
      filtered =
        dataset
        |> Stream.lazy()
        |> Stream.filter_stream(fn item -> item.value > 2 end)
        |> Enum.to_list()

      assert length(filtered) == 3
      assert Enum.all?(filtered, fn item -> item.value > 2 end)
    end
  end

  describe "parallel_map/3" do
    test "processes items in parallel", %{dataset: dataset} do
      results =
        dataset
        |> Stream.lazy()
        |> Stream.parallel_map(
          fn item -> %{item | value: item.value * 2} end,
          max_concurrency: 2
        )
        |> Enum.to_list()

      assert length(results) == 5
      assert Enum.all?(results, fn item -> rem(item.value, 2) == 0 end)
    end
  end

  describe "from_file/2" do
    test "streams JSONL file" do
      # Create temporary JSONL file
      path = "/tmp/test_stream.jsonl"

      File.write!(
        path,
        """
        {"text": "line1"}
        {"text": "line2"}
        {"text": "line3"}
        """
      )

      items = Stream.from_file(path, format: :jsonl) |> Enum.to_list()
      assert length(items) == 3
      assert hd(items)["text"] == "line1"

      File.rm!(path)
    end
  end
end
