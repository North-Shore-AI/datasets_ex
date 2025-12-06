defmodule DatasetsEx.LoaderTest do
  use ExUnit.Case, async: true
  alias DatasetsEx.Loader

  @test_dir "/tmp/datasets_ex_test"

  setup do
    File.mkdir_p!(@test_dir)
    on_exit(fn -> File.rm_rf!(@test_dir) end)
    :ok
  end

  describe "load_jsonl/2" do
    test "loads JSONL file" do
      path = Path.join(@test_dir, "test.jsonl")

      content = """
      {"id": 1, "text": "hello"}
      {"id": 2, "text": "world"}
      """

      File.write!(path, content)

      {:ok, dataset} = Loader.load_jsonl(path)

      assert dataset.name == "test"
      assert length(dataset.data) == 2
      assert Enum.at(dataset.data, 0)["id"] == 1
      assert Enum.at(dataset.data, 1)["text"] == "world"
    end

    test "loads JSONL with limit" do
      path = Path.join(@test_dir, "test.jsonl")

      content =
        1..100
        |> Enum.map(&~s({"id": #{&1}}))
        |> Enum.join("\n")

      File.write!(path, content)

      {:ok, dataset} = Loader.load_jsonl(path, limit: 10)

      assert length(dataset.data) == 10
    end

    test "loads JSONL with offset" do
      path = Path.join(@test_dir, "test.jsonl")

      content =
        1..10
        |> Enum.map(&~s({"id": #{&1}}))
        |> Enum.join("\n")

      File.write!(path, content)

      {:ok, dataset} = Loader.load_jsonl(path, offset: 5, limit: 3)

      assert length(dataset.data) == 3
      assert Enum.at(dataset.data, 0)["id"] == 6
    end

    test "handles empty lines" do
      path = Path.join(@test_dir, "test.jsonl")

      content = """
      {"id": 1}

      {"id": 2}


      {"id": 3}
      """

      File.write!(path, content)

      {:ok, dataset} = Loader.load_jsonl(path)

      assert length(dataset.data) == 3
    end
  end

  describe "load_json/2" do
    test "loads JSON array" do
      path = Path.join(@test_dir, "test.json")

      content = """
      [
        {"id": 1, "text": "hello"},
        {"id": 2, "text": "world"}
      ]
      """

      File.write!(path, content)

      {:ok, dataset} = Loader.load_json(path)

      assert dataset.name == "test"
      assert length(dataset.data) == 2
    end

    test "loads single JSON object as list" do
      path = Path.join(@test_dir, "test.json")

      content = """
      {"id": 1, "text": "hello"}
      """

      File.write!(path, content)

      {:ok, dataset} = Loader.load_json(path)

      assert length(dataset.data) == 1
      assert Enum.at(dataset.data, 0)["id"] == 1
    end
  end

  describe "load_csv/2" do
    test "loads CSV with headers" do
      path = Path.join(@test_dir, "test.csv")

      content = """
      id,text,label
      1,hello,greeting
      2,world,noun
      """

      File.write!(path, content)

      {:ok, dataset} = Loader.load_csv(path)

      assert length(dataset.data) == 2
      assert Enum.at(dataset.data, 0) == %{"id" => "1", "text" => "hello", "label" => "greeting"}
    end

    test "loads CSV with limit" do
      path = Path.join(@test_dir, "test.csv")

      rows = ["id,value"] ++ Enum.map(1..100, &"#{&1},value#{&1}")
      File.write!(path, Enum.join(rows, "\n"))

      {:ok, dataset} = Loader.load_csv(path, limit: 5)

      assert length(dataset.data) == 5
    end
  end
end
