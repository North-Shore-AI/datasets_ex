defmodule DatasetsEx.IntegrationTest do
  use ExUnit.Case, async: false
  alias DatasetsEx.{Dataset, Export}

  @test_dir "/tmp/datasets_ex_integration_test"

  setup do
    File.mkdir_p!(@test_dir)
    on_exit(fn -> File.rm_rf!(@test_dir) end)
    :ok
  end

  describe "full workflow" do
    test "create, split, version, and export dataset" do
      # Create dataset
      {:ok, dataset} =
        DatasetsEx.create("test_workflow", %{
          data:
            Enum.map(1..100, fn i ->
              %{id: i, text: "item #{i}", label: if(rem(i, 2) == 0, do: :even, else: :odd)}
            end),
          schema: :test_schema,
          metadata: %{source: "integration_test"}
        })

      assert Dataset.size(dataset) == 100

      # Split dataset
      {train, test} = DatasetsEx.split(dataset, ratio: 0.8, seed: 42)
      assert Dataset.size(train) == 80
      assert Dataset.size(test) == 20

      # Version dataset
      {:ok, versioned} = DatasetsEx.version(dataset, "v1.0.0")
      assert versioned.version == "v1.0.0"
      assert versioned.hash != nil

      # Export to JSONL
      jsonl_path = Path.join(@test_dir, "export.jsonl")
      {:ok, _path} = Export.export(train, format: :jsonl, path: jsonl_path)
      assert File.exists?(jsonl_path)

      # Verify exported data
      exported_count =
        jsonl_path
        |> File.stream!()
        |> Enum.count()

      assert exported_count == 80

      # Export to JSON
      json_path = Path.join(@test_dir, "export.json")
      {:ok, _path} = Export.export(test, format: :json, path: json_path, pretty: true)
      assert File.exists?(json_path)

      # Export to CSV
      csv_path = Path.join(@test_dir, "export.csv")
      {:ok, _path} = Export.export(dataset, format: :csv, path: csv_path, limit: 10)
      assert File.exists?(csv_path)

      # Verify CSV has header + 10 rows
      csv_lines =
        csv_path
        |> File.stream!()
        |> Enum.count()

      # header + 10 data rows
      assert csv_lines == 11
    end

    test "three-way split workflow" do
      {:ok, dataset} =
        DatasetsEx.create("three_split_test", %{
          data: Enum.map(1..100, &%{id: &1, value: &1 * 2})
        })

      {train, val, test} = DatasetsEx.split_three(dataset, seed: 42)

      assert Dataset.size(train) == 70
      assert Dataset.size(val) == 15
      assert Dataset.size(test) == 15

      # Export each split
      Enum.each([train, val, test], fn split ->
        name = if split == train, do: "train", else: if(split == val, do: "val", else: "test")
        path = Path.join(@test_dir, "#{name}.jsonl")
        {:ok, _} = Export.export(split, format: :jsonl, path: path)
        assert File.exists?(path)
      end)
    end

    test "k-fold cross-validation workflow" do
      {:ok, dataset} =
        DatasetsEx.create("kfold_test", %{
          data: Enum.map(1..50, &%{id: &1})
        })

      folds = DatasetsEx.k_fold(dataset, k: 5, seed: 42)

      assert length(folds) == 5

      # Each fold should work
      Enum.with_index(folds, fn {train, test}, idx ->
        train_path = Path.join(@test_dir, "fold_#{idx}_train.jsonl")
        test_path = Path.join(@test_dir, "fold_#{idx}_test.jsonl")

        {:ok, _} = Export.export(train, format: :jsonl, path: train_path)
        {:ok, _} = Export.export(test, format: :jsonl, path: test_path)

        assert File.exists?(train_path)
        assert File.exists?(test_path)
      end)
    end

    test "stratified split workflow" do
      # Create imbalanced dataset
      data =
        Enum.map(1..70, &%{id: &1, label: :a}) ++
          Enum.map(71..100, &%{id: &1, label: :b})

      {:ok, dataset} = DatasetsEx.create("stratified_test", %{data: data})

      {train, _test} =
        DatasetsEx.stratified_split(dataset,
          label_key: :label,
          ratio: 0.8,
          seed: 42
        )

      # Count labels
      train_a = Enum.count(train.data, &(&1.label == :a))
      train_b = Enum.count(train.data, &(&1.label == :b))

      # Should maintain roughly 70:30 ratio
      ratio = train_a / (train_a + train_b)
      assert_in_delta ratio, 0.7, 0.1
    end

    test "load and export custom file" do
      # Create test JSONL file
      jsonl_path = Path.join(@test_dir, "input.jsonl")

      content =
        1..10
        |> Enum.map(&~s({"id": #{&1}, "value": "test#{&1}"}))
        |> Enum.join("\n")

      File.write!(jsonl_path, content)

      # Load it
      {:ok, dataset} = DatasetsEx.load_file(jsonl_path, schema: :custom)

      assert Dataset.size(dataset) == 10
      assert dataset.schema == :custom

      # Export it back
      output_path = Path.join(@test_dir, "output.jsonl")
      {:ok, _} = Export.export(dataset, format: :jsonl, path: output_path)

      # Verify content matches
      input_lines = File.read!(jsonl_path) |> String.split("\n", trim: true)
      output_lines = File.read!(output_path) |> String.split("\n", trim: true)

      assert length(input_lines) == length(output_lines)
    end
  end

  describe "dataset registry" do
    test "lists available datasets" do
      datasets = DatasetsEx.list()
      assert :scifact in datasets
      assert :fever in datasets
    end

    test "gets dataset info" do
      info = DatasetsEx.info(:scifact)
      assert info != nil
      assert length(info.splits) > 0
      assert info.size > 0
    end
  end
end
