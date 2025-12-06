defmodule DatasetsEx.TransformTest do
  use ExUnit.Case, async: true

  alias DatasetsEx.{Dataset, Transform}

  setup do
    dataset =
      Dataset.new("test_dataset",
        data: [
          %{text: "Hello World", label: :positive},
          %{text: "GOODBYE WORLD", label: :negative},
          %{text: "Hello Again", label: :positive},
          %{text: "Hello Again", label: :positive},
          %{text: "Test   Message", label: :neutral}
        ]
      )

    {:ok, dataset: dataset}
  end

  describe "map/2" do
    test "transforms all items", %{dataset: dataset} do
      transformed =
        Transform.map(dataset, fn item ->
          %{item | text: String.upcase(item.text)}
        end)

      assert Enum.all?(transformed.data, fn item ->
               item.text == String.upcase(item.text)
             end)
    end
  end

  describe "filter/2" do
    test "filters items based on predicate", %{dataset: dataset} do
      filtered = Transform.filter(dataset, fn item -> item.label == :positive end)
      assert length(filtered.data) == 3
      assert Enum.all?(filtered.data, fn item -> item.label == :positive end)
    end
  end

  describe "normalize_text/2" do
    test "normalizes text with default options", %{dataset: dataset} do
      normalized = Transform.normalize_text(dataset)

      texts = Enum.map(normalized.data, & &1.text)
      assert "hello world" in texts
      assert "goodbye world" in texts
      assert "test message" in texts
    end

    test "normalizes with custom text key" do
      dataset =
        Dataset.new("test",
          data: [
            %{content: "HELLO WORLD", label: :test}
          ]
        )

      normalized = Transform.normalize_text(dataset, text_key: :content)
      assert hd(normalized.data).content == "hello world"
    end
  end

  describe "deduplicate/2" do
    test "removes duplicate items", %{dataset: dataset} do
      deduped = Transform.deduplicate(dataset, :text)
      assert length(deduped.data) == 4
      assert length(dataset.data) == 5
    end
  end

  describe "sample/3" do
    test "samples random subset", %{dataset: dataset} do
      sampled = Transform.sample(dataset, 3, seed: 42)
      assert length(sampled.data) == 3
    end

    test "deterministic with seed", %{dataset: dataset} do
      sample1 = Transform.sample(dataset, 3, seed: 42)
      sample2 = Transform.sample(dataset, 3, seed: 42)
      assert sample1.data == sample2.data
    end
  end

  describe "balance_classes/2" do
    test "balances classes with undersampling" do
      dataset =
        Dataset.new("imbalanced",
          data: [
            %{text: "a", label: :positive},
            %{text: "b", label: :positive},
            %{text: "c", label: :positive},
            %{text: "d", label: :positive},
            %{text: "e", label: :negative},
            %{text: "f", label: :negative}
          ]
        )

      balanced = Transform.balance_classes(dataset, strategy: :undersample, seed: 42)
      assert length(balanced.data) == 4

      grouped = Enum.group_by(balanced.data, & &1.label)
      assert length(grouped[:positive]) == 2
      assert length(grouped[:negative]) == 2
    end

    test "balances classes with oversampling" do
      dataset =
        Dataset.new("imbalanced",
          data: [
            %{text: "a", label: :positive},
            %{text: "b", label: :positive},
            %{text: "c", label: :negative}
          ]
        )

      balanced = Transform.balance_classes(dataset, strategy: :oversample, seed: 42)
      assert length(balanced.data) == 4

      grouped = Enum.group_by(balanced.data, & &1.label)
      assert length(grouped[:positive]) == 2
      assert length(grouped[:negative]) == 2
    end
  end
end
