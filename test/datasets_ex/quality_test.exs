defmodule DatasetsEx.QualityTest do
  use ExUnit.Case, async: true

  alias DatasetsEx.{Dataset, Quality}

  describe "validate_schema/2" do
    test "validates required keys" do
      dataset =
        Dataset.new("test",
          data: [
            %{text: "hello", label: :positive},
            %{text: "world", label: :negative}
          ]
        )

      {:ok, result} = Quality.validate_schema(dataset, required_keys: [:text, :label])
      assert result.valid
      assert result.total_items == 2
    end

    test "detects missing required keys" do
      dataset =
        Dataset.new("test",
          data: [
            %{text: "hello"},
            %{label: :negative}
          ]
        )

      {:error, errors} = Quality.validate_schema(dataset, required_keys: [:text, :label])
      assert length(errors) == 2
    end

    test "validates types" do
      dataset =
        Dataset.new("test",
          data: [
            %{text: "hello", score: 5},
            %{text: "world", score: "invalid"}
          ]
        )

      {:error, errors} =
        Quality.validate_schema(dataset,
          type_checks: %{text: &is_binary/1, score: &is_integer/1}
        )

      assert length(errors) == 1
      assert Enum.any?(errors, &String.contains?(&1, "score"))
    end
  end

  describe "detect_duplicates/2" do
    test "detects exact duplicates" do
      dataset =
        Dataset.new("test",
          data: [
            %{text: "hello"},
            %{text: "world"},
            %{text: "hello"},
            %{text: "hello"}
          ]
        )

      result = Quality.detect_duplicates(dataset, key: :text)
      assert result.total_items == 4
      assert result.duplicate_groups == 1
      assert result.duplicate_items == 3
      assert result.duplicate_rate == 0.75
    end

    test "detects case-insensitive duplicates" do
      dataset =
        Dataset.new("test",
          data: [
            %{text: "hello"},
            %{text: "HELLO"},
            %{text: "Hello"}
          ]
        )

      result = Quality.detect_duplicates(dataset, key: :text, ignore_case: true)
      assert result.duplicate_groups == 1
      assert result.duplicate_items == 3
    end
  end

  describe "label_distribution/2" do
    test "analyzes label distribution" do
      dataset =
        Dataset.new("test",
          data: [
            %{label: :positive},
            %{label: :positive},
            %{label: :positive},
            %{label: :negative}
          ]
        )

      result = Quality.label_distribution(dataset)
      assert result.total_items == 4
      assert result.num_classes == 2
      assert result.distribution[:positive].count == 3
      assert result.distribution[:negative].count == 1
      assert_in_delta result.distribution[:positive].percentage, 75.0, 0.1
    end

    test "detects imbalanced distribution" do
      dataset =
        Dataset.new("test",
          data: [
            %{label: :a},
            %{label: :a},
            %{label: :a},
            %{label: :a},
            %{label: :b}
          ]
        )

      result = Quality.label_distribution(dataset)
      refute result.is_balanced
    end

    test "detects balanced distribution" do
      dataset =
        Dataset.new("test",
          data: [
            %{label: :a},
            %{label: :a},
            %{label: :b},
            %{label: :b}
          ]
        )

      result = Quality.label_distribution(dataset)
      assert result.is_balanced
    end
  end

  describe "profile/2" do
    test "profiles dataset characteristics" do
      dataset =
        Dataset.new("test",
          data: [
            %{text: "hello world", score: 5},
            %{text: "foo bar baz", score: 10}
          ]
        )

      result = Quality.profile(dataset)
      assert result.total_items == 2
      assert result.text_stats.min_length == 11
      assert result.text_stats.max_length == 11
      assert result.vocabulary.total_tokens == 5
      assert result.vocabulary.unique_tokens == 5
    end

    test "detects missing values" do
      dataset =
        Dataset.new("test",
          data: [
            %{text: "hello", score: 5},
            %{text: "", score: nil},
            %{text: "world"}
          ]
        )

      result = Quality.profile(dataset)
      assert result.missing_values[:text].missing == 1
      assert result.missing_values[:score].missing == 2
    end
  end

  describe "detect_outliers/2" do
    test "detects outliers using IQR method" do
      dataset =
        Dataset.new("test",
          data: [
            %{score: 1},
            %{score: 2},
            %{score: 3},
            %{score: 4},
            %{score: 100}
          ]
        )

      result = Quality.detect_outliers(dataset, field: :score, method: :iqr)
      assert result.outlier_count > 0
    end

    test "detects outliers using z-score method" do
      dataset =
        Dataset.new("test",
          data: [
            %{score: 10},
            %{score: 11},
            %{score: 12},
            %{score: 13},
            %{score: 14},
            %{score: 15},
            %{score: 16},
            %{score: 1000}
          ]
        )

      result = Quality.detect_outliers(dataset, field: :score, method: :zscore, threshold: 2)
      assert result.outlier_count > 0
    end
  end
end
