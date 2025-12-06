defmodule DatasetsEx.SplitterTest do
  use ExUnit.Case, async: true
  alias DatasetsEx.{Dataset, Splitter}

  describe "split/2" do
    test "splits dataset into train and test with default ratio" do
      data = Enum.map(1..100, &%{x: &1})
      dataset = Dataset.new("test", data: data)

      {train, test} = Splitter.split(dataset, shuffle: false)

      assert Dataset.size(train) == 80
      assert Dataset.size(test) == 20
    end

    test "splits dataset with custom ratio" do
      data = Enum.map(1..100, &%{x: &1})
      dataset = Dataset.new("test", data: data)

      {train, test} = Splitter.split(dataset, ratio: 0.7, shuffle: false)

      assert Dataset.size(train) == 70
      assert Dataset.size(test) == 30
    end

    test "splits dataset with shuffling" do
      data = Enum.map(1..100, &%{x: &1})
      dataset = Dataset.new("test", data: data)

      {train, test} = Splitter.split(dataset, seed: 42, shuffle: true)

      # Data should be shuffled
      first_train_x = train.data |> List.first() |> Map.get(:x)
      # Unlikely to be first element after shuffle
      assert first_train_x != 1
    end

    test "splits are reproducible with same seed" do
      data = Enum.map(1..100, &%{x: &1})
      dataset = Dataset.new("test", data: data)

      {train1, test1} = Splitter.split(dataset, seed: 42)
      {train2, test2} = Splitter.split(dataset, seed: 42)

      assert train1.data == train2.data
      assert test1.data == test2.data
    end
  end

  describe "split_three/2" do
    test "splits dataset into train, val, and test" do
      data = Enum.map(1..100, &%{x: &1})
      dataset = Dataset.new("test", data: data)

      {train, val, test} = Splitter.split_three(dataset, shuffle: false)

      assert Dataset.size(train) == 70
      assert Dataset.size(val) == 15
      assert Dataset.size(test) == 15
    end

    test "splits with custom ratios" do
      data = Enum.map(1..100, &%{x: &1})
      dataset = Dataset.new("test", data: data)

      {train, val, test} =
        Splitter.split_three(dataset,
          ratios: [0.6, 0.2, 0.2],
          shuffle: false
        )

      assert Dataset.size(train) == 60
      assert Dataset.size(val) == 20
      assert Dataset.size(test) == 20
    end

    test "raises error if ratios don't sum to 1.0" do
      data = Enum.map(1..100, &%{x: &1})
      dataset = Dataset.new("test", data: data)

      assert_raise ArgumentError, fn ->
        Splitter.split_three(dataset, ratios: [0.5, 0.3, 0.1])
      end
    end
  end

  describe "k_fold/2" do
    test "creates k-fold splits" do
      data = Enum.map(1..100, &%{x: &1})
      dataset = Dataset.new("test", data: data)

      folds = Splitter.k_fold(dataset, k: 5, shuffle: false)

      assert length(folds) == 5

      # Each fold should have roughly equal test size
      test_sizes = Enum.map(folds, fn {_train, test} -> Dataset.size(test) end)
      assert Enum.all?(test_sizes, &(&1 in 19..20))
    end

    test "each example appears in test set exactly once" do
      data = Enum.map(1..50, &%{x: &1})
      dataset = Dataset.new("test", data: data)

      folds = Splitter.k_fold(dataset, k: 5, shuffle: false)

      all_test_data =
        folds
        |> Enum.flat_map(fn {_train, test} -> test.data end)
        |> Enum.sort_by(& &1.x)

      assert length(all_test_data) == 50
      assert Enum.map(all_test_data, & &1.x) == Enum.to_list(1..50)
    end
  end

  describe "stratified_split/2" do
    test "maintains class distribution in splits" do
      # Create dataset with 60% class A, 40% class B
      data =
        Enum.map(1..60, &%{x: &1, label: :a}) ++
          Enum.map(1..40, &%{x: &1, label: :b})

      dataset = Dataset.new("test", data: data)

      {train, test} =
        Splitter.stratified_split(dataset,
          label_key: :label,
          ratio: 0.8,
          seed: 42
        )

      # Count labels in each split
      train_a = Enum.count(train.data, &(&1.label == :a))
      train_b = Enum.count(train.data, &(&1.label == :b))
      test_a = Enum.count(test.data, &(&1.label == :a))
      test_b = Enum.count(test.data, &(&1.label == :b))

      # Check proportions are maintained (approximately)
      train_ratio_a = train_a / (train_a + train_b)
      test_ratio_a = test_a / (test_a + test_b)

      assert_in_delta train_ratio_a, 0.6, 0.05
      assert_in_delta test_ratio_a, 0.6, 0.05
    end
  end
end
