defmodule DatasetsEx.LoadersTest do
  use ExUnit.Case, async: false

  alias DatasetsEx.{Loaders, Registry}

  describe "GSM8K loader" do
    test "loads GSM8K dataset" do
      info = Registry.info(:gsm8k)
      {:ok, dataset} = Loaders.Gsm8k.load(info, split: :train)

      assert dataset.name == "gsm8k"
      assert dataset.schema == :math_word_problems
      assert dataset.metadata.task == "math_reasoning"
    end
  end

  describe "HumanEval loader" do
    test "loads HumanEval dataset" do
      info = Registry.info(:human_eval)
      {:ok, dataset} = Loaders.HumanEval.load(info)

      assert dataset.name == "human_eval"
      assert dataset.schema == :code_generation
      assert dataset.metadata.task == "code_generation"
    end
  end

  describe "MMLU loader" do
    test "loads MMLU dataset" do
      info = Registry.info(:mmlu)
      {:ok, dataset} = Loaders.Mmlu.load(info, split: :test)

      assert dataset.name == "mmlu"
      assert dataset.schema == :multiple_choice
      assert dataset.metadata.task == "knowledge_evaluation"
    end
  end

  describe "TruthfulQA loader" do
    test "loads TruthfulQA dataset" do
      info = Registry.info(:truthful_qa)
      {:ok, dataset} = Loaders.TruthfulQa.load(info)

      assert dataset.name == "truthful_qa"
      assert dataset.schema == :truthfulness
      assert dataset.metadata.task == "truthfulness_evaluation"
    end
  end

  describe "HellaSwag loader" do
    test "loads HellaSwag dataset" do
      info = Registry.info(:hellaswag)
      {:ok, dataset} = Loaders.Hellaswag.load(info, split: :train)

      assert dataset.name == "hellaswag"
      assert dataset.schema == :commonsense_nli
      assert dataset.metadata.task == "commonsense_inference"
    end
  end
end
