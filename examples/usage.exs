# DatasetsEx Usage Examples
# Run with: mix run examples/usage.exs

require Logger

Logger.info("DatasetsEx Usage Examples")
Logger.info("=" |> String.duplicate(50))

# Example 1: Create a custom dataset
Logger.info("\n1. Creating a custom dataset...")

{:ok, dataset} =
  DatasetsEx.create("example_dataset", %{
    data: [
      %{claim: "The sky is blue", evidence: "Visual observation", label: "SUPPORTS"},
      %{claim: "Water is dry", evidence: "Common knowledge", label: "REFUTES"},
      %{claim: "Cats are mammals", evidence: "Scientific classification", label: "SUPPORTS"}
    ],
    schema: :claim_evidence,
    metadata: %{
      created_by: "example_script",
      source: "manual"
    }
  })

Logger.info("Created dataset: #{dataset.name}")
Logger.info("Size: #{DatasetsEx.size(dataset)} examples")

# Example 2: Split the dataset
Logger.info("\n2. Splitting dataset...")

{train, test} = DatasetsEx.split(dataset, ratio: 0.67, seed: 42)

Logger.info("Train size: #{DatasetsEx.size(train)}")
Logger.info("Test size: #{DatasetsEx.size(test)}")

# Example 3: Export to different formats
Logger.info("\n3. Exporting dataset...")

tmp_dir = System.tmp_dir!()

# Export to JSONL
jsonl_path = Path.join(tmp_dir, "dataset.jsonl")
{:ok, _} = DatasetsEx.export(train, format: :jsonl, path: jsonl_path)
Logger.info("Exported to JSONL: #{jsonl_path}")

# Export to JSON
json_path = Path.join(tmp_dir, "dataset.json")
{:ok, _} = DatasetsEx.export(train, format: :json, path: json_path, pretty: true)
Logger.info("Exported to JSON: #{json_path}")

# Export to CSV
csv_path = Path.join(tmp_dir, "dataset.csv")
{:ok, _} = DatasetsEx.export(train, format: :csv, path: csv_path)
Logger.info("Exported to CSV: #{csv_path}")

# Example 4: Version the dataset
Logger.info("\n4. Versioning dataset...")

{:ok, v1} = DatasetsEx.version(dataset, "v1.0.0")
Logger.info("Created version: #{v1.version}")
Logger.info("Hash: #{String.slice(v1.hash, 0..15)}...")

# Example 5: Three-way split
Logger.info("\n5. Three-way split (train/val/test)...")

{:ok, larger_dataset} =
  DatasetsEx.create("larger_example", %{
    data:
      Enum.map(1..30, fn i ->
        %{id: i, text: "Example #{i}", value: i * 10}
      end)
  })

{train, val, test} = DatasetsEx.split_three(larger_dataset, seed: 42)

Logger.info("Train: #{DatasetsEx.size(train)} examples")
Logger.info("Val: #{DatasetsEx.size(val)} examples")
Logger.info("Test: #{DatasetsEx.size(test)} examples")

# Example 6: K-fold cross-validation
Logger.info("\n6. K-fold cross-validation...")

folds = DatasetsEx.k_fold(larger_dataset, k: 5, seed: 42)

Logger.info("Created #{length(folds)} folds")

Enum.with_index(folds, fn {fold_train, fold_test}, idx ->
  Logger.info(
    "  Fold #{idx + 1}: train=#{DatasetsEx.size(fold_train)}, test=#{DatasetsEx.size(fold_test)}"
  )
end)

# Example 7: List available datasets
Logger.info("\n7. Available datasets...")

datasets = DatasetsEx.list()
Logger.info("Registered datasets: #{Enum.join(datasets, ", ")}")

# Example 8: Load from file
Logger.info("\n8. Loading from file...")

# Create a test file
test_jsonl = Path.join(tmp_dir, "test_input.jsonl")

File.write!(test_jsonl, """
{"id": 1, "text": "First example"}
{"id": 2, "text": "Second example"}
{"id": 3, "text": "Third example"}
""")

{:ok, loaded} = DatasetsEx.load_file(test_jsonl, schema: :test)
Logger.info("Loaded #{DatasetsEx.size(loaded)} examples from file")

Logger.info("\n" <> ("=" |> String.duplicate(50)))
Logger.info("Examples completed successfully!")
Logger.info("Temporary files created in: #{tmp_dir}")
