# DatasetsEx

Dataset management library for ML experiments in Elixir.

DatasetsEx provides a comprehensive toolkit for managing datasets in machine learning pipelines, with built-in support for standard datasets (SciFact, FEVER), custom dataset creation, versioning, splitting, and export to multiple formats.

## Features

- **Standard Dataset Loading**: Built-in loaders for SciFact, FEVER, and other common ML datasets
- **Custom Datasets**: Create and manage your own datasets with flexible schemas
- **Versioning & Lineage**: Track dataset versions with full lineage history
- **Smart Splitting**: Train/test splits with support for stratification and k-fold cross-validation
- **Multiple Formats**: Import/export JSONL, JSON, and CSV formats
- **Reproducibility**: Deterministic splits with seed support

## Installation

Add `datasets_ex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:datasets_ex, "~> 0.1.0"}
  ]
end
```

## Quick Start

### Loading Standard Datasets

```elixir
# Load entire dataset
{:ok, scifact} = DatasetsEx.load(:scifact)

# Load specific split with limit
{:ok, fever_train} = DatasetsEx.load(:fever, split: :train, limit: 1000)

# List available datasets
DatasetsEx.list()
# => [:scifact, :fever]

# Get dataset info
DatasetsEx.info(:scifact)
# => %{name: :scifact, size: 5183, splits: [:train, :test], ...}
```

### Loading Custom Files

```elixir
# Load JSONL file
{:ok, dataset} = DatasetsEx.load_file("data.jsonl")

# Load CSV with options
{:ok, dataset} = DatasetsEx.load_file("data.csv",
  format: :csv,
  schema: :text_classification
)

# Load JSON
{:ok, dataset} = DatasetsEx.load_file("data.json")
```

### Creating Custom Datasets

```elixir
{:ok, dataset} = DatasetsEx.create("my_dataset", %{
  data: [
    %{claim: "The sky is blue", evidence: "...", label: "SUPPORTS"},
    %{claim: "The earth is flat", evidence: "...", label: "REFUTES"}
  ],
  schema: :claim_evidence,
  metadata: %{
    source: "manual_annotation",
    created_by: "researcher@example.com"
  }
})
```

### Splitting Datasets

```elixir
# Simple train/test split
{train, test} = DatasetsEx.split(dataset, ratio: 0.8, seed: 42)

# Train/validation/test split
{train, val, test} = DatasetsEx.split_three(dataset,
  ratios: [0.7, 0.15, 0.15],
  seed: 42
)

# K-fold cross-validation
folds = DatasetsEx.k_fold(dataset, k: 5, seed: 42)
for {train_fold, test_fold} <- folds do
  # Train and evaluate on each fold
end

# Stratified split (maintains class distribution)
{train, test} = DatasetsEx.stratified_split(dataset,
  label_key: :label,
  ratio: 0.8,
  seed: 42
)
```

### Versioning & Lineage

```elixir
# Create a version
{:ok, v1} = DatasetsEx.version(dataset, "v1.0.0")

# Load specific version
{:ok, dataset} = DatasetsEx.load_version("my_dataset", "v1.0.0")

# List all versions
DatasetsEx.list_versions("my_dataset")
# => ["v1.0.0", "v1.1.0", "v2.0.0"]

# Get version history
DatasetsEx.lineage("my_dataset")
# => [
#   %{version: "v2.0.0", hash: "abc...", created_at: ~U[...], size: 1500},
#   %{version: "v1.1.0", hash: "def...", created_at: ~U[...], size: 1200},
#   %{version: "v1.0.0", hash: "ghi...", created_at: ~U[...], size: 1000}
# ]
```

### Exporting Datasets

```elixir
# Export to JSONL
DatasetsEx.export(dataset, format: :jsonl, path: "output.jsonl")

# Export to JSON (pretty-printed)
DatasetsEx.export(dataset, format: :json, path: "output.json", pretty: true)

# Export to CSV
DatasetsEx.export(dataset, format: :csv, path: "output.csv")

# Export specific split
DatasetsEx.export(dataset, format: :jsonl, path: "train.jsonl", split: :train)

# Export with limit
DatasetsEx.export(dataset, format: :jsonl, path: "sample.jsonl", limit: 100)
```

### Working with Dataset Splits

```elixir
# Get specific split
train_data = DatasetsEx.get_split(dataset, :train)

# List available splits
DatasetsEx.list_splits(dataset)
# => [:train, :test, :validation]

# Get dataset size
DatasetsEx.size(dataset)
# => 5183
```

## Architecture

```
datasets_ex/
├── lib/
│   └── datasets_ex/
│       ├── dataset.ex          # Core Dataset struct
│       ├── registry.ex         # Dataset catalog (GenServer)
│       ├── loader.ex           # Multi-format loader
│       ├── loaders/
│       │   ├── scifact.ex      # SciFact dataset loader
│       │   ├── fever.ex        # FEVER dataset loader
│       │   └── jsonl.ex        # Generic JSONL loader
│       ├── splitter.ex         # Train/test splitting
│       ├── versioning.ex       # Version management
│       └── export.ex           # Multi-format export
├── priv/
│   └── datasets/               # Cached datasets and versions
└── test/
    └── datasets_ex/            # Comprehensive test suite
```

## Built-in Datasets

### SciFact
- **Size**: 5,183 claims
- **Splits**: train, test
- **Schema**: claim_evidence
- **Task**: Scientific claim verification

### FEVER
- **Size**: 185,445 claims
- **Splits**: train, dev, test
- **Schema**: claim_evidence
- **Task**: Fact extraction and verification

## Design Patterns

### Dataset Struct
All datasets use a consistent structure:

```elixir
%DatasetsEx.Dataset{
  name: "my_dataset",
  data: [%{...}, %{...}],           # Optional: flat data
  splits: %{train: [...], test: [...]},  # Optional: pre-split data
  schema: :claim_evidence,
  metadata: %{source: "...", ...},
  version: "v1.0.0",
  hash: "abc123..."                 # SHA-256 of content
}
```

### Reproducibility
All splitting operations support seeded randomization:

```elixir
# Same seed = same splits
{train1, test1} = DatasetsEx.split(dataset, seed: 42)
{train2, test2} = DatasetsEx.split(dataset, seed: 42)

train1.data == train2.data  # => true
```

### Streaming & Memory Efficiency
Large datasets are processed using Elixir streams:

```elixir
# Only loads what's needed
{:ok, dataset} = DatasetsEx.load(:fever, limit: 1000, offset: 5000)
```

## Use Cases

### CNS (Critic-Network Synthesis)
```elixir
# Load SciFact for claim extraction training
{:ok, scifact} = DatasetsEx.load(:scifact, split: :train)

# Create versioned training set
{:ok, v1} = DatasetsEx.version(scifact, "training-v1.0.0")

# Export for external training
DatasetsEx.export(v1, format: :jsonl, path: "training_data.jsonl")
```

### Cross-validation Experiments
```elixir
dataset = load_dataset()
folds = DatasetsEx.k_fold(dataset, k: 5, seed: 42)

results = for {train, test} <- folds do
  model = train_model(train)
  evaluate(model, test)
end

avg_score = Enum.sum(results) / length(results)
```

### Dataset Curation Pipeline
```elixir
# Load raw data
{:ok, raw} = DatasetsEx.load_file("raw_data.jsonl")

# Clean and transform
cleaned_data = Enum.map(raw.data, &clean_example/1)
{:ok, cleaned} = DatasetsEx.create("cleaned_dataset", %{data: cleaned_data})

# Version it
{:ok, v1} = DatasetsEx.version(cleaned, "v1.0.0")

# Split for experiments
{train, val, test} = DatasetsEx.split_three(v1, seed: 42)

# Export splits
DatasetsEx.export(train, format: :jsonl, path: "train.jsonl")
DatasetsEx.export(val, format: :jsonl, path: "val.jsonl")
DatasetsEx.export(test, format: :jsonl, path: "test.jsonl")
```

## Testing

Run the test suite:

```bash
mix test
mix test --cover
```

## Documentation

Generate documentation:

```bash
mix docs
```

View at `doc/index.html`

## Contributing

DatasetsEx is part of the North Shore AI monorepo. Contributions welcome!

## License

MIT

