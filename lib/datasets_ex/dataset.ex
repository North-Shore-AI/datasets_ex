defmodule DatasetsEx.Dataset do
  @moduledoc """
  Core dataset structure for managing ML datasets.

  A dataset contains data, metadata, and optional splits for training/testing.
  """

  @type split :: :train | :test | :validation | atom()
  @type data :: [map()]
  @type splits :: %{split() => data()}

  @type t :: %__MODULE__{
          name: String.t(),
          data: data() | nil,
          splits: splits(),
          schema: atom() | nil,
          metadata: map(),
          version: String.t() | nil,
          hash: String.t() | nil
        }

  defstruct [
    :name,
    :data,
    :schema,
    :version,
    :hash,
    splits: %{},
    metadata: %{}
  ]

  @doc """
  Creates a new dataset.

  ## Examples

      iex> DatasetsEx.Dataset.new("my_dataset", data: [%{text: "hello"}])
      %DatasetsEx.Dataset{name: "my_dataset", data: [%{text: "hello"}]}
  """
  def new(name, opts \\ []) do
    %__MODULE__{
      name: name,
      data: Keyword.get(opts, :data),
      splits: Keyword.get(opts, :splits, %{}),
      schema: Keyword.get(opts, :schema),
      metadata: Keyword.get(opts, :metadata, %{}),
      version: Keyword.get(opts, :version),
      hash: Keyword.get(opts, :hash)
    }
  end

  @doc """
  Returns the size of the dataset.
  """
  def size(%__MODULE__{data: data}) when is_list(data), do: length(data)

  def size(%__MODULE__{splits: splits}) when map_size(splits) > 0 do
    splits
    |> Map.values()
    |> Enum.map(&length/1)
    |> Enum.sum()
  end

  def size(_), do: 0

  @doc """
  Gets a specific split from the dataset.
  """
  def get_split(%__MODULE__{splits: splits}, split) do
    Map.get(splits, split)
  end

  @doc """
  Lists available splits.
  """
  def list_splits(%__MODULE__{splits: splits}) do
    Map.keys(splits)
  end

  @doc """
  Adds or updates a split in the dataset.
  """
  def put_split(%__MODULE__{} = dataset, split, data) do
    %{dataset | splits: Map.put(dataset.splits, split, data)}
  end

  @doc """
  Computes a hash for the dataset content.
  """
  def compute_hash(%__MODULE__{data: data}) when is_list(data) do
    :crypto.hash(:sha256, :erlang.term_to_binary(data))
    |> Base.encode16(case: :lower)
  end

  def compute_hash(%__MODULE__{splits: splits}) when map_size(splits) > 0 do
    :crypto.hash(:sha256, :erlang.term_to_binary(splits))
    |> Base.encode16(case: :lower)
  end

  def compute_hash(_), do: nil

  @doc """
  Updates the dataset hash.
  """
  def with_hash(%__MODULE__{} = dataset) do
    %{dataset | hash: compute_hash(dataset)}
  end
end
