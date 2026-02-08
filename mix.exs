defmodule DatasetsEx.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/North-Shore-AI/datasets_ex"

  def version, do: @version

  def project do
    [
      app: :datasets_ex,
      version: @version,
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      docs: docs(),
      name: "DatasetsEx",
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  defp description do
    """
    Dataset management library for ML experiments with support for GSM8K, HumanEval, MMLU loaders and evaluation metrics (BLEU, ROUGE, F1).
    """
  end

  defp package do
    [
      name: "datasets_ex",
      maintainers: ["North Shore AI"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE assets)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      assets: %{"assets" => "assets"},
      logo: "assets/datasets_ex.svg",
      extras: [
        "README.md",
        "LICENSE"
      ],
      groups_for_extras: [
        Guides: ~r/guides\/.*/
      ],
      groups_for_modules: [
        Core: [
          DatasetsEx,
          DatasetsEx.Dataset,
          DatasetsEx.Loader,
          DatasetsEx.Registry
        ],
        "Versioning & Lineage": [
          DatasetsEx.Versioning,
          DatasetsEx.Lineage
        ],
        Utilities: [
          DatasetsEx.Splitter,
          DatasetsEx.Quality,
          DatasetsEx.Transform,
          DatasetsEx.Stream,
          DatasetsEx.Export
        ]
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {DatasetsEx.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:lineage_ir, path: "../lineage_ir"},
      {:ecto, "~> 3.11"},
      {:jason, "~> 1.4"},
      {:nimble_csv, "~> 1.2"},
      {:req, "~> 0.4"},
      {:gen_stage, "~> 1.2"},
      {:ex_doc, "~> 0.40.0", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end
end
