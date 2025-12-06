defmodule DatasetsEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :datasets_ex,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Dataset management library for ML experiments",
      package: package(),
      docs: docs()
    ]
  end

  defp package do
    [
      maintainers: ["North Shore AI"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/North-Shore-AI/datasets_ex"}
    ]
  end

  defp docs do
    [
      main: "DatasetsEx",
      extras: ["README.md"]
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
      {:jason, "~> 1.4"},
      {:nimble_csv, "~> 1.2"},
      {:req, "~> 0.4"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end
end
