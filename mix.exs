defmodule QueryBuilder.MixProject do
  use Mix.Project

  @version "1.4.2"

  def project do
    [
      app: :query_builder,
      elixir: "~> 1.14",
      deps: deps(),
      aliases: aliases(),
      elixirc_paths: elixirc_paths(Mix.env()),

      # Hex
      version: @version,
      package: package(),
      description: "Compose Ecto queries without effort",

      # ExDoc
      name: "Query Builder",
      source_url: "https://github.com/mathieuprog/query_builder",
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:jason, "~> 1.4"},
      {:ecto, "~> 3.13"},
      {:ecto_sql, "~> 3.13", only: :test},
      {:postgrex, "~> 0.21", only: :test},
      {:ex_machina, "~> 2.8", only: :test},
      {:map_diff, "~> 1.3", only: :test},
      {:ex_doc, "~> 0.39", only: :dev},
      {:inch_ex, "~> 2.1", only: :dev},
      {:dialyxir, "~> 1.4", only: :dev}
    ]
  end

  defp aliases do
    [
      test: [
        "ecto.create --quiet",
        "ecto.rollback --all",
        "ecto.migrate",
        "test"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      licenses: ["Apache-2.0"],
      maintainers: ["Mathieu Decaffmeyer"],
      links: %{
        "GitHub" => "https://github.com/mathieuprog/query_builder",
        "Sponsor" => "https://github.com/sponsors/mathieuprog"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_ref: "v#{@version}"
    ]
  end
end
