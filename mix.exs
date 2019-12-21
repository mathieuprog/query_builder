defmodule QueryBuilder.MixProject do
  use Mix.Project

  @version "0.6.0"

  def project do
    [
      app: :query_builder,
      elixir: "~> 1.9",
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
      {:ecto, "~> 3.2"},
      {:ecto_sql, "~> 3.2", only: :test},
      {:postgrex, "~> 0.14", only: :test},
      {:ex_machina, "~> 2.3", only: :test},
      {:map_diff, "~> 1.3", only: :test},
      {:ex_doc, "~> 0.21", only: :dev},
      {:inch_ex, "~> 2.0", only: :dev},
      {:dialyxir, "~> 0.5", only: :dev}
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
      licenses: ["Apache 2.0"],
      maintainers: ["Mathieu Decaffmeyer"],
      links: %{"GitHub" => "https://github.com/mathieuprog/query_builder"}
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
