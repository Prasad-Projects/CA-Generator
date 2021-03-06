defmodule Mmtp.Mixfile do
  use Mix.Project

  def project do
    [app: :mmtp,
     version: "0.0.1",
     elixir: "~> 1.2",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger, :distancex, :httpoison, :json, :distance_api_matrix]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [{:consolex, "~> 0.0.3"}, {:mariaex, "~> 0.4.2"}, {:distancex, "~> 0.1.0"}, {:httpoison, "~> 0.8.0"}, { :json,   "~> 0.3.0"}, {:distance_api_matrix, git: "https://github.com/sivsushruth/distance-matrix-api"}, {:exprof, "~> 0.2.0"}]
  end
end
