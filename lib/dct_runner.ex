require Dct
defmodule DctRunner do
  import ExProf.Macro
  def go do
    File.write!("profiling/eprof.log", "Start of eprof data \n_______________________________________\n")
    (1..3)
    |> Enum.map(&run_profile/1)
  end
  
  def format_result(result) do
    output = result
    #|> Enum.filter(&(&1.function |> String.downcase |> String.contains?("dct")))
    |> Enum.sort_by(fn x -> x.time end, &>=/2)
    |> Enum.map(fn x -> [x.function, x.time, x.percent, x.calls] |> Enum.join(" | ") end)
    |> Enum.join("\n")
    File.write!("profiling/eprof.log", output <> "\n_______________________________________\n", [:append])
  end
      
  def run_profile(n) do
    result =  profile do
       Dct.dct_txt(n)
    end
    format_result(result)
  end
end
