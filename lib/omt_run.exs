defmodule OMTRun do
 def main do
  {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
  {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT station_name FROM station_details WHERE mode='airport'")
  result = Map.get(result, :rows)
  result 
  |> Enum.map(fn x ->  run(x |> Enum.at(0)) end)
 end

 def run(x) do
  IO.inspect :os.cmd('mix run lib/mmtp.exs search #{x}' )
 end
end
OMTRun.main()
