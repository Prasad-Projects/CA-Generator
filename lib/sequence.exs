defmodule Sequence do
def main do
 {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
 {:ok,result} = Mariaex.Connection.query(p, "SELECT COUNT(*) FROM station_details")
 result = Map.get(result, :rows) |> Enum.at(0) |> Enum.at(0)
 {ok,result} = Mariaex.Connection.query(p, "SELECT id FROM station_details")
 result = Map.get(result, :rows) 
 |> Enum.reduce(1, fn x, acc -> save(x, acc, p) end)
end

def save(x, acc, p) do
 {ok,result} = Mariaex.Connection.query(p, "INSERT INTO seq (seq, station_id) VALUES (#{acc}, #{Enum.at(x, 0)})")
 acc + 1
end

def update_connections do
 {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
 {:ok,result} = Mariaex.Connection.query(p, "SELECT COUNT(*) FROM station_details")
 count = Map.get(result, :rows) |> Enum.at(0) |> Enum.at(0)
 (0..count)
 |> Enum.map(fn x ->  update_connections(p, x) end)
end

end

#IO.inspect Sequence.main
#Sequence.main
#Sequence.update_connections
