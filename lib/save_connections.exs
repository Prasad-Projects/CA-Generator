defmodule SaveConnections do 

def save do
 {:ok, content} = File.read("data/connections.txt")
 content
 |> String.split("\n")
 |> Enum.chunk(2000, 2000, [])
 |> Enum.map(fn x ->  save_connection(x) end)
end

def save_connection(x) do
{:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
 x
 |> Enum.map(fn x -> parse_row(String.split(x, " ")) end)
 |> Enum.filter(fn x -> x != nil end)
 |> Enum.join(", ")
 |> insert_values(p)
end

def insert_values(x, p) do
 if x != "" do
  {:ok,result} = Mariaex.Connection.query(p, "INSERT INTO connections (vehicle_id, src, dest, dep_t, arr_t, mode) VALUES #{x}")
 end
end

def parse_row([vehicle_id, src, dest, dep_t, arr_t, mode]) do
 "('#{vehicle_id}', '#{src}', '#{dest}', '#{dep_t}', '#{arr_t}', '#{mode}')"
end

def parse_row(x) do
 nil
end

end
SaveConnections.save
