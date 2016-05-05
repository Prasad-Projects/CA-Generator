defmodule Bus do 

def fetch_connections do
 station_list = get_station_lists
 (1..12219)
 |> Enum.chunk(200, 200, [])
 |> Enum.map(fn x ->  connection_data(x, station_list) end)  
end

def connection_data(batch, station_list) do
 {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
 ids = Enum.join(batch, ", ") 
 {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM mytable where ID in(#{ids})")
 result = Map.get(result, :rows)
 result = result
  |> Enum.map(fn x -> prepare_line(x, station_list) end)
 result 
 |> Enum.each(fn x -> save_connection(x, p) end)
 result = result
  |> Enum.join("\n")
 File.write("data/connections_buses.txt", result <> "\n", [:append])
end

def save_connection(x, p) do
 x
 |> String.split("\n")
 |> Enum.map(fn x -> parse_row(String.split(x, " ")) end)
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

def prepare_line(row, station_list) do
 [id, _, dep, _, arr, route_id, dep_t, arr_t | _] = row
 dep = get_station_id(dep, station_list)
 arr = get_station_id(arr, station_list)
 dep_t = dep_t * 60
 arr_t = arr_t * 60
 if arr_t < dep_t  do
   arr_t = arr_t + 86400
 end
 sample = [route_id, dep, arr, dep_t, arr_t, "bus"]
 replicate_line(sample)
end

def replicate_line([route_id, dep, arr, dep_t, arr_t, "bus"]) do
 x = (0..7)
 |> Enum.map(fn x -> [route_id, dep, arr, dep_t + (x * 86400), arr_t + (x * 86400), "bus"] end)
 |> Enum.map(fn x -> Enum.join(x, " ") end)
 |> Enum.join("\n")
end
@p Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
def get_station_id(dep, station_list) do
 station = station_list
 |> Enum.find(fn x -> dep == Enum.at(x, 1)  end)
 case station do
  nil -> 
   nil
  _ ->
   {:ok, p} = @p
   id = Enum.at(station, 0)
   {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM seq WHERE station_id = #{id}")
   Map.get(result, :rows) |> Enum.at(0) |> Enum.at(0)
 end 
end

def get_station_lists do
 {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
 {:ok, result} = Mariaex.Connection.query(p, "SELECT id, station_name FROM station_details WHERE mode='bus'")
 result = result
 |> Map.fetch!(:rows)
 result
end

end
Bus.fetch_connections
