defmodule Connection do
 def fetch_train_connections(size, offset) do 
  #{:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  sch = fetch_train_days
  tasks = (1..50879)
  |> Enum.chunk(1000, 1000,  [])
  |> Enum.map(fn x -> Task.async(fn -> batch_trips(x, sch) end) end)
  Task.yield_many(tasks, 60000000)
 end

 def fetch_train_days do 
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainDaysInfo")
  result = result 
  |> Map.get(:rows)
  result
 end

 def batch_trips(batch, sch) do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  Enum.map(batch, fn x -> find_trip(x, p, sch) end)
 end

 def find_trip(i, p, sch) do
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainschdInfo where ID in(#{i}, #{i}+1)")
  case length(Map.get(result, :rows)) do
   1 ->
    nil
   _ ->
   [src, dst] = Map.get(result, :rows)
   connection = connection_from_trip(src, dst, p)
   train_set = nil
   if connection != nil && Enum.at(connection, 1) != nil && Enum.at(connection, 2) != nil do
    train_set = final_connection_data(connection, sch) 
#    train_set |> save_connection
    train_set = train_set
    |> Enum.map(fn x -> Enum.join(x, " ")end)
    |> Enum.join("\n")
    File.write("data/connections.txt", train_set <> "\n", [:append])
   end
   train_set
  end
 end

def save_connection(x) do
{:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
 x
 |> Enum.map(fn x -> parse_row(x) end)
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

 def final_connection_data(sample, sch) do
   [train_number, o_id, d_id, dep_s, arr_s, _] = sample
   schedule = sch
   |> Enum.find(fn x -> String.contains?(Enum.at(x, 0), "#{train_number}") end)
   |> Enum.at(1)
   |> String.split(" ")
   |> Enum.map(fn x -> final_connection(x, sample) end)
   |> Enum.filter(fn x -> x != nil end)
 end

 def final_connection(selected, sample) do
  day_struct = %{"MON" => 0, "TUE" => 1, "WED" => 2, "THU" => 3, "FRI" => 4, "SAT" => 5, "SUN" => 6}
  days = Map.get(day_struct, selected)
  [train_number, o_id, d_id, dep_s, arr_s, mode] = sample
  case days do 
   nil -> 
    nil
   _ -> 
    try do
     dep_s = dep_s + (86400 * days)
     arr_s = arr_s + (86400 * days)
     if arr_s < dep_s do
       arr_s = arr_s + 86400
     end

     [train_number, o_id, d_id, dep_s, arr_s,  mode]
    rescue
     _ -> 
      IO.puts "error *************************************************************************************************************"
      nil
    end
  end
 end

 def connection_from_trip(src, dst, p) do
  [_, train_number, o_code, _, _, dep] = src
  [_, _, d_code, _, arr, _]= dst
  o_id = get_station_id(o_code, p)
  d_id = get_station_id(d_code, p)
  case dep do 
    "Destination" ->
     nil
    _ -> 
     arr_s = time_to_seconds(arr)
     dep_s = time_to_seconds(dep)
     [train_number, o_id, d_id, dep_s, arr_s, "train"]  
  end
 end

 def get_station_id(code, p) do
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM StationInfo WHERE Station_Code = \"#{code}\"")
  result = Map.get(result, :rows)  
  |> Enum.at(0)
  case result do 
   nil -> 
    nil
   _   ->
    {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM station_details WHERE station_name = \"#{Enum.at(result, 1)}\"")
    result = Map.get(result, :rows) |> Enum.at(0)
    id = Enum.at(result, 0)
    {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM seq WHERE station_id = #{id}")
     Map.get(result, :rows) |> Enum.at(0) |> Enum.at(0)
  end
 end

 def time_to_seconds(time) do
  case time do 
   "Source" ->
     nil
   _ ->
   [hour, minute] = String.split(time, ":")
    hour_s = String.to_integer(hour) * 3600 
    minute_s = String.to_integer(minute) * 60
    hour_s + minute_s
  end
 end

end

Connection.fetch_train_connections(10, 0)
#Connection.fetch_train_days
