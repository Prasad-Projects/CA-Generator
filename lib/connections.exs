defmodule Connection do
 def fetch_train_connections(size, offset) do 
  sch = fetch_train_days
  station_ids = fetch_station_ids
  :ets.new(:station_ids, [:public, :named_table])
  IO.puts "Running connections script (This may take a few minutes) ........"
  station_ids
  |> Enum.map(fn {code, id} -> :ets.insert(:station_ids, {String.strip(code), id}) end)

  tasks = (1..50879)
  |> Enum.chunk(1000, 1000,  [])
  |> Enum.map(fn x -> Task.async(fn -> batch_trips(x, sch) end) end)
  Task.yield_many(tasks, 60000000)
 end

 def fetch_train_days do 
  {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainDaysInfo")
  result = result 
  |> Map.get(:rows)
  result
 end

 def batch_trips(batch, sch) do
  {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
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
{:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
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
      nil
    end
  end
 end

 def connection_from_trip(src, dst, p) do
  [_, train_number, o_code, _, _, dep] = src
  [_, _, d_code, _, arr, _]= dst
  {_, o_id} = :ets.lookup(:station_ids, String.strip(o_code)) |> Enum.at(0)
  {_, d_id} = :ets.lookup(:station_ids, String.strip(d_code)) |> Enum.at(0)
  case dep do 
    "Destination" ->
     nil
    _ -> 
     arr_s = time_to_seconds(arr)
     dep_s = time_to_seconds(dep)
     [train_number, o_id, d_id, dep_s, arr_s, "train"]  
  end
 end
 
 def fetch_station_ids do 
  {:ok, p} = Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
  {:ok,codes} = Mariaex.Connection.query(p, "SELECT DISTINCT stn_code FROM TrainschdInfo")
  codes = codes |> Map.get(:rows)
  {:ok,info} = Mariaex.Connection.query(p, "SELECT * FROM StationInfo")
  info = info |> Map.get(:rows)
  {:ok, names} = Mariaex.Connection.query(p, "SELECT DISTINCT id, station_name FROM station_details", [], timeout: 999999)
  names = names |> Map.get(:rows)
  {:ok,seq} = Mariaex.Connection.query(p, "SELECT * FROM seq")
  seq = seq |> Map.get(:rows)
  result = codes
  |> Enum.map(fn [x] -> {x, get_station_id(x, info, names, seq)} end)
 end

 def get_station_id(code, info, names, seq) do
  result = Enum.find(info, fn [_, _, x] -> String.rstrip(x) == String.rstrip(code)  end)
  case result do
   nil ->
    nil
   _   ->
    [_, name, _] = result
    IO.inspect result
    station_detail = Enum.find(names, fn [_, x] -> x == name  end) |> Enum.at(0)
    seq_id = Enum.find(seq, fn [seq_id_a, id] -> id == station_detail end) 
    # if Enum.at(seq_id, 1) == "2234" do
#      IO.inspect seq_id
    #end
    id = Enum.at(seq_id, 0)
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
