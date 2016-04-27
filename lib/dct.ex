defmodule Dct do 
 def dct_txt do
   {:ok, p} = Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
   {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT Train_Num FROM TrainschdInfo", [], timeout: 500000)
   station_ids = fetch_station_ids
   :ets.new(:station_ids, [:public, :named_table])
   IO.inspect "starting ets update"
   station_ids
   |> Enum.map(fn {code, id} -> :ets.insert(:station_ids, {String.strip(code), id}) end)
   sch = fetch_train_days
   result = result
   |> Map.get(:rows)
   |> Enum.chunk(500, 500, [])
   |> Enum.map(fn x -> spawn(fn -> get_c_many(x, sch, p) end) end)
   :timer.sleep(:infinity)
 end

 def get_c_many(x, sch, p) do
  Enum.map(x, fn [y] -> get_c(y, sch, p) end)
 end

 def get_c(train_id, sch, p) do 
   {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainschdInfo WHERE Train_Num = #{train_id}", [], timeout: 500000)
   IO.puts "started writing  --- #{train_id}"
   result = result
   |> Map.get(:rows)  
   |> Enum.map(fn x -> format_entry(x) end)
   combination(result)
   |> Enum.map(fn x-> make_connection(x, sch, p) end) 
 end

 def format_entry(entry) do
   [id, train_id, station, route, arr, dep] = entry
   #station_id = get_station_id(station)
   {_, station_id} = :ets.lookup(:station_ids, String.strip(station)) |> Enum.at(0)
   [id, train_id, station_id, route, arr, dep]  
 end

 def write(train_set) do
    train_set = train_set
    |> Enum.join(" ")
    File.write("data/dct.txt", train_set <> "\n", [:append])
 end


 def final_connection_data(sample, sch) do
   [train_number, o_id, d_id, dep_s, arr_s, _] = sample
   schedule = sch
   |> Enum.find(fn x -> String.contains?(Enum.at(x, 0), "#{train_number}") end)
   |> Enum.at(1)
   |> String.split(" ")
   |> Enum.map(fn x -> final_connection(x, sample) end)
 end

 def final_connection(selected, sample) do
  day_struct = %{"MON" => 0, "TUE" => 1, "WED" => 2, "THU" => 3, "FRI" => 4, "SAT" => 5, "SUN" => 6}
  days = Map.get(day_struct, selected)
  [train_number, o_id, d_id, dep_s, arr_s, mode] = sample
  case {days, dep_s && arr_s} do
   {_, nil} ->
    nil
   {nil, _} ->
    nil
   _ ->
     dep_s = dep_s + (86400 * days)
     arr_s = arr_s + (86400 * days)
     if arr_s < dep_s do
       arr_s = arr_s + 86400
     end
     write([train_number, o_id, d_id, dep_s, arr_s,  mode])
  end
 end


 def make_connection([src, dest], sch, p) do
  [_, train_id, src_id, _,  _ , dep_t] =  src
  [_, _, dest_id, _, arr_t, _] = dest
  if (src_id != nil && dest_id != nil) do
    x = [train_id, src_id, dest_id, time_to_seconds(dep_t), time_to_seconds(arr_t), "train"]
    final_connection_data(x, sch)
  else 
    nil
  end
 end

 def get_station_id(code) do
  # entry =  station_ids |> Enum.find(fn {station_code, id} -> String.rstrip(station_code) == String.rstrip(code)  end ) 
  # case entry do
  #   nil -> 
  #     nil  
  #   _ ->
  #     entry |> Tuple.to_list |> Enum.at(1)
  #  end
 end

 def time_to_seconds(time) do
  case time do
   "Source" ->
     nil 
   "<FONT COLOR = red>Slip So" -> 
     nil
   "Destination" ->
     nil
   _ ->   
    [hour, minute] = String.split(time, ":")
    hour_s = String.to_integer(hour) * 3600
    minute_s = String.to_integer(minute) * 60
    hour_s + minute_s
  end
 end

 def parse_row([vehicle_id, src, dest, dep_t, arr_t, mode]) do
  "('#{vehicle_id}', '#{src}', '#{dest}', '#{dep_t}', '#{arr_t}', '#{mode}')"
 end

 # def combination(0, _), do: [[]]
 # def combination(_, []), do: []
 # def combination(n, [x|xs]) do
 #   (for y <- combination(n - 1, xs), do: [x|y]) ++ combination(n, xs)
 # end

 def combination(list) do
    list
    |> Enum.map(fn x ->  Task.async(fn -> get_combinations(x, list) end) end)
    |> Enum.map(&Task.await/1)
    |> Enum.concat
  end

  def get_combinations(x, list) do
    index = Enum.find_index(list, fn y -> y == x end)
    list
    |> Enum.slice(index, length(list) - 1)
    |> Enum.map(fn y -> [x, y] end)
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
    station_detail = Enum.find(names, fn [_, x] -> x == name  end) |> Enum.at(0)
    seq_id = Enum.find(seq, fn [seq_id_a, id] -> id == station_detail end) 
    id = Enum.at(seq_id, 0)
  end
 end

 def fetch_train_days do
  {:ok, p} = Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainDaysInfo", [], timeout: 99999)
  result = result
  |> Map.get(:rows)
  result
 end

end