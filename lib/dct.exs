defmodule Dct do 
 @p Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
 def dct_txt do
   {:ok, p} = @p 
   {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT Train_Num FROM TrainschdInfo", [], timeout: 500000)
   station_ids = fetch_station_ids
   sch = fetch_train_days
   result = result
   |> Map.get(:rows)
#   |> Enum.map(fn [x] -> get_c(x, sch, station_ids) end )
   |> Enum.chunk(500, 500)
   |> Enum.map(fn x -> spawn(Dct, :get_c_many, [x, sch, station_ids]) end)
   IO.puts "Waiting"
   :timer.sleep(:infinity)
 end

 def get_c_many(x, sch, station_ids) do
  Enum.map(x, fn [y] -> get_c(y, sch, station_ids) end)
 end

 def get_c(train_id, sch, station_ids) do 
   {:ok, p} = @p
   {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainschdInfo WHERE Train_Num = #{train_id}", [], timeout: 500000)
   IO.puts "started writing  --- #{train_id}"
   result = result
   |> Map.get(:rows)  
   combination(2, result)
   |> Enum.map(fn x-> make_connection(x, sch, station_ids) end) 
#   |> Enum.filter(fn x -> x != nil end)
#   |> Enum.map(fn x -> spawn(fn -> final_connection_data(x, sch) end)end)
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
#   |> Enum.filter(fn x -> x != nil end)
#   write(schedule)
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
#    try do
     dep_s = dep_s + (86400 * days)
     arr_s = arr_s + (86400 * days)
     if arr_s < dep_s do
       arr_s = arr_s + 86400
     end
     write([train_number, o_id, d_id, dep_s, arr_s,  mode])
#    rescue
#     _ ->
#      IO.puts "error *************************************************************************************************************"
#      nil
#    end
  end
 end


 def make_connection([src, dest], sch, station_ids) do
  [_, train_id, src_s, _,  _ , dep_t] =  src
  [_, _, dest_s, _, arr_t, _] = dest
  src_id = get_station_id(src_s, station_ids)
  dest_id = get_station_id(dest_s, station_ids)
  if (src_id != nil && dest_id != nil) do
    x = [train_id, src_id, dest_id, time_to_seconds(dep_t), time_to_seconds(arr_t), "train"]
    final_connection_data(x, sch)
  else 
    nil
  end
 end

 def get_station_id(code, station_ids) do
  entry =  station_ids |> Enum.find(fn {station_code, id} -> String.rstrip(station_code) == String.rstrip(code)  end ) 
  case entry do
    nil -> 
      nil  
    _ ->
      entry |> Tuple.to_list |> Enum.at(1)
   end
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

 def combination(0, _), do: [[]]
 def combination(_, []), do: []
 def combination(n, [x|xs]) do
   (for y <- combination(n - 1, xs), do: [x|y]) ++ combination(n, xs)
 end

 def fetch_station_ids do 
  {:ok, p} = @p
  {:ok,codes} = Mariaex.Connection.query(p, "SELECT DISTINCT stn_code FROM TrainschdInfo")
  codes = codes |> Map.get(:rows)
  {:ok,info} = Mariaex.Connection.query(p, "SELECT * FROM StationInfo")
  info = info |> Map.get(:rows)
  {:ok, names} = Mariaex.Connection.query(p, "SELECT DISTINCT id, station_name FROM station_details")
  names = names |> Map.get(:rows)
  {:ok,seq} = Mariaex.Connection.query(p, "SELECT * FROM seq")
  seq = seq |> Map.get(:rows)
  IO.inspect codes
  IO.inspect names
  IO.inspect seq
  IO.inspect info
  result = codes
  |> Enum.map(fn [x] -> {x, get_station_id(x, info, names, seq)} end)
 end


 def get_station_id(code, info, names, seq) do
  #{:ok, p} = @p
  #{:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM StationInfo WHERE Station_Code = \"#{code}\"")
  #result = Map.get(result, :rows)
  #|> Enum.at(0)
  result = Enum.find(info, fn [_, _, x] -> String.rstrip(x) == String.rstrip(code)  end)
  case result do
   nil ->
    nil
   _   ->
    [_, name, _] = result
    #{:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM station_details WHERE station_name = \"#{Enum.at(result, 1)}\"")
    #result = Map.get(result, :rows) |> Enum.at(0)
    station_detail = Enum.find(names, fn [_, x] -> x == name  end) |> Enum.at(0)
    seq_id = Enum.find(seq, fn [seq_id_a, id] -> id == station_detail end) 
    id = Enum.at(seq_id, 0)
    #{:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM seq WHERE station_id = #{id}")
    # Map.get(result, :rows) |> Enum.at(0) |> Enum.at(0)
  end
 end

 def fetch_train_days do
  {:ok, p} = @p
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainDaysInfo")
  result = result
  |> Map.get(:rows)
  result
 end

end


IO.inspect Dct.dct_txt
#IO.inspect Dct.fetch_station_ids
