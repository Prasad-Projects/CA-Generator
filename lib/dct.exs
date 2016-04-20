defmodule Dct do 
 @p Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
 def dct_txt do
   {:ok, p} = @p 
   {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT Train_Num FROM TrainschdInfo", [], timeout: 500000)
   sch = fetch_train_days
   result = result
   |> Map.get(:rows)
   #|> Enum.map(fn [x] -> get_c(x, sch) end )
   |> Enum.chunk(1000, 1000)
   |> Enum.map(fn x -> spawn(fn -> get_c_many(x, sch) end) end)
   #|> Task.yield_many(10000000000)
   :timer.sleep(:infinity)
 end

 def get_c_many(x, sch) do
  Enum.map(x, fn [y] -> get_c(y, sch) end)
 end

 def get_c(train_id, sch) do 
   {:ok, p} = @p
   {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainschdInfo WHERE Train_Num = #{train_id}", [], timeout: 500000)
   IO.puts "writing  --- #{train_id}"
   result = result
   |> Map.get(:rows)  
   combination(2, result)
   |> Enum.map(&(spawn( fn -> make_connection(&1, sch) end))) 
#   |> Enum.filter(fn x -> x != nil end)
#   |> Enum.map(fn x -> spawn(fn -> final_connection_data(x, sch) end)end)
 end

 def write(train_set) do
    train_set = train_set
    |> Enum.map(fn x -> Enum.join(x, " ")end)
    |> Enum.join("\n")
    File.write("data/dct.txt", train_set <> "\n", [:append])
 end


 def final_connection_data(sample, sch) do
   [train_number, o_id, d_id, dep_s, arr_s, _] = sample
   schedule = sch
   |> Enum.find(fn x -> String.contains?(Enum.at(x, 0), "#{train_number}") end)
   |> Enum.at(1)
   |> String.split(" ")
   |> Enum.map(fn x -> final_connection(x, sample) end)
   |> Enum.filter(fn x -> x != nil end)
   write(schedule)
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


 def make_connection([src, dest], sch) do
  [_, train_id, src_s, _,  _ , dep_t] =  src
  [_, _, dest_s, _, arr_t, _] = dest
  src_id = get_station_id(src_s)
  dest_id = get_station_id(dest_s)
  if (src_id != nil && dest_id != nil) do
    x = [train_id, src_id, dest_id, time_to_seconds(dep_t), time_to_seconds(arr_t), "train"]
    final_connection_data(x, sch)
  else 
    nil
  end
 end


 def time_to_seconds(time) do
  case time do
   "Source" ->
     nil 
   "<FONT COLOR = red>Slip So" -> 
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


 def get_station_id(code) do
  {:ok, p} = @p
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

 def fetch_train_days do
  {:ok, p} = @p
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainDaysInfo")
  result = result
  |> Map.get(:rows)
  result
 end

end


IO.inspect Dct.dct_txt
