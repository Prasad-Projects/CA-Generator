defmodule Airline do

@p Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")

def fetch_connections do
 station_list = get_station_lists
 (1..2501)
 |> Enum.chunk(200, 200, [])
 |> Enum.map(fn x ->  connection_data(x, station_list) end)
end

def connection_data(batch, station_list) do
 {:ok, p} = @p
 ids = Enum.join(batch, ", ")
 {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM airline where sys_id in(#{ids})")
 result = Map.get(result, :rows)
 result = result
 |> Enum.map(fn x -> prepare_line(x, station_list) end)
 |> Enum.filter(fn x -> x != nil && (Enum.at(x, 1) != nil && Enum.at(x, 2) != nil )end)
 result
 |> Enum.each(fn x -> save_connection(p, x) end)
 result = result
 |> Enum.map(fn x -> Enum.join(Enum.map(x, fn y -> Enum.join(y, " ") end ), "\n") end)
 |> Enum.join("\n")
 File.write("data/connections_airline.txt", result <> "\n", [:append])
end

def save_connection(p, x) do
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

def prepare_line(row, station_list) do
 [id, dep, _, arr, _, plane_id, days, dep_t, arr_t | _] = row
 dep_t = get_seconds(dep_t)
 arr_t = get_seconds(arr_t)
 dep = get_station_id(dep, station_list)
 arr = get_station_id(arr, station_list)
 if dep_t > arr_t do
  arr_t = arr_t + 86400
 end
 plane_id = Enum.join(String.split(plane_id, " "), "_")
 sample = [plane_id, dep, arr, dep_t, arr_t, "plane"]
 try do  
  get_connections(sample, days)
 rescue 
  _ ->
   nil
 end
end

def get_connections(sample, days) do
 if days == "Daily" do
  days = "1234567"
 end
 connections =  days
 |> String.split("")
 |> Enum.filter(fn x -> "#{x}" != "" && String.to_integer(x) != 0 end)
 |> Enum.map(fn x -> get_day_connection(x, sample) end)
end

def get_day_connection(day, sample) do
 [plane_id, dep, arr, dep_t, arr_t, mode] = sample
 if String.to_integer(day) == 0 do
  IO.inspect day
 end
 day = String.to_integer(day) - 1
 dep_t = (day * 86400) + dep_t
 arr_t = (day * 86400) + arr_t
 [plane_id, dep, arr, dep_t, arr_t, mode]
end

def get_seconds(time) do
 [hour, minute] = Enum.slice(Tuple.to_list(time), 0, 2)
 (hour * 3600) + (minute * 60)
end

def get_station_id(dep, station_list) do
 station = station_list
 |> Enum.find(fn x -> Enum.at(x, 1) == dep end)
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
 {:ok, p} = @p
 {:ok, result} = Mariaex.Connection.query(p, "SELECT id, station_name FROM station_details WHERE mode='airport'")
 result = result
 |> Map.fetch!(:rows)
 result
end

end

Airline.fetch_connections()
