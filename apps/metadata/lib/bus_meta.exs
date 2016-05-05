defmodule Bus do

@p Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")

def fetch_connections do
 station_list = get_station_lists
 (1..12219)
 |> Enum.chunk(200, 200, [])
 |> Enum.map(fn x ->  connection_data(x, station_list) end)
end

def connection_data(batch, station_list) do
 {:ok, p} = @p
 ids = Enum.join(batch, ", ")
 {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM mytable where id in(#{ids})")
 result = Map.get(result, :rows)
 result = result
 |> Enum.map(fn x -> prepare_line(x, station_list) end)
 |> Enum.filter(fn x -> x != nil && (Enum.at(x, 1) != nil && Enum.at(x, 2) != nil )end)

 #result
 save_meta(p, result)
 #|> Enum.each(fn x -> save_meta(p, x) end)
end

def save_meta(p, x) do
 x
 |> IO.inspect
 |> Enum.map(fn x -> parse_row(x) end)
 |> Enum.join(", ") 
 |> insert_values(p)
end

def insert_values(x, p) do 
 if x != "" do
  {:ok,result} = Mariaex.Connection.query(p, "INSERT INTO MetaData (src, dest, vehicle_id, mode, distance, fare, rating, isFood, isAC) VALUES #{x}")
 end
end

def parse_row([dep, arr, bus_id, _, distance, fare, rating, _, isAC]) do
 "('#{dep}', '#{arr}', '#{bus_id}', 'bus', '#{distance}', '#{fare}', '#{rating}', 'false', '#{isAC}')"
end

def prepare_line(row, station_list) do
 [id, _, dep, _, arr, bus_id, dep_t, arr_t, _, fare, rating, isAC, _ | _ ] = row
 dep = get_station_id(dep, station_list)
 arr = get_station_id(arr, station_list)
 fare_id = get_fare_id(fare)
 try do  
  [dep, arr, bus_id, "bus", 0, fare_id, rating, "false", isAC]
 rescue 
  _ ->
   nil
 end
end

def get_fare_id(fare) do
  {:ok, p} = @p
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM Fares WHERE fare = '#{fare}'")
  Map.get(result, :rows) |> Enum.at(0) |> Enum.at(0)
end

def get_seconds(time) do
 [hour, minute] = Enum.slice(Tuple.to_list(time), 0, 2)
 (hour * 3600) + (minute * 60)
end

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

Bus.fetch_connections()