defmodule GeocodeBusStations do

 def create_bus_stops do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT station_name from station_details WHERE mode=\"train\"")
  stations = Map.get(result, :rows)
  results = stations
  |> Enum.map(fn(x) -> save_stop(x, p) end)
 end

 def save_stop(row, p) do
   name = Enum.at(row, 0)
   x = Mariaex.Connection.query(p, "INSERT INTO station_details (station_name, mode) VALUES (\"#{name}\", \"bus\")")
 end

 def main do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT station_name, id from station_details WHERE mode='bus' AND (address=\"\" OR address LIKE \"%error%\")")
  stations = Map.get(result, :rows)
  results = stations
  |> Enum.slice(0, 30)
  |> Enum.map(fn(x) -> x |> geocode_bus end)
  ""
 end

 def geocode_bus(stop) do
  [name, id] = stop
  
  url_name = String.replace(name, " ", "%20")
  url = "https://maps.googleapis.com/maps/api/geocode/json?address=#{url_name}%20bus%20station,India&key=AIzaSyCgEUBBK2jh3eMcT7RajrxNolcmpZBiSmQ"
  case HTTPoison.get(url) do
   {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
     {:ok, address} = body |> String.replace("\n", "") |> JSON.decode
     save_bus_address(id, address)
     name
   _  ->
     ""
  end
 end

 def save_bus_address(id, address) do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  {:ok, address_string} = JSON.encode(address)
  IO.puts id
  Mariaex.Connection.query(p, "UPDATE station_details set address = '#{address_string}' WHERE id = #{id}")
 end

end
GeocodeBusStations.main
#GeocodeBusStations.create_bus_stops
