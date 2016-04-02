defmodule StationDetails do 

def fetch_stations do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp_backup_2")
  0..3607
  |> Enum.chunk(300, 300, [])
  |> Enum.map(fn x -> transfer_data(x, p) end)
  
end

def transfer_data(range, p) do
  ids = Enum.join(range, ", ")
  {:ok, result} = Mariaex.Connection.query(p, "SELECT * FROM StationInfo WHERE id in (#{ids})")
  {:ok, p2} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  result
  |> Map.get(:rows)
  |> Enum.map(fn x -> save_in_p2(x, p2) end)
  ""
end

def save_in_p2(data, p2) do
 IO.inspect data
 [_, station_name, station_code, station_address, _] = data
 x = Mariaex.Connection.query(p2, "INSERT INTO station_details (station_name, mode, address) VALUES ('#{station_name}', 'train', '#{station_address}')")
 IO.inspect x
 station_name
end

def fetch_bus_stations do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp_backup_2")
  {:ok, result} = Mariaex.Connection.query(p, "SELECT DISTINCT dest_name, stop_address FROM mytable")
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  result
  |> Map.get(:rows)
  |> Enum.map(fn x -> transfer_bus_data(x, p) end)
end

def transfer_bus_data(row, p) do
 [station_name, station_address] = row
 x = Mariaex.Connection.query(p, "INSERT INTO station_details (station_name, mode, address) VALUES ('#{station_name}', 'bus', '#{station_address}')")
 IO.inspect x
end


def fetch_airports do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  {:ok, result} = Mariaex.Connection.query(p, "SELECT DISTINCT dest_name, stop_address FROM airline")
  result
  |> Map.get(:rows)
  |> Enum.map(fn x -> transfer_airport_data(x, p) end)
end

def transfer_airport_data(row, p) do
 [station_name, station_address] = row
 x = Mariaex.Connection.query(p, "INSERT INTO station_details (station_name, mode, address) VALUES ('#{station_name}', 'airport', '#{station_address}')")
 IO.inspect x
end


def save_airline_data(name, address) do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  {:ok, address_string} = JSON.encode(address)
  Mariaex.Connection.query(p, "UPDATE airline set stop_address = '#{address_string}' WHERE src = '#{name}'")
end

def geocode_airport(name) do
  url_name = String.replace(name, " ", "%20")
  url = "https://maps.googleapis.com/maps/api/geocode/json?address=#{url_name}%20Airport,India&key=AIzaSyCgEUBBK2jh3eMcT7RajrxNolcmpZBiSmQ"
  case HTTPoison.get(url) do
   {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
     {:ok, address} = body |> String.replace("\n", "") |> JSON.decode
     save_airline_data(name, address)
     IO.inspect address
     name
   _  ->
     ""
  end
end

def geocode_airports do
 {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
 {:ok, result} = Mariaex.Connection.query(p, "SELECT DISTINCT src, stop_address FROM airline WHERE stop_address IS NULL OR stop_address LIKE \"%error%\"")
 result
 |> Map.get(:rows)
 |> Enum.map(fn x -> geocode_airport(Enum.at(x, 0)) end)
end


end

#StationDetails.fetch_airports
StationDetails.fetch_bus_stations

