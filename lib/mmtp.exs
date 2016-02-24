defmodule Mmtp do
 
 def main do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "", database: "mmtp")
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM StationInfo WHERE address IS NULL OR address LIKE \"%error%\"")
  stations = Map.get(result, :rows)
  results = stations
  |> Enum.slice(0, 10)
  |> Enum.map(fn(x) -> x |> geocode end) 
 end
 
 def search(city) do
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "", database: "mmtp")
  {:ok, result} = Mariaex.Connection.query(p, "SELECT * FROM StationInfo WHERE address LIKE \"%#{city}%\"")   
  search_result_temp = result
   |> Map.get(:rows)
   |> Enum.at(0)
   |> Enum.at(4)
  if(search_result_temp) do
   search_results = search_result_temp
  else
   station_names = result
   |> Map.get(:rows)
   |> Enum.map(fn x -> Enum.at(x, 1)  end)
     
   stations = result
   |> Map.get(:rows)
   |> Enum.map(fn x -> Enum.at(x, 3)  end)
   |> Enum.map(fn x -> "place_id:#{place_id(x)}" end)
  
   
   IO.inspect station_names
   addresses_params  = %{origins: stations, destinations: stations}
   exact_address_distance_matrix  = DistanceMatrixApi.get_distances(addresses_params)
   search_results = formatted_search(station_names, exact_address_distance_matrix)
   persist_search(station_names, search_results)
  end
 end

 def persist_search(station_names, search_results) do
   {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "", database: "mmtp")
   station_names
   |> Enum.map(fn x ->  Mariaex.Connection.query(p, "UPDATE StationInfo set results = '#{search_results}' WHERE Station_Name = '#{x}'") end)
 end

 def formatted_search(station_names, exact_address_distance_matrix) do
  {:ok, stations_combinations} = station_names
  |> Enum.map(fn x -> combine_paths(x, station_names) end)
  |> Enum.map_reduce(0, fn x, i -> {append_data_to_path(x, i, exact_address_distance_matrix), i+1} end) 
  |> JSON.encode
  stations_combinations
 end
 
 def append_data_to_path(paths, i, matrix) do
  data_path = paths
  |> Enum.map_reduce(0, fn x,j -> {path_data(x, i, j, matrix), j+1}end)
  |> Tuple.to_list
  |> Enum.at(0)
end
 
 def path_data(path, i, j, matrix) do
  {:ok, data} = matrix["rows"]
  |> Enum.at(i)
  |> Map.fetch("elements")
  data = data
  |> Enum.at(j) 
  |> Map.merge(path)
  |> Map.delete("status")
  data
 end
 
 def combine_paths(selected_station, station_names) do
  station_names
  |> Enum.map(fn x -> %{"origin" => selected_station, "destination" =>  x}  end)
 end

 def place_id(address) do
   {:ok, address_full} = JSON.decode address
   {:ok, id} = address_full
   |> Map.get("results")
   |> Enum.at(0)
   |> Map.fetch("place_id")
   id
 end

 def geocode(station) do
  name = Enum.at(station, 1)
  code = Enum.at(station, 2)
  url_name = String.replace(name, " ", "%20")
#15
  url = "https://maps.googleapis.com/maps/api/geocode/json?address=#{url_name}(#{code},India)"
  case HTTPoison.get(url) do
   {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
     {:ok, address} = body |> String.replace("\n", "") |> JSON.decode
     save_address(name, address)
     name   
   _  -> 
     ""
  end
 end
#26
 def save_address(name, address) do 
  {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
  {:ok, address_string} = JSON.encode(address)
  Mariaex.Connection.query(p, "UPDATE StationInfo set address = '#{address_string}' WHERE Station_Name = '#{name}'")
 end

 def faulter do
   Mmtp.main()
   :timer.sleep(3000)
   faulter
 end
  
end

main_argv = Enum.at(System.argv, 0)
if main_argv == "search" do
  Mmtp.search(Enum.at(System.argv, 1))
else
  IO.puts "Filling in addresses"
  Mmtp.faulter
end
