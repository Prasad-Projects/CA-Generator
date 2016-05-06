defmodule Mmtp do
  @api_key System.get_env("google_api_key") 
  def main do
    {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
    {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM StationInfo WHERE address IS NULL OR address LIKE \"%error%\"")
    stations = Map.get(result, :rows)
    results = stations
    |> Enum.slice(0, 30)
    |> Enum.map(fn(x) -> x |> geocode end) 
  end

  def bus_main do 
    {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
    {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT dest_name, stop_address from mytable WHERE stop_address IS NULL or stop_address LIKE \"%error%\"")
    bus_stops = Map.get(result, :rows)
    results = bus_stops
    |> Enum.slice(0, 30)
    |> Enum.map(fn(x) -> x |> geocode_bus end)
    ""
  end

  def geocode_bus(stop) do
    name = Enum.at(stop, 0)
    url_name = String.replace(name, " ", "%20")
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=#{url_name}%20bus%20station,India&key=#{@api_key}"
    case HTTPoison.get(url) do
      {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
        {:ok, address} = body |> String.replace("\n", "") |> JSON.decode
        save_bus_address(name, address)
        name
      _  ->
        ""
    end
  end

  def save_bus_address(name, address) do
    {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
    {:ok, address_string} = JSON.encode(address)
    Mariaex.Connection.query(p, "UPDATE mytable set stop_address = '#{address_string}' WHERE dest_name = '#{name}'")
  end

  def search(city) do
    {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
    {:ok, result} = Mariaex.Connection.query(p, "SELECT * FROM station_details WHERE address LIKE \"%#{city}%\"")   

    search_result_temp = result
    |> Map.get(:rows)
    |> Enum.at(0)
    |> Enum.at(4)
    if(search_result_temp && search_result_temp != "" && search_result_temp != "[]") do
      search_results = search_result_temp
    else
      station_ids = result
      |> Map.get(:rows)
      |> Enum.map(fn x -> Enum.at(x, 0)  end)

      stations = result
      |> Map.get(:rows)
      |> Enum.map(fn x -> [Enum.at(x, 0), Enum.at(x, 3)]  end)
      |> Enum.map(fn x -> [Enum.at(x, 0), "place_id:#{place_id(Enum.at(x,1))}"] end)
      matrix = get_distance_matrix(stations)    
      {:ok, matrix} = matrix |> List.flatten |> JSON.encode
      persist_search(station_ids, "#{matrix}")
    end
  end

  def extract(x) do
  end

  def get_distance_matrix(stations) do
    stations
    |> Enum.chunk(10, 10, [])
    |> Enum.map(fn selected -> add_to_matrix(selected, stations) end)
  end

  def add_to_matrix(origins, stations) do
    matrix = stations
    |> Enum.chunk(10, 10, [])
    |> Enum.map(fn destinations ->  get_matrix(origins, destinations, stations) end)
    |> Enum.map(fn x -> x end)
  end

  def attach_data(origins, destinations, matrix, i) do
    origin = origins |> Enum.at(i)
    {matrix_attached, count} = matrix 
    |> Enum.map_reduce(0, fn(x, i) -> { attach_data_single("#{origin |> Enum.at(0)}", destinations |> Enum.at(i), x)  ,  i+1 } end) 	 	
    matrix_attached |> List.flatten
  end

  def attach_data_single(origin, destination, data) do
    %{origin: "#{origin}",destination: "#{destination |> Enum.at(0)}", data: data}
  end

  def get_matrix(origins, destinations, stations) do 
    address_params = %{origins: Enum.map(origins, fn x -> Enum.at(x, 1) end), destinations: Enum.map(destinations, fn x -> Enum.at(x, 1) end)}
    x = address_params
    |> DistanceMatrixApi.get_distances
    {matrix, count} = x    
    |> Map.fetch!("rows") 
    |> Enum.map_reduce(0, fn x, i -> {attach_data(origins, destinations, Map.fetch!(x, "elements"), i), i + 1} end)
    matrix
  end

  def persist_search(station_names, search_results) do
    {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
    station_names
    |> Enum.map(fn x ->  Mariaex.Connection.query(p, "UPDATE station_details set results = '#{search_results}' WHERE id = #{x}") end)
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
    if address != nil do
      {:ok, address_full} = JSON.decode address
      {:ok, id} = address_full
      |> Map.get("results")
      |> Enum.at(0)
      |> Map.fetch("place_id")
      id
    else
      ""
    end
  end

  def geocode(station) do
    name = Enum.at(station, 1)
    code = Enum.at(station, 2)
    url_name = String.replace(name, " ", "%20")
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=#{url_name}%20#{code},India&key=#{@api_key}"
    case HTTPoison.get(url) do
      {:ok, %HTTPoison.Response{status_code: 200, body: body}} ->
        {:ok, address} = body |> String.replace("\n", "") |> JSON.decode
        save_address(name, address)
        name   
      _  -> 
        ""
    end
  end
  def save_address(name, address) do 
    {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
    {:ok, address_string} = JSON.encode(address)
    Mariaex.Connection.query(p, "UPDATE StationInfo set address = '#{address_string}' WHERE Station_Name = '#{name}'")
  end

  def faulter(i) when i > 0 do
    Mmtp.bus_main()
    :timer.sleep(1000)
    faulter(i-1)
  end

  def faulter() do
    IO.puts "done"
  end

end

main_argv = Enum.at(System.argv, 0)
if main_argv == "search" do
  Mmtp.search(Enum.at(System.argv, 1))
else
  IO.puts "Filling in addresses"
  Mmtp.faulter(2000)
end
