defmodule Omt do

def omt_gen do
 {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
 {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT results FROM station_details WHERE results IS NOT NULL")
 stations = Map.get(result, :rows)
 results = stations
 |> Enum.map(fn(x) -> get_matrix(x) end)
 |> List.flatten
 |> Enum.uniq
 |> Enum.filter(fn x -> !String.contains?("#{inspect x}", "NOT_FOUND") && !String.contains?("#{inspect x}", "ZERO") end)
 |> Enum.map(fn x -> prepare_line(x) end)
 |> Enum.uniq
 |> Enum.map(fn x -> save(x, p) end)
end

def omt_txt_gen do
 {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
 content = 0..16280
 |> Enum.chunk(500, 500, []) 
 |> Enum.map(fn x -> get_omt_rows(x, p) end) 
 |> Enum.join("\n")
 File.write("data/omt.txt", content)
end
 
def get_omt_rows(x, p) do
 ids = x |> Enum.join(", ")
 {:ok,result} = Mariaex.Connection.query(p, "SELECT src, dest, duration FROM omt WHERE id in (#{ids})")
 result 
 |> Map.get(:rows)
 |> Enum.map(fn x -> Enum.join(x, " ") end) 
 |> Enum.join("\n")
end
@p Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
def get_station_seq(station_id) do
  {:ok, p} = @p
  {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM seq WHERE station_id = #{String.to_integer(station_id)}")
   Map.get(result, :rows) |> Enum.at(0) |> Enum.at(0)
end

def save([src, dest, duration, distance], p) do
 src = get_station_seq(src)
 dest = get_station_seq(dest)
 {:ok,result} = Mariaex.Connection.query(p, "INSERT INTO omt (src, dest, distance, duration) VALUES ('#{src}', '#{dest}', '#{distance}', '#{duration}')")
 "#{src} #{dest} #{duration}"
end

def get_matrix(x) do
 json_x = get_json(x)
end

def get_json(x) do
 {:ok, data} = x
 |> Enum.at(0)
 |> JSON.decode
 data
end


def prepare_line(data) do
   origin = data["origin"]
   destination = data["destination"]
    seconds = 0
   hours = Regex.scan(~r/[0-9]+ hour/, "#{data["data"]["duration"]["text"]}") |> Enum.at(0)
   if hours != nil do
     hours = hours |> Enum.at(0) |> String.split(" ") |> Enum.at(0)
     seconds = seconds + (String.to_integer(hours))*3600
   end
   mins = Regex.scan(~r/[0-9]+ min/, "#{data["data"]["duration"]["text"]}") |> Enum.at(0)
   if mins != nil do
     mins = mins |> Enum.at(0) |> String.split(" ") |> Enum.at(0)
     seconds = seconds + (String.to_integer(mins)*60)
   end
   #"#{origin} #{destination} #{seconds}"
   distance = data["data"]["distance"]["text"] |> String.split(" ") |> Enum.at(0)
   [origin, destination, seconds, distance]
end

def lone_stations do
 {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
 {:ok,result} = Mariaex.Connection.query(p, "SELECT COUNT(*) FROM station_details  WHERE results IS NULL AND mode ='train'")
 count = result |> Map.get(:rows) |> Enum.at(0) |> Enum.at(0)
 per_page = 300
 total_pages = div(count,per_page) + 1
 #total_pages = 1
 (0..total_pages-1)
 |> Enum.map(fn x -> get_batch(per_page, x) end)
 |> Enum.map(fn x -> get_bus_stop(p, x) end)
end

def get_batch(per_page, page_number) do
 limit = per_page
 offset = page_number * limit
 {:ok, p} =  Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
 {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM station_details WHERE results IS NULL AND mode = 'train' LIMIT #{limit} OFFSET #{offset}")
 result |> Map.get(:rows) 
end

def get_bus_stop(p, x) do
 z = x 
 |> Enum.filter(fn y ->!String.contains?("#{inspect y}", "error") && !(String.contains?("#{inspect y}", "ZERO")) end)
 |> Enum.map(fn y -> query_bus_stop(p, y) end)
 |> Enum.filter(fn y -> !String.contains?("#{inspect y}", "ZERO") end)
 |> Enum.filter(fn y ->!String.contains?("#{inspect y |> Enum.at(1)}", "error") && String.contains?("#{inspect y |> Enum.at(1)}", "results") end)
 |> Enum.map(fn z -> get_data(z, p) end)
end

def query_bus_stop(p, station) do
 [id, name, "train", results, nil ] = station
 {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM station_details WHERE station_name = '#{name}' AND mode = 'bus'")
 bus_stop = result |> Map.get(:rows) |> Enum.at(0)
 [station, bus_stop]
end

def get_data([station, bus_stop], p) do

  parsed_station = [Enum.at(station, 0), Enum.at(station, 3)] 
  parsed_station = [Enum.at(parsed_station, 0), "place_id:#{place_id(Enum.at(parsed_station,1))}"] 
  parsed_bus = [Enum.at(bus_stop, 0), Enum.at(bus_stop, 3)]
  parsed_bus = [Enum.at(parsed_bus, 0), "place_id:#{place_id(Enum.at(parsed_bus,1))}"]
  matrix = get_distance_matrix([parsed_station, parsed_bus])
  {:ok, matrix} = matrix |> List.flatten |> JSON.encode
  station_id = parsed_station |> Enum.at(0)
  bus_id = parsed_bus |> Enum.at(0)
  IO.inspect bus_id
  Mariaex.Connection.query(p, "UPDATE station_details set results = '#{matrix}' WHERE id IN (#{station_id}, #{bus_id})")  
  matrix
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
  {matrix, count} = address_params
  |> DistanceMatrixApi.get_distances
  |> Map.fetch!("rows")
  |> Enum.map_reduce(0, fn x, i -> {attach_data(origins, destinations, Map.fetch!(x, "elements"), i), i + 1} end)
  matrix
end



def place_id(address) do
  if address != nil && address != "" do
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


end


#IO.inspect Omt.omt_txt_gen
#Omt.lone_stations
#Omt.omt_gen
main_argv = Enum.at(System.argv, 0)
if main_argv == "omt_gen" do
  Omt.omt_gen
else 
  if main_argv == "lone_stations" do
    Omt.lone_stations
  else
    Omt.omt_txt_gen
  end
end

