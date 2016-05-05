defmodule MetaText do
  @p Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
  def main do
    IO.puts "Running Metadata text for all combinations of train journeys ........"
    {:ok, p} = Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
    {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT Train_Num FROM TrainschdInfoMeta", [], timeout: 9999999)
    station_ids = fetch_station_ids
    :ets.new(:station_ids, [:public, :named_table])
    station_ids
    |> Enum.map(fn {code, id} -> :ets.insert(:station_ids, {String.strip(code), id}) end)

    fares = File.read!("data/Fares.csv")
    |> String.replace("\r", "")
    |> String.split("\n")
    |> Enum.map(&(String.split(&1, ",") |> Enum.map(fn x-> String.to_integer(x) end)))

    result = result
    |> Map.get(:rows)
    |> Enum.map(fn [train_id] -> Task.async(fn -> create_meta(train_id, fares) end) end)
    |> Enum.map(fn x -> Task.await(x, 99999999) end)
    IO.puts "-------------------- Generated Metadata text --------------------"
  end

  def create_meta(train_id, fares) do
    {:ok, p} = @p
    {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainschdInfoMeta WHERE Train_Num = #{train_id}", [], timeout: 99999999)
    result = result
    |> Map.get(:rows)
    |> Enum.map(fn x -> format_entry(x) end)
    combination(result)
    |> Enum.map(fn x-> make_connection(x, fares) end) 
    |> Enum.filter(fn x -> x != nil end)
    IO.inspect train_id
  end

  def write(metadata) do
    line = metadata
    |> Enum.join(" ")
    File.write("data/metadata.txt", line <> "\n", [:append])
  end

  def make_connection([src, dest], fares) do
    [_, train_id, src_id, _,  _ , _, distance_src] =  src
    [_, _, dest_id, _, _, _, distance_dest] = dest
    distance = distance_dest - distance_src
    final_fare = get_fares(distance, fares)
    if (src_id != nil && dest_id != nil && final_fare != nil) do
      write([src_id, dest_id, train_id, "train", distance, final_fare, 0,  "true", "true"])
    else
      nil
    end
  end

  def get_fares(distance, fares) do
    # Distance Slabs,1AC*,2AC*,FC,3AC,CC,SL,II
    slab = find_slab(distance, fares)
    case slab do
      nil -> nil
      _ -> slab + 1
    end
  end

  def find_slab(distance, fares) do
    slab = fares
    |>Enum.find_index(fn [slab_start, slab_end | _] -> slab_start <= distance && slab_end > distance end)
  end

  def format_entry(entry) do
    [id, train_id, station, route, arr, dep, distance] = entry
    #station_id = get_station_id(station)
    {_, station_id} = :ets.lookup(:station_ids, String.strip(station)) |> Enum.at(0)
    [id, train_id, station_id, route, arr, dep, distance]
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
end

MetaText.main