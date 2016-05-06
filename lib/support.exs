defmodule Support do
	
  def create_omt do
    {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp_backup_2")
    stations = File.read!("data/stations.txt")  
    stations = stations
    |> String.split("\n")
    |> Enum.map(fn x -> get_station_data(x) end)
  
    matrix_data_coll = fetch_matrix_data_collection(p)
    |> List.flatten
    
    data = matrix_data_coll
    |> Enum.uniq
    |> Enum.filter(fn x -> entry_exists?(x, stations) end)
    |> Enum.map(fn x -> prepare_line(x, stations) end )
    |> Enum.join("\n")
   
    File.write "data/OMT.txt", data 
    data  
  end

  def entry_exists?(data, stations) do
   origin = stations
   |> Enum.any?(fn x -> Enum.at(x, 1) == data["origin"] end)
   destination = stations
   |> Enum.any?(fn x -> Enum.at(x, 1) == data["destination"] end)
   origin and destination
  end

  def prepare_line(data, stations) do
   origin = stations
   |> Enum.find(fn x -> Enum.at(x, 1) == data["origin"] end)
   |> Enum.at(0)

   destination = stations
   |> Enum.find(fn x -> Enum.at(x, 1) == data["destination"] end)
   |> Enum.at(0)

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
   "#{origin} #{destination} #{seconds}"
  end


  def fetch_matrix_data_collection(p) do
    {:ok, result} = Mariaex.Connection.query(p, "SELECT DISTINCT results FROM StationInfo WHERE results IS NOT NULL")
    result
    |> Map.fetch!(:rows)  
    |> Enum.map(fn x -> get_json(x)  end)
  end

  def get_json(x) do
    {:ok, data} = x
    |> Enum.at(0)
    |> JSON.decode
    data
  end

  def get_station_data(line) do
    station_name = line
    |> String.split(" ")
    |> Enum.slice(1..-1)
    |> Enum.join(" ")
    id = line
    |> String.split(" ")
    |> Enum.at(0) 
    [id, station_name]
  end
end

Support.create_omt()
