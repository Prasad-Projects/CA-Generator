defmodule StationsGen do

def main do
 {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp_123", database: "mmtp")
 {:ok,seq} = Mariaex.Connection.query(p, "SELECT * from seq")
 seq = seq |> Map.fetch!(:rows) 
 seq
 |> Enum.map(fn x -> get_details(x) end)
 |> Enum.join("\n")
 |> write_to_file
 
end

def write_to_file(x) do
 {:ok, file} = File.open "stations.txt", [:write]
 IO.binwrite file, x
end

@p  Mariaex.Connection.start_link(username: "root", password: "mmtp_123", database: "mmtp")
def get_details([seq, station_id]) do 
 {:ok, p} = @p
 {:ok,result} = Mariaex.Connection.query(p, "SELECT id, station_name, mode FROM station_details WHERE id = #{station_id}")
 result = result |> Map.fetch!(:rows) |> Enum.at(0)
 [_, name, mode] = result
 IO.puts seq
 "#{seq} #{name} #{mode}" 
end

def query(x) do
 {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp_123", database: "mmtp")
 {:ok,result} = Mariaex.Connection.query(p, "SELECT id, station_name, mode FROM station_details WHERE id IN (#{x})")
 
 result = result 
 |> Map.fetch!(:rows)
 IO.inspect result
 result
end

end
IO.inspect StationsGen.main
