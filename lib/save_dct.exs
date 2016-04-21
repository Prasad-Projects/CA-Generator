defmodule SaveConnections do 

@p Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
def save do
 File.stream!("data/dct.txt", [:utf8, :read_ahead], :line)
 |> Enum.chunk(1000, 1000, [])
 |> Enum.each(fn x -> save_connection(x) end)
 :timer.sleep(:infinity)
end

def save_connection(x) do
 x
 |> Enum.map(fn x -> parse_row(String.split(x, " ")) end)
 |> Enum.filter(fn x -> x != nil end)
 |> Enum.join(", ")
 |> insert_values
end

def insert_values(x) do
 {:ok, p} = @p
 if x != "" do
  IO.inspect {:ok,result} = Mariaex.Connection.query(p, "INSERT INTO dct (vehicle_id, src, dest, dep_t, arr_t, mode) VALUES #{x}", [], timeout: :infinity)
 end
end

def parse_row([vehicle_id, src, dest, dep_t, arr_t, mode]) do
 "('#{vehicle_id}', '#{src}', '#{dest}', '#{dep_t}', '#{arr_t}', 'train')"
end

def parse_row(x) do
 nil
end

end
SaveConnections.save
