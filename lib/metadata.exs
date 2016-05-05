defmodule Metadata do
    @p Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")

    def update_table() do
        IO.puts "updating schedule table"
        {:ok, p} = @p
        {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT Train_Num FROM TrainschdInfo", [], timeout: 500000)
        result = result
        |> Map.get(:rows)

        l = length(result)
        result
        |> Enum.slice(l-461, l)
        |> Enum.chunk(100, 100, []) 
        |> Enum.map(fn x -> Task.async(fn -> update_train(x) end) end)
        |> Enum.map(fn x -> Task.await(x, 9999999) end)
    end

    def update_train(x) do
        x
        |> Enum.map(&([&1, fetch_schedule(&1)]))
        |> Enum.filter(fn [_, x] -> x != nil end)
        |> Enum.map(fn [[train_id], x] -> [fetch_train_data(train_id), x] end)
        #|> Enum.map(fn [[train_id], x] -> Task.async(fn -> [fetch_train_data(train_id), x] end) end)
        #|> Enum.map(fn x -> Task.await(x, 9999999) end)
        |> Enum.filter(fn [sch, x] -> length(sch) == length(x) end)
        |> Enum.map(fn x -> update_table(x) end)
        #|> Enum.map(fn x -> Task.async(update_table(x)) end)
        #|> Enum.map(fn x -> Task.await(x, 9999999) end)
        |> IO.inspect
    end

    def update_table([sch, x]) do
        train_id = sch |> Enum.at(0) |> Enum.at(1)
        IO.puts "Updating table for #{train_id}"
        data = Enum.zip(sch, x)
        data
        |> Enum.map(fn {[id | _], distance} -> update_row(id, distance) end)
        data
    end

    def update_row(id, distance) do
        IO.puts "#{id} -- #{distance}"
        {:ok, p} =  @p
        {:ok,result} = Mariaex.Connection.query(p, "UPDATE TrainschdInfo SET distance = #{distance} WHERE id = #{id}", [], timeout: 500000)
    end

    def fetch_train_data(train_id) do
        IO.puts "Fetching data for #{train_id}"
        {:ok, p} =  @p
        {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainschdInfo WHERE Train_Num = #{train_id}", [], timeout: 500000)
        result |> Map.get(:rows)
    end

    def fetch_schedule([train_id]) do
        data = File.read("data/html/#{train_id}.html")
        html = case data do
            {:error, _} -> 
                file = File.read("data/html/0#{train_id}.html")
                case file do
                    {:error, _} -> nil
                    {:ok, src} -> src
                end
            {:ok, src} -> src
        end
        case html do
            nil -> 
                nil
            _ ->
                d = html
                |> String.replace("\n", "")
                |> String.replace("\r", "")
                |> String.split("<TH width=\"20%\">Remark</TH>")
                |> Enum.at(1)

                distances = case d do
                    nil -> 
                        nil
                    _ -> 
                        d
                        |> String.split("</TR>")
                        |> Enum.filter(&(String.contains?(&1, "<TD>")))
                        |> Enum.map(&(String.split(&1, "</TD>") |> Enum.at(7) |> String.split("<TD>") |> Enum.at(1)))
                    end

                distances
        end
    end
end

#Metadata.fetch_schedule("02779")
Metadata.update_table