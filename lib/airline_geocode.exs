defmodule AirlineGeocode do
  @api_key System.get_env("google_api_key")
  def save_airline_data(name, address) do
    {:ok, p} =  Mariaex.Connection.start_link(username: "root", password: "mmtp123", database: "mmtp")
    {:ok, address_string} = JSON.encode(address)
    Mariaex.Connection.query(p, "UPDATE airline set stop_address = '#{address_string}' WHERE src = '#{name}'")
  end

  def geocode_airport(name) do
    url_name = String.replace(name, " ", "%20")
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=#{url_name}%20Airport,India&key=#{@api_key}"
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
