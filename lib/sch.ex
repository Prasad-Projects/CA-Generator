defmodule Sch do 

 @p Mariaex.Connection.start_link(username: "sushruth456", password: "sushruth456", database: "mmtp")
 def correct_timings do 
   {:ok, p} = @p
   {:ok,result} = Mariaex.Connection.query(p, "SELECT DISTINCT Train_Num FROM TrainschdInfo", [], timeout: 500000)
   result = result
   |> Map.get(:rows)
   #|> Enum.slice(0,2)
   |> Enum.chunk(100, 100)
   |> Enum.map(&(Task.async fn -> get_sch_many(&1) end))
   |> Task.yield_many(1000000)
   result
 end  
 
 def get_sch_many(sch) do
   Enum.map(sch, &get_sch/1)
 end

 def get_sch([train_id]) do
   {:ok, p} =  @p
   {:ok,result} = Mariaex.Connection.query(p, "SELECT * FROM TrainschdInfo WHERE Train_Num = #{train_id}", [], timeout: 500000)
   result = result
   |> Map.get(:rows)
   |> correct_train(0, "00:00", [])
   |> Enum.map(&prepare_update/1)
   |> Enum.join(",")
   |> save
   |> IO.inspect
 end
 
 def save(values) do
   query = "INSERT INTO TrainschdInfo (id, arr_time, dep_time) VALUES #{values} ON DUPLICATE KEY UPDATE arr_time=VALUES(arr_time),dep_time=VALUES(dep_time)"
   {:ok, p} = @p
   {:ok,result} = Mariaex.Connection.query(p, query)
 end
 
 def prepare_update(entry) do
   [id, train_num, code, route, arr_t, dep_t] = entry
   "(#{id}, '#{arr_t}', '#{dep_t}')"   
 end

 def correct_train([], _, _, sch) do
   sch
 end

 def correct_train([entry | tail], day, p_dep, sch) do
   [id, train_num, code, route, arr_t, dep_t] = entry
   p_dep_t = time_to_seconds(p_dep)
   arr_t_t = time_to_seconds(arr_t)
   dep_t_t = time_to_seconds(dep_t)
   case {arr_t, dep_t, (p_dep_t && arr_t_t)} do
    {_, _, nil} -> 
      correct_train(tail, day, dep_t, sch)
    {"Source", _, _} ->  
      correct_train(tail, day, dep_t, sch)
    {_, "Destination", _} -> 
      if (arr_t_t < p_dep_t) do
        new_entry = [id, train_num, code, route, s_to_t(arr_t_t + ((day+1) * 86400)), dep_t]
        correct_train(tail, day+1, dep_t, sch ++ [new_entry])
      else
        new_entry = [id, train_num, code, route, s_to_t(arr_t_t + ((day) * 86400)), dep_t]
        correct_train(tail, day, dep_t, sch ++ [new_entry])
      end
    {_, _, _} -> 
      if (arr_t_t < p_dep_t) do
        new_entry = [id, train_num, code, route, s_to_t(arr_t_t + ((day + 1) * 86400)), s_to_t(dep_t_t + ((day + 1) * 86400))]
        correct_train(tail, day+1, dep_t, sch ++ [new_entry])
      else
        new_entry = [id, train_num, code, route, s_to_t(arr_t_t + ((day) * 86400)), s_to_t(dep_t_t + ((day) * 86400))]
        correct_train(tail, day, dep_t, sch ++ [new_entry])
      end
   end
 end

 def s_to_t(seconds) do
  h = div(seconds, 3600)  
  min = div(rem(seconds, 3600), 60)
  "#{h}:#{min}"
 end

 def time_to_seconds(time) do
  case time do
   "Source" ->
     nil
   "Destination" ->
     nil
   "<FONT COLOR = red>Slip So" ->
     nil
   _ ->
   [hour, minute] = String.split(time, ":")
    hour_s = String.to_integer(hour) * 3600
    minute_s = String.to_integer(minute) * 60
    hour_s + minute_s
  end
 end
end

#IO.inspect Sch.correct_timings
# Sch.correct_timings
