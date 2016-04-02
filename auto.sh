echo "================== Housekeeping ==========================="
mix run lib/sequence.exs
echo "=================== OMT scripts running (Will take a while, please wait)========================="
mix run lib/omt_run.exs
echo "1. Distance Matrix for Cities containing Airports"
mix run lib/omt.exs lone_stations
echo "2. Distance Matrix Lone Stations "
mix run lib/omt.exs omt_gen
echo "3. OMT DB Persisted "
mix run lib/omt.exs omt
echo "4. OMT.txt Generated"
echo "=================== Connection scripts running ========================"
rm data/connections.txt
mix run lib/connections.exs
echo "1. Connections for trains created"
mix run lib/save_connections.exs
echo "2. Connections for trains DB Persisted"
mix run lib/bus.exs
echo "3. Connections for bus created and DB Persisted"
mix run lib/airline.exs
echo "4. Connections for airlines created and DB persisted"
