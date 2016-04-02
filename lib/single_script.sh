mix run lib/omt_run.exs
mix run lib/omt.exs
mix run lib/sequence.exs
mix run lib/connections.exs
mix run lib/save_connections.exs
mix run lib/buses.exs
mix run lib/airline.exs

Export connections as CSV SELECT vehicle_id, src, dest, dep_t, arr_t, MODE FROM connection sORDER BY CAST( dep_t AS UNSIGNED )
