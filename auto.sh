spinner()
{
    local pid=$!
    local delay=0.75
    local spinstr='...'
    echo "Running task "
    while [ "$(ps a | awk '{print $1}' | grep $pid)" ]; do
        local temp=${spinstr?}
        printf "%s  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b"
    done
    printf "    \b\b\b\b"
}

echo "================== Housekeeping ==========================="
echo "0. Sequence generating"
((mix run lib/sequence.exs >/dev/null 2>&1) & spinner) & spinner
echo "=================== OMT scripts running (Will take a while, please wait)========================="
echo "1. Distance Matrix for Cities containing Airports"
#Requires google api
(mix run lib/omt_run.exs >/dev/null 2>&1) & spinner
echo "2. Distance Matrix Lone Stations "
#Requires google api
(mix run lib/omt.exs lone_stations >/dev/null 2>&1) & spinner
rm data/omt.txt >/dev/null 2>&1
echo "3. OMT DB persisting "
(mix run lib/omt.exs omt_gen >/dev/null 2>&1) & spinner
echo "4. OMT.txt generating"
(mix run lib/omt.exs omt >/dev/null 2>&1) & spinner
echo "=================== Connection scripts running ========================"
rm data/connections.txt >/dev/null 2>&1
echo "1. Connections for trains creating"
(mix run lib/connections.exs >/dev/null 2>&1) & spinner
echo "2. Connections for trains DB persisting"
(mix run lib/save_connections.exs >/dev/null 2>&1) & spinner
echo "3. Connections for bus created and DB Persisted"
(mix run lib/bus.exs >/dev/null 2>&1) & spinner
echo "4. Connections for airlines created and DB persisted"
(mix run lib/airline.exs >/dev/null 2>&1) & spinner
rm data/dct.txt >/dev/null 2>&1
rm /var/lib/mysql/mmtp/connections.txt
echo "4. Generating final connection data text for 7 days"
echo "use mmtp;select vehicle_id,src,dest,dep_t, arr_t, mode FROM connections order by cast(dep_t as unsigned) INTO OUTFILE 'connections.txt' FIELDS TERMINATED BY ' ' ENCLOSED BY '' LINES TERMINATED BY '\n';" | mysql -u root -pmmtp_123
cp /var/lib/mysql/mmtp/connections.txt data
echo "5. Generating DCT for 7 days"
(mix run lib/dct.exs >/dev/null 2>&1) & spinner
rm /var/lib/mysql/mmtp/dct.txt
cp data/dct.txt /var/lib/mysql/mmtp
echo "use mmtp; LOAD DATA INFILE 'dct.txt' INTO TABLE dct FIELDS TERMINATED BY ' ' ENCLOSED BY '' LINES TERMINATED BY '\n';" | mysql -u root -pmmtp_123
rm data/metadata.txt > /dev/null
echo "6. Generating metadata text for trains"
(mix run lib/meta_text.exs >/dev/null 2>&1) & spinner
rm /var/lib/mysql/mmtp/metadata.txt
cp data/metadata.txt /var/lib/mysql/mmtp
echo "7. Persisting metadata for trains"
echo "use mmtp; LOAD DATA INFILE 'metadata.txt' INTO TABLE MetaData FIELDS TERMINATED BY ' ' ENCLOSED BY '' LINES TERMINATED BY '\n';" | mysql -u root -pmmtp_123
echo "8. Persisting metadata for airlines"
(mix run lib/airline_meta.exs >/dev/null 2>&1) & spinner
echo "9. Persisting metadata for buses"
(mix run lib/bus_text.exs >/dev/null 2>&1) & spinner
