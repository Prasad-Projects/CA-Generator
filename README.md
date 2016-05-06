# Mmtp Data generation and transformation

Clone and run `sh auto.sh`


Skeleton db is present in data/mmtp_skeleton.sql.

**The db does not contain geocoded address and distance matrices**

To setup google api, visit console and set whitelisted IPs and add environment variable "google_api_key"

To run selective commands check `auto.sh` and comment out unnecessary commands.
To run delta update for DCT run 

```
mix run lib/dct.exs update <train_id>
```

For generating stations list text:

```
mix run lib/stations_gen.exs
```

Metadata parse from irctc files are there in the metadata folder under apps.



