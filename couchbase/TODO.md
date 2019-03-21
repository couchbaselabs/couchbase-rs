# Overall
    - turn all expect/unwrap into handleable errors
    - add logging infra

# Instance
    - handle shutdown (signal thread shutdown, watch handle)
     .. make sure when instance is destroyed the cookie is too

# KV
    - implement get with result and decoding using serde
    - add error handling for get
    - implement insert, upsert and replace, encoding with serde
    - add error handling for those 3 mutation options
    - implement options for get and the 3 mutation options