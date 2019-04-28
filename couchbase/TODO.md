# Overall
    - remove all expect from code and turn it into errors
    - add logging infrastructure

# Instance
    - handle shutdown (signal thread shutdown, watch handle)
     .. make sure when instance is destroyed the cookie is too

# KV
    - implement options for get and the 3 mutation options
    - remaining commands plus options:
        - replica get
        - exists
        - touch
        - unlock
        - lookupIn
        - mutateIn
    - Binary collection
        - append
        - prepend
        - increment
        - decrement

# Querying
    - add all query options for
        * n1ql
        * analytics
    - add views plus options
    - add fts plus options

# Testing
    - set up and run basic testing infra with mock and real node