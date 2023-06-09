import redis
import time
import random


def update_redis_10Secs():
    # Connect to your Redis instance
    r = redis.Redis(host='localhost', port=6379, db=0)

    while True:
        # Start a pipeline
        pipe = r.pipeline()

        # Keys of interest
        keys = ["XPD/USD", "XAU/USD", "XAG/USD", "XPT/USD"]

        for key in keys:
            # Fetch current values
            values = r.hgetall(key)
            ask = float(values[b'ask'])
            bid = float(values[b'bid'])

            # Generate a random percentage change between -2% and 2%
            change = random.uniform(-0.02, 0.02)

            # Apply the change to ask and bid prices
            ask += ask * change
            bid += bid * change

            # Get the current timestamp
            timestamp = int(time.time())

            # Update the values in redis
            pipe.hset(key, mapping={'ask': ask, 'bid': bid, 'timestamp': timestamp})

        # Execute all commands in the pipeline
        pipe.execute()

        # Update the global timestamp
        r.set("timestamp", int(time.time()))

        # Wait for 10 seconds before the next update
        time.sleep(10)


def add_padding_per_metal_to_redis():
    # Connect to your Redis instance
    r = redis.Redis(host='localhost', port=6379, db=0)

    # Your data
    data = {
        "retail_padding": {
            "gold": "10",
            "silver": "0.25",
            "platinum": "15",
            "palladium": "30"
        },
        "wholesale_padding": {
            "gold": "1.5",
            "silver": "0.05",
            "platinum": "3",
            "palladium": "17.5"
        }
    }

    # Start a pipeline
    pipe = r.pipeline()

    # Add each item to the pipeline
    for key, value in data.items():
        pipe.hset(key, mapping=value)

    # Execute all commands in the pipeline
    pipe.execute()


def add_metalpricing_to_redis():

    r = redis.Redis(host='localhost', port=6379, db=0)
    # Your JSON data
    data = {
        "XPD/USD": {"ask": 1424.922, "bid": 1418.228, "timestamp": 1686032434},
        "XAU/USD": {"ask": 1960.15, "bid": 1959.97, "timestamp": 1686032440},
        "XAG/USD": {"ask": 23.5914, "bid": 23.5836, "timestamp": 1686032440},
        "XPT/USD": {"ask": 1036.716, "bid": 1034.394, "timestamp": 1686032437},
        "timestamp": 1686032440
    }

    # Start a pipeline
    pipe = r.pipeline()

    # Add each item to the pipeline
    for key, value in data.items():
        if isinstance(value, dict):
            pipe.hmset(key, value)
        else:
            pipe.set(key, value)

    # Execute all commands in the pipeline
    pipe.execute()


def add_padding_per_metal():
    # Connect to your Redis instance
    r = redis.Redis(host='localhost', port=6379, db=0)

    # Define metal to currency mapping
    metal_to_currency = {
        "gold": "XAU/USD",
        "silver": "XAG/USD",
        "platinum": "XPT/USD",
        "palladium": "XPD/USD"
    }

    # Start a pipeline
    pipe = r.pipeline()

    # Get padding values
    retail_padding = r.hgetall("retail_padding")
    wholesale_padding = r.hgetall("wholesale_padding")   

    # Add padding to each metal price
    for metal, currency in metal_to_currency.items():
        # Get current ask and bid
        values = r.hgetall(currency)
        ask = float(values[b'ask'])
        bid = float(values[b'bid'])

        # Get padding values for the metal
        retail_pad = float(retail_padding[metal.encode()])
        wholesale_pad = float(wholesale_padding[metal.encode()])

        # Add padding to ask and bid
        retail_padded_ask = ask + retail_pad
        retial_padded_bid = bid + retail_pad          
        wholesale_padded_ask = ask + wholesale_pad
        wholesale_padded_bid = bid + wholesale_pad

        # Store padded prices in new hash
        retail_padded_currency = "retail:" + metal + ":padding"
        pipe.hset(retail_padded_currency, 
                  mapping={'ask': retail_padded_ask,
                           'bid': retial_padded_bid})
        wholesale_padded_currency = "wholesale:" + metal + ":padding"
        pipe.hset(wholesale_padded_currency, 
                  mapping={'ask': wholesale_padded_ask,
                           'bid': wholesale_padded_bid})

    # Execute all commands in the pipeline
    pipe.execute()


# Run the function
add_metalpricing_to_redis()
add_padding_per_metal_to_redis()
add_padding_per_metal()

# update_redis_10Secs()
