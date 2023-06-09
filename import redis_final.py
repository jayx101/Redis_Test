import redis
import time
start_time = time.time()

# Connect to your Redis instance
r = redis.Redis(host='localhost', port=6379, db=0)

# Load your JSON data
data = {
    "retail": {
        "id":"10167",
        "metal":"gold",
        "weightoz":"1",
        "margin": "3",
        "marginpct": "0.2",
        "wirepct":"0.1",
        "cardpct":"0.5",
        "cryptopct":"0.01",
        "buypricemargin":"",
        "buypricepct":"",
        "lpmBuyPrice":"",
        "minimalprice":"",
        "calc_timestamp":"",
        "tierprices": [
            {
                "qty": 1,
                "margin": "2",
                "marginpct": "0.1",
                "price":"",
                "wireprice":"",
                "cardprice":"",
                "cryptoprice":""
            },
            {
                "qty": 20,
                "margin": "3",
                "marginpct": "0.2",
                "price":"",
                "wireprice":"",
                "cardprice":"",
                "cryptoprice":""
            },
            {
                "qty": 100,
                "margin": "4",
                "marginpct": "0.3",
                "price":"",
                "wireprice":"",
                "cardprice":"",
                "cryptoprice":""
            }
        ]
    },
    
    "wholesale": {
        "id":"10167",
        "metal":"gold",
        "weightoz":"1",
        "margin": "1.5",
        "marginpct": "0.1",
        "wirepct":"0.1",
        "cardpct":"0.5",
        "cryptopct":"0.01",
        "buypricemargin":"",
        "buypricepct":"",
        "lpmBuyPrice":"",
        "minimalprice":"1.2",
        "calc_timestamp":"",
        "tierprices": [
            {
                "qty": 1,
                "margin": "1.4",
                "marginpct": "0.09",
                "price":"",
                "wireprice":"",
                "cardprice":"",
                "cryptoprice":""
            },
            {
                "qty": 20,
                "margin": "1.3",
                "marginpct": "0.08",
                "price":"",
                "wireprice":"",
                "cardprice":"",
                "cryptoprice":""
            },
            {
                "qty": 100,
                "margin": "1.2",
                "marginpct": "0.07",
                "price":"",
                "wireprice":"",
                "cardprice":"",
                "cryptoprice":""
            }
        ]
    }
}

# Open a Redis pipeline
pipe = r.pipeline()

# Here we loop over the main keys (retail, wholesale)
for key, values in data.items():
    # We'll use the id and the key (retail, wholesale) to form unique Redis keys
    redis_key = f"{values['id']}:{key}"

    # Delete existing tier price keys
    pipe.delete(f"{redis_key}:tierprice_keys")

    # For each sub dictionary, we will store it as a hash in Redis
    for sub_key, sub_value in values.items():
        if sub_key != 'tierprices':
            # For regular keys, we just store them as they are
            pipe.hset(redis_key, sub_key, sub_value)

    # For 'tierprices', we will create a separate hash for each tier price
    tierprice_keys = []
    for i, tierprice in enumerate(values['tierprices']):
        tier_redis_key = f"{redis_key}:tierprice:{i}"
        tierprice_keys.append(tier_redis_key)
        for tier_key, tier_value in tierprice.items():
            pipe.hset(tier_redis_key, tier_key, tier_value)
    
    # Now we store the list of tier price keys
    pipe.rpush(f"{redis_key}:tierprice_keys", *tierprice_keys)

# Execute all commands in the pipeline
pipe.execute()


print("Process finished WRITE--- %s seconds ---" % (time.time() - start_time))




def get_product_data(product_type, product_id):
    # Connect to your Redis instance
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # Form the base key for the product
    base_key = f"{product_id}:{product_type}"

    # Open a Redis pipeline
    pipe = r.pipeline()

    # Get the base product data
    pipe.hgetall(base_key)

    # Get the list of tier price keys
    pipe.lrange(f"{base_key}:tierprice_keys", 0, -1)

    # Execute the commands to get the base product data and the list of tier price keys
    results = pipe.execute()

    # The first result is the base product data
    product_data = results[0]

    # The second result is the list of tier price keys
    tierprice_keys = results[1]

    # Open another Redis pipeline to get the tier prices
    pipe = r.pipeline()

    # Get all tier prices using the tier price keys
    for key in tierprice_keys:
        pipe.hgetall(key)

    # Execute the commands to get the tier prices
    tierprices_results = pipe.execute()

    # The results are the tier prices
    # Note: If a tier price doesn't exist, hgetall will return an empty dictionary
    tierprices = [result for result in tierprices_results if result]

    # Add the tier prices to the product data
    product_data['tierprices'] = tierprices

    return product_data


# Usage:
print(get_product_data('retail', '10167'))
print(get_product_data('wholesale', '10167'))
print("Process finished READ--- %s seconds ---" % (time.time() - start_time))



def update_pricing(product_type, product_id):
    # Connect to your Redis instance
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

    # Form the base key for the product
    base_key = f"{product_id}:{product_type}"

    # Fetch the product data
    product_data = r.hgetall(base_key)

    # Fetch the metal price
    metal_price = float(r.hget(f"{product_type}:{product_data['metal']}:padding", 'ask'))

    # Calculate lpmBuyPrice
    weightoz = float(product_data['weightoz'])
    margin_multiplier = 1 + float(product_data.get('marginpct', 0))
    lpmBuyPrice = weightoz * metal_price * margin_multiplier

    # Update lpmBuyPrice in Redis
    r.hset(base_key, 'lpmBuyPrice', lpmBuyPrice)

    # Fetch the keys to the tierprices
    tierprice_keys = r.lrange(f"{base_key}:tierprice_keys", 0, -1)

    # Process each tierprice
    for tierprice_key in tierprice_keys:
        # Fetch the tierprice data
        tierprice_data = r.hgetall(tierprice_key)

        # Calculate price
        margin_multiplier = 1 + float(tierprice_data.get('marginpct', 0))
        price = weightoz * metal_price * margin_multiplier

        # Calculate wireprice, cardprice, cryptoprice
        wireprice = price * float(product_data.get('wirepct', 0))
        cardprice = price * float(product_data.get('cardpct', 0))
        cryptoprice = price *  float(product_data.get('cryptopct', 0))


        # Update calculated values in Redis
        r.hmset(tierprice_key, {'price': price, 'wireprice': wireprice, 'cardprice': cardprice, 'cryptoprice': cryptoprice})

# Run the function for a product
update_pricing('retail', '10167')
update_pricing('wholesale', '10167')



# for _ in range(5000):
#     update_pricing('retail', '10167')
#     update_pricing('wholesale', '10167')
#     print("Process finished READ--- %s seconds ---" % (time.time() - start_time))
