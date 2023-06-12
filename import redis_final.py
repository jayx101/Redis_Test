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
    product_id = values['id']

    # Product details (exclude tierprices)
    details = {k: v for k, v in values.items() if k != 'tierprices'}
    detail_key = f"{product_id}:details:{key}"
    pipe.hmset(detail_key, details)

    # Tier prices
    for i, tierprice in enumerate(values['tierprices']):
        tier_redis_key = f"{product_id}:tierprices:{i}:{key}"
        pipe.hmset(tier_redis_key, tierprice)

# Execute all commands in the pipeline
pipe.execute()
print("Process finished WRITE--- %s seconds ---" % (time.time() - start_time))




import random
import copy

def create_data_variations(data, n=15000):
    variations = []
    for _ in range(n):
        # Make a deep copy of the original data to avoid modifying it
        data_copy = copy.deepcopy(data)

        # Increment the product ID
        for product_type in data_copy:
            data_copy[product_type]['id'] = str(int(data_copy[product_type]['id']) + len(variations) + 1)

            # Modify the margin and marginpct values randomly within 3 percent of their original values
            margin = float(data_copy[product_type]['margin'])
            marginpct = float(data_copy[product_type]['marginpct'])
            data_copy[product_type]['margin'] = str(margin + margin * random.uniform(-0.03, 0.03))
            data_copy[product_type]['marginpct'] = str(marginpct + marginpct * random.uniform(-0.03, 0.03))

            for tierprice in data_copy[product_type]['tierprices']:
                margin = float(tierprice['margin'])
                marginpct = float(tierprice['marginpct'])
                tierprice['margin'] = str(margin + margin * random.uniform(-0.03, 0.03))
                tierprice['marginpct'] = str(marginpct + marginpct * random.uniform(-0.03, 0.03))

        variations.append(data_copy)

    return variations


def write_variations_to_redis(r, data_variations):
    chunk_size = 5000  # Adjust this number based on your system's capacity

    for i in range(0, len(data_variations), chunk_size):
        pipe = r.pipeline()

        # Process each chunk of data
        for data in data_variations[i:i+chunk_size]:

            # Here we loop over the main keys (retail, wholesale)
            for key, values in data.items():
                product_id = values['id']

                # Product details (exclude tierprices)
                details = {k: v for k, v in values.items() if k != 'tierprices'}
                detail_key = f"{product_id}:details:{key}"
                pipe.hmset(detail_key, details)

                # Tier prices
                for i, tierprice in enumerate(values['tierprices']):
                    tier_redis_key = f"{product_id}:tierprices:{i}:{key}"
                    pipe.hmset(tier_redis_key, tierprice)

        # Execute all commands in the pipeline
        pipe.execute()
        print("Process finished WRITE CHUNK--- %s seconds ---" % (time.time() - start_time))



data_variations = create_data_variations(data)
print("Process finished Variations--- %s seconds ---" % (time.time() - start_time))

write_variations_to_redis(r,data_variations)
print("Process finished WRITE ALL--- %s seconds ---" % (time.time() - start_time))



# def get_product_data(product_type, product_id):
#     # Connect to your Redis instance
#     r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

#     # Form the base key for the product
#     base_key = f"{product_id}:{product_type}"

#     # Open a Redis pipeline
#     pipe = r.pipeline()

#     # Get the base product data
#     pipe.hgetall(base_key)

#     # Get the list of tier price keys
#     pipe.lrange(f"{base_key}:tierprice_keys", 0, -1)

#     # Execute the commands to get the base product data and the list of tier price keys
#     results = pipe.execute()

#     # The first result is the base product data
#     product_data = results[0]

#     # The second result is the list of tier price keys
#     tierprice_keys = results[1]

#     # Open another Redis pipeline to get the tier prices
#     pipe = r.pipeline()

#     # Get all tier prices using the tier price keys
#     for key in tierprice_keys:
#         pipe.hgetall(key)

#     # Execute the commands to get the tier prices
#     tierprices_results = pipe.execute()

#     # The results are the tier prices
#     # Note: If a tier price doesn't exist, hgetall will return an empty dictionary
#     tierprices = [result for result in tierprices_results if result]

#     # Add the tier prices to the product data
#     product_data['tierprices'] = tierprices

#     return product_data


# # Usage:
# print(get_product_data('retail', '10167'))
# print(get_product_data('wholesale', '10167'))
# print("Process finished READ--- %s seconds ---" % (time.time() - start_time))



# def update_pricing(product_type, product_id):
#     # Connect to your Redis instance
#     r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

#     # Form the base key for the product
#     base_key = f"{product_id}:{product_type}"

#     # Fetch the product data
#     product_data = r.hgetall(base_key)

#     # Fetch the metal price
#     metal_price = float(r.hget(f"{product_type}:{product_data['metal']}:padding", 'ask'))

#     # Calculate lpmBuyPrice
#     weightoz = float(product_data['weightoz'])
#     margin_multiplier = 1 + float(product_data.get('marginpct', 0))
#     lpmBuyPrice = weightoz * metal_price * margin_multiplier

#     # Update lpmBuyPrice in Redis
#     r.hset(base_key, 'lpmBuyPrice', lpmBuyPrice)

#     # Fetch the keys to the tierprices
#     tierprice_keys = r.lrange(f"{base_key}:tierprice_keys", 0, -1)

#     # Process each tierprice
#     for tierprice_key in tierprice_keys:
#         # Fetch the tierprice data
#         tierprice_data = r.hgetall(tierprice_key)

#         # Calculate price
#         margin_multiplier = 1 + float(tierprice_data.get('marginpct', 0))
#         price = weightoz * metal_price * margin_multiplier

#         # Calculate wireprice, cardprice, cryptoprice
#         wireprice = price * float(product_data.get('wirepct', 0))
#         cardprice = price * float(product_data.get('cardpct', 0))
#         cryptoprice = price *  float(product_data.get('cryptopct', 0))


#         # Update calculated values in Redis
#         r.hmset(tierprice_key, {'price': price, 'wireprice': wireprice, 'cardprice': cardprice, 'cryptoprice': cryptoprice})

# # Run the function for a product
# update_pricing('retail', '10167')
# update_pricing('wholesale', '10167')



# # for _ in range(5000):
# #     update_pricing('retail', '10167')
# #     update_pricing('wholesale', '10167')
# #     print("Process finished READ--- %s seconds ---" % (time.time() - start_time))


import redis

# Connect to your Redis instance
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Fetch all product IDs
def get_all_product_ids():
    return r.keys('*:retail:details') + r.keys('*:wholesale:details')

# Fetch all product data in batch
def get_all_product_data(product_ids):
    product_data = {}
    for product_id in product_ids:
        product_data[product_id] = get_product_data(product_id)
    return product_data

def get_product_data(product_id):
    # Open a Redis pipeline
    pipe = r.pipeline()

    # Get the base product data for both retail and wholesale
    for product_type in ["retail", "wholesale"]:
        base_key = f"{product_id}:{product_type}:details"
        pipe.hgetall(base_key)

    # Execute the commands to get the base product data and the list of tier price keys
    results = pipe.execute()

    # The first result is the retail data, the second is the wholesale data
    product_data = {'retail': results[0], 'wholesale': results[1]}
    return product_data

def update_all_pricing(product_ids):
    for product_id in product_ids:
        for product_type in ["retail", "wholesale"]:
            update_pricing(product_type, product_id)

def update_pricing(product_type, product_id):
    # Form the base key for the product
    base_key = f"{product_id}:{product_type}:details"

    # Open a Redis pipeline
    pipe = r.pipeline()

    # Fetch the product details and metal price
    pipe.hgetall(base_key)
    metal_price_key = f"{product_type}:{product_id}:metal:padding"
    pipe.hget(metal_price_key, 'ask')

    # Execute the pipeline and store the results
    product_details, metal_price = pipe.execute()

    # Calculate lpmBuyPrice
    weightoz = float(product_details['weightoz'])
    margin_multiplier = 1 + float(product_details.get('marginpct', 0))
    lpmBuyPrice = weightoz * float(metal_price) * margin_multiplier

    # Update lpmBuyPrice in Redis
    pipe.hset(base_key, 'lpmBuyPrice', lpmBuyPrice)

    # Tier prices
    tierprice_keys = r.keys(f"{product_id}:{product_type}:tierprices:*")

    for tierprice_key in tierprice_keys:
        # Fetch the tierprice data
        tierprice_data = r.hgetall(tierprice_key)

        # Calculate price
        margin_multiplier = 1 + float(tierprice_data.get('marginpct', 0))
        price = weightoz * float(metal_price) * margin_multiplier

        # Calculate wireprice, cardprice, cryptoprice
        wireprice = price * float(product_details.get('wirepct', 0))
        cardprice = price * float(product_details.get('cardpct', 0))
        cryptoprice = price *  float(product_details.get('cryptopct', 0))

        # Update calculated values in Redis
        pipe.hmset(tierprice_key, {'price': price, 'wireprice': wireprice, 'cardprice': cardprice, 'cryptoprice': cryptoprice})

    # Execute all commands in the pipeline
    pipe.execute()


# Get all product IDs
product_ids = get_all_product_ids()

# Get all product data
all_product_data = get_all_product_data(product_ids)

# Update all pricing
update_all_pricing(product_ids)
