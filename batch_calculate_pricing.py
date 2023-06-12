import redis
import json
import time

start_time = time.time()

# Connect to your Redis instance
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Fetch all product IDs
def get_all_product_ids():
    return [k.split(':')[0] for k in r.keys('*:details:retail')]

# Fetch all product data in batch
def get_all_product_data(product_ids):
    # Open a Redis pipeline for fetching product data
    pipe = r.pipeline()

    # Fetch product details and tier prices for both retail and wholesale
    for product_id in product_ids:
        for product_type in ["retail", "wholesale"]:
            base_key = f"{product_id}:details:{product_type}"
            pipe.hmget(base_key, 'metal', 'weightoz', 'marginpct')
            pipe.hget(base_key, 'tierprices')

    # Execute the pipeline and store the results
    results = pipe.execute()

    # Organize the results into product data dictionary
    product_data = {}
    count = 0
    for product_id in product_ids:
        product_types = {}
        for product_type in ["retail", "wholesale"]:
            metal, weightoz, marginpct = results[count]
            tier_prices_json = results[count + 1]
            tier_prices = json.loads(tier_prices_json) if tier_prices_json else []
            product_types[product_type] = {'metal': metal, 'weightoz': weightoz, 'marginpct': marginpct, 'tier_prices': tier_prices}
            count += 2
        product_data[product_id] = product_types

    return product_data

def update_all_pricing(product_data, batch_size=1000):
    # Open a Redis pipeline for updating pricing
    pipe = r.pipeline()

    count = 0  # Counter for tracking the number of products processed

    for product_id, product_types in product_data.items():
        for product_type, data in product_types.items():
            metal = data['metal']
            weightoz = float(data['weightoz'])
            marginpct = float(data.get('marginpct', 0))

            # Fetch the metal price
            metal_price_key = f"{product_type}:{metal}:padding"
            pipe.hget(metal_price_key, 'ask')

            # Calculate lpmBuyPrice
            margin_multiplier = 1 + marginpct
            lpmBuyPrice = weightoz * float(pipe.execute()[0]) * margin_multiplier

            # Update lpmBuyPrice in Redis
            base_key = f"{product_id}:details:{product_type}"
            pipe.hset(base_key, 'lpmBuyPrice', lpmBuyPrice)

            count += 1  # Increment the counter

            # Execute the pipeline in batches
            if count % batch_size == 0:
                pipe.execute()
                print("Process finished Calculate & Update BATCH --- %s seconds ---" % (time.time() - start_time))

    # Execute any remaining commands in the pipeline
    pipe.execute()


def main():
    # Get all product IDs
    product_ids = get_all_product_ids()
    print("Process finished Get ID's --- %s seconds ---" % (time.time() - start_time))

    # Get all product data
    all_product_data = get_all_product_data(product_ids)
    print("Process finished Get Data for ID's --- %s seconds ---" % (time.time() - start_time))

    # Update all pricing
    update_all_pricing(all_product_data)
    print("Process finished Calculate & Update --- %s seconds ---" % (time.time() - start_time))


if __name__ == "__main__":
    main()
