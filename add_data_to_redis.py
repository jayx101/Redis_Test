import redis
import time
import json

start_time = time.time()

# Connect to your Redis instance
r = redis.Redis(host='localhost', port=6379, db=0)

# Load your JSON data
data = {
    "retail": {
        "id": "10167",
        "metal": "gold",
        "weightoz": "1",
        "margin": "3",
        "marginpct": "0.2",
        "wirepct": "0.1",
        "cardpct": "0.5",
        "cryptopct": "0.01",
        "buypricemargin": "",
        "buypricepct": "",
        "lpmBuyPrice": "",
        "minimalprice": "",
        "calc_timestamp": "",
        "tierprices": [
            {
                "qty": 1,
                "margin": "2",
                "marginpct": "0.1",
                "price": "",
                "wireprice": "",
                "cardprice": "",
                "cryptoprice": ""
            },
            {
                "qty": 20,
                "margin": "3",
                "marginpct": "0.2",
                "price": "",
                "wireprice": "",
                "cardprice": "",
                "cryptoprice": ""
            },
            {
                "qty": 100,
                "margin": "4",
                "marginpct": "0.3",
                "price": "",
                "wireprice": "",
                "cardprice": "",
                "cryptoprice": ""
            }
        ]
    },
    "wholesale": {
        "id": "10167",
        "metal": "gold",
        "weightoz": "1",
        "margin": "1.5",
        "marginpct": "0.1",
        "wirepct": "0.1",
        "cardpct": "0.5",
        "cryptopct": "0.01",
        "buypricemargin": "",
        "buypricepct": "",
        "lpmBuyPrice": "",
        "minimalprice": "1.2",
        "calc_timestamp": "",
        "tierprices": [
            {
                "qty": 1,
                "margin": "1.4",
                "marginpct": "0.09",
                "price": "",
                "wireprice": "",
                "cardprice": "",
                "cryptoprice": ""
            },
            {
                "qty": 20,
                "margin": "1.3",
                "marginpct": "0.08",
                "price": "",
                "wireprice": "",
                "cardprice": "",
                "cryptoprice": ""
            },
            {
                "qty": 100,
                "margin": "1.2",
                "marginpct": "0.07",
                "price": "",
                "wireprice": "",
                "cardprice": "",
                "cryptoprice": ""
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

    # Tier prices (convert to JSON format before storing)
    tierprices = values['tierprices']
    tierprices_json = json.dumps(tierprices)
    pipe.hset(detail_key, 'tierprices', tierprices_json)

# Execute all commands in the pipeline
pipe.execute()
print("Process finished WRITE--- %s seconds ---" % (time.time() - start_time))



import random
import copy
import json

def create_data_variations(data, n=3000):
    variations = []
    for i in range(n):
        data_copy = copy.deepcopy(data)

        for product_type in data_copy:
            product_id = int(data_copy[product_type]['id'])
            data_copy[product_type]['id'] = str(product_id + i + 1)

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
    chunk_size = 5000

    for i in range(0, len(data_variations), chunk_size):
        pipe = r.pipeline()

        for data in data_variations[i:i+chunk_size]:
            for key, values in data.items():
                product_id = values['id']

                details = {k: v for k, v in values.items() if k != 'tierprices'}
                detail_key = f"{product_id}:details:{key}"
                pipe.hmset(detail_key, details)

                # Tier prices (convert to JSON format before storing)
                tierprices = values['tierprices']
                tierprices_json = json.dumps(tierprices)
                pipe.hset(detail_key, 'tierprices', tierprices_json)

        pipe.execute()
        print("Process finished WRITE CHUNK--- %s seconds ---" % (time.time() - start_time))


data_variations = create_data_variations(data)
print("Process finished Variations--- %s seconds ---" % (time.time() - start_time))

write_variations_to_redis(r,data_variations)
print("Process finished WRITE ALL--- %s seconds ---" % (time.time() - start_time))

