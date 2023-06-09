import redis
import time
start_time = time.time()


def store_data(id, data):
    r = redis.Redis(host='localhost', port=6379, db=0)

    # Store the main attributes
    main_attributes = {k: v for k, v in data.items() if k != 'tierprices'}
    r.hmset(f'{id}', main_attributes)

    # Store the tierprices
    for i, tierprice in enumerate(data['tierprices']):
        r.hmset(f'{id}:tierprices:{i}', tierprice)

    # Create a list of keys to the tierprices
    r.delete(f'{id}:tierprices')  # delete the key in case it already exists
    for i in range(len(data['tierprices'])):
        r.rpush(f'{id}:tierprices', f'{id}:tierprices:{i}')


data1 = {
    "currencySymbol": "US$",
    "lpmBuyPrice": "",
    "priceMinimal": "33.84",
    "tierprices": [
        {
            "qty": 1,
            "wireprice": "34.84",
            "cardprice": "36.67",
            "cryptoprice": "35.19"
        },
        {
            "qty": 20,
            "wireprice": "34.34",
            "cardprice": "36.15",
            "cryptoprice": "34.69"
        },
        {
            "qty": 100,
            "wireprice": "33.84",
            "cardprice": "35.62",
            "cryptoprice": "34.18"
        }
    ]
}

data2 = {
    "currencySymbol": "US$",
    "lpmBuyPrice": "",
    "priceMinimal": "23.84",
    "tierprices": [
        {
            "qty": 1,
            "wireprice": "24.84",
            "cardprice": "26.67",
            "cryptoprice": "25.19"
        },
        {
            "qty": 20,
            "wireprice": "24.34",
            "cardprice": "26.15",
            "cryptoprice": "24.69"
        },
        {
            "qty": 100,
            "wireprice": "23.84",
            "cardprice": "25.62",
            "cryptoprice": "24.18"
        }
    ]
}

store_data('10167', data1)
print("Process finished 10167--- %s seconds ---" % (time.time() - start_time))
store_data('10168', data2)
print("Process finished 10168--- %s seconds ---" % (time.time() - start_time))



def get_tierprice_usd(id, tier):
    # Connect to your Redis instance
    r = redis.Redis(host='localhost', port=6379, db=0)

    # Get the tierprice hash
    tierprice_usd = r.hgetall(f'{id}:tierprices:{tier}:USD')
    
    # Convert bytes to string
    tierprice_usd = {k.decode('utf-8'): v.decode('utf-8') for k, v in tierprice_usd.items()}

    return tierprice_usd

# Get USD tierprice for a specific ID and tier
id = "10167"
tier = 0  # replace with the tier index you want
tierprice_usd = get_tierprice_usd(id, tier)
print(tierprice_usd)

