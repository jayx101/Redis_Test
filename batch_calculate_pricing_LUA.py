import redis
import time

start_time = time.time()

# Connect to your Redis instance
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Lua script for updating pricing
update_pricing_script = """
local product_ids_retail = redis.call("KEYS", "*:details:retail")
local product_ids_wholesale = redis.call("KEYS", "*:details:wholesale")

local function update_tierprices(product_ids, product_type)
    for _, product_id in ipairs(product_ids) do
        local weightoz = redis.call("HGET", product_id, "weightoz")
        local marginpct = redis.call("HGET", product_id, "marginpct")
        local wirepct = redis.call("HGET", product_id, "wirepct")
        local cardpct = redis.call("HGET", product_id, "cardpct")
        local cryptopct = redis.call("HGET", product_id, "cryptopct")
        local metal_type = redis.call("HGET", product_id, "metal")
        
        local ask_key = product_type .. ":" .. metal_type .. ":padding"
        local ask = redis.call("HGET", ask_key, "ask")
        local tierprices_json = redis.call("HGET", product_id, "tierprices")
        
        if ask and tierprices_json then
            local lpmBuyPrice = tonumber(weightoz) * tonumber(ask) * (1 + tonumber(marginpct))
            redis.call("HSET", product_id, "lpmBuyPrice", lpmBuyPrice)
            local timestamp = redis.call("TIME")[1] -- Get the current timestamp using the Redis TIME command
            redis.call("HSET", product_id, "calc_timestamp", tonumber(timestamp))

            local tierprices = cjson.decode(tierprices_json)
            if tierprices then
                for _, tierprice in ipairs(tierprices) do
                    local qty = tonumber(tierprice.qty)
                    local marginpct = tonumber(tierprice.marginpct)
                    
                    local price = tonumber(weightoz) * tonumber(ask) * (1 + marginpct)
                    local wireprice = price * tonumber(wirepct)
                    local cardprice = price * tonumber(cardpct)
                    local cryptoprice = price * tonumber(cryptopct)
                    
                    tierprice.price = price
                    tierprice.wireprice = wireprice
                    tierprice.cardprice = cardprice
                    tierprice.cryptoprice = cryptoprice
                end
                
                redis.call("HSET", product_id, "tierprices", cjson.encode(tierprices))
            end
        end
    end
end

update_tierprices(product_ids_retail, "retail")
update_tierprices(product_ids_wholesale, "wholesale")


"""


def update_all_pricing():
    # Execute the Lua script to update pricing
    r.eval(update_pricing_script, 0, 10000)

def main():
    # # Update all pricing
    # update_all_pricing()
    # print("Process finished Calculate & Update --- %s seconds ---" % (time.time() - start_time))

    import time

    while True:
        start_time = time.time()

        # Call the update_all_pricing function
        update_all_pricing()

        # Calculate the time taken
        elapsed_time = time.time() - start_time

        # Print the time taken
        print("Process finished Calculate & Update --- %s seconds ---" % elapsed_time)

        # Delay for 1 second
        time.sleep(1)



if __name__ == "__main__":
    main()
