from binance.spot import Spot

client = Spot()

# Get server timestamp
print(client.time())
# Get klines of BTCUSDT at 1m interval
#print(client.klines("BTCUSDT", "1m"))
Btc_list =[];
Btc_list=client.klines("BTCUSDT","1w",limit=1);
Btc_list.sort(key=lambda x: abs(x[0]))
# Get last 10 klines of BNBUSDT at 1h interval
print(client.klines("BNBUSDT", "1h", limit=10))



# Get account and balance information
print(client.account())

# Post a new order
params = {
    'symbol': 'BTCUSDT',
    'side': 'SELL',
    'type': 'LIMIT',
    'timeInForce': 'GTC',
    'quantity': 0.002,
    'price': 9500
}

response = client.new_order(**params)
print(response)

