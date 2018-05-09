import urllib
import json

def downloadSymbols(limit, filepath):
    #Get coinmarketcap data
    coins = {}

    url = 'https://api.coinmarketcap.com/v1/ticker/?limit=' + str(limit)
    response = urllib.urlopen(url)
    coinmarketcap = json.loads(response.read())
    for coin in coinmarketcap:
        name = coin['name'].lower()
        name = name.replace(" ", "-")
        symbol = coin['symbol'].lower()

        coins[name] = symbol

    with open(filepath, 'w') as outfile:
        json.dump(coins, outfile)

def getPriceStats(limit):
    coins = {}

    url = 'https://api.coinmarketcap.com/v1/ticker/?limit=' + str(limit)
    response = urllib.urlopen(url)
    coinmarketcap = json.loads(response.read())
    for coin in coinmarketcap:
        symbol = coin['symbol'].lower()
        usd = ['price_usd']
        btc = ['price_btc']
        marketcap = coin['market_cap_usd']
        day_volume = coin['24h_volume_usd']
        hour_change = ['percent_change_1h']
        day_change = ['percent_change_24h']
        week_change = ['percent_change_7d']
        last_update = ['last_updated']


        coins[symbol] = {}
        coins[symbol]['usd'] = usd
        coins[symbol]['btc'] = btc
        coins[symbol]['marketcap'] = marketcap
        coins[symbol]['day_volume'] = day_volume
        coins[symbol]['hour_change'] = hour_change
        coins[symbol]['day_change'] = day_change
        coins[symbol]['week_change'] = week_change
        coins[symbol]['last_update'] = last_update

    return coins
