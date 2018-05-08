import urllib2
import json
import time
from datetime import datetime

hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}

coins = []

def lookupCoins():
    global coins
    url = 'https://www.cryptocompare.com/api/data/coinlist/'
    request = urllib2.Request(url, headers=hdr)
    response = urllib2.urlopen(request)

    data = json.load(response)

    coins = data['data']
    return coins

def lookupPrice(timelist, symbol, currency, local):
    if local:
        offset = time.timezone if (time.localtime().tm_isdst == 0) else time.altzone
    else:
        offset = 0

    pricelist = []
    if len(timelist[0] >= 13):
        if len(timelist[0] == 20):
            multiple = 8
            api = 'https://min-api.cryptocompare.com/data/histominute'
            timeformat = "%Y-%m-%dT%H:%M:%SZ"
        if len(timelist[0]) == 13:
            multiple = 1
            api = 'https://min-api.cryptocompare.com/data/histohour'
            timeformat = "%Y-%m-%dT%H"

        limit = len(timelist) * multiple
        url = api + '?fsym=' + symbol + '&tsym=' + currency + '&limit=' + str(limit)
        request = urllib2.Request(url, headers=hdr)
        response = urllib2.urlopen(request)

        data = json.load(response)

        for t in timelist:
            dt = datetime.strptime(t, timeformat)
            unixtime = time.mktime(dt.timetuple())
            unixtime = int(unixtime) - offset
            unixtime = str(unixtime)

            i = timelist.index(t) * multiple
            if data['Data'][i]['time'] == unixtime:
                pricelist.append(data['Data'][i]['close'])
            else:
                print 'Times do not match!'

    else:
        for t in timelist:
            dt = datetime.strptime(t, "%Y-%m-%d")
            unixtime = time.mktime(dt.timetuple())
            unixtime = int(unixtime) - offset
            unixtime = str(unixtime)

            url = 'https://min-api.cryptocompare.com/data/pricehistorical?fsym=' + symbol + '&tsyms=' + currency + '&ts=' + unixtime

            request = urllib2.Request(url, headers=hdr)
            response = urllib2.urlopen(request)

            data = json.load(response)
            #print json.dumps(data, indent=4, separators=(',', ': '))
            pricelist.append(data[symbol.upper()][currency.upper()])



    return pricelist

def getPrice(symbol):
    url = 'https://min-api.cryptocompare.com/data/price?fsym=' + symbol.upper() + '&tsyms=BTC,USD,EUR'
    request = urllib2.Request(url, headers=hdr)
    response = urllib2.urlopen(request)

    data = json.load(response)

    usd = data['USD']
    return usd


def getSocialStats(symbol):
    global coins

    if len(coins) == 0:
        coins = lookupCoins()

    coin_id = coins[symbol.upper()]

    url = 'https://www.cryptocompare.com/api/data/socialstats/?id=' + coin_id
    request = urllib2.Request(url, headers=hdr)
    response = urllib2.urlopen(request)

    data = json.load(response)
    stats = {}
    stats['twitter'] = data['Twitter']
    stats['reddit'] = data['Reddit']
    stats['facebook'] = data['Facebook']
    stats['repository'] = data['CodeRepository']['List'][0]
    stats['repository']['Points'] = data['CodeRepository']['Points']

    return stats
