# -*- coding: utf-8 -*-
import urllib2
import json
import time
import datetime
import csv
import math
import re
import sys
import errno
from socket import error as SocketError
from bs4 import BeautifulSoup
from Queue import Queue

sys.path.insert(0, '../../Utilities')
import vpnswitcher
import useragentswitcher

#api_limit = 0.2
api_limit = 0.3
start = time.time() - api_limit
elapsed = api_limit
vpn_list = vpnswitcher.getVPNs()

hdr = useragentswitcher.newHeader()
print(hdr)

counter = 0
account_ids = {}
q = Queue() #You can also specify the maximum size of the Queue here
######################################################

#################      CSV      ######################

######################################################
def createCSVs():
    with open('csv/accounts.csv', 'w') as acc_csv:
        acc_writer = csv.writer(acc_csv, delimiter=',')
        head_row = ['id', 'label', 'account', 'type']
        acc_writer.writerow(head_row)

    with open('csv/transactions.csv', 'w') as tx_csv:
        tx_writer = csv.writer(tx_csv, delimiter=',')
        head_row = ['source', 'target', 'type', 'weight', 'tx', 'block', 'date', 'unix']
        tx_writer.writerow(head_row)

def storeAccount(acc_id, acc_label, account, acc_type):
    #print('Storing in csv...')
    with open('csv/accounts.csv', 'a') as acc_csv:
        acc_writer = csv.writer(acc_csv, delimiter=',')
        acc_row = [str(acc_id), acc_label, account, acc_type]
        acc_writer.writerow(acc_row)

def storeTx(source_id, target_id, tx_type, weight, tx, block, date, unix):
    #print('Storing in csv...')
    with open('csv/transactions.csv', 'a') as tx_csv:
        tx_writer = csv.writer(tx_csv, delimiter=',')
        tx_row = [str(source_id), str(target_id), 'Directed', str(weight), tx, str(block), date, str(unix)]
        tx_writer.writerow(tx_row)

def loadIndex():
    global counter
    global account_ids

    with open('csv/accounts.csv', 'r') as acc_csv:
        acc_reader = csv.reader(acc_csv)
        first = True
        for row in acc_reader:
            if first:
                first = False
            else:
                account_ids[row[2]] = int(row[0])

        counter = len(account_ids)
######################################################

def resetTime():
    global start
    start = time.time()

def timeElapsed(t):
    global elapsed
    now = time.time()
    #print('Now: ' + str(now))
    elapsed = now - start
    #print('Elapsed time: ' + str(elapsed))
    if elapsed > t:
        return True
    else:
        return False

def queryAPI(url):
    try:
        if not timeElapsed(api_limit):
            wait_time = api_limit - elapsed
            #print('Not enough time has elapsed...Sleeping ' + str(wait_time) + ' seconds...')
            time.sleep(wait_time)

        request = urllib2.Request(url, headers=hdr)
        response = urllib2.urlopen(request)
        resetTime()

        data = json.load(response)
        return data

    except Exception as e:
        print(e)
        print('Waiting 15 minutes...')
        time.sleep(15 * 60)
        queryAPI(url)

def queryHTML(url):
    global hdr

    try:
        if not timeElapsed(api_limit):
            wait_time = api_limit - elapsed
            #print('Not enough time has elapsed...Sleeping ' + str(wait_time) + ' seconds...')
            time.sleep(wait_time)

        request = urllib2.Request(url, headers=hdr)
        response = urllib2.urlopen(request)
        #try:
        #    response = urllib2.urlopen(request)
        #except URLError, e:
        #     print e.code
        #     print e.read()
        resetTime()
        soup = BeautifulSoup(response, 'html.parser')

        return soup

    #except SocketError as e:
    except Exception as e:
        print(e)
        print('Changing VPN and User Agent...')
        vpnswitcher.randomVPN(vpn_list)
        hdr = useragentswitcher.newHeader()
        queryHTML(url)

    #except Exception as e:
    #    print(e)
    #    print('Waiting 15 minutes...')
    #    time.sleep(15 * 60)
    #    queryAPI(url)


def getTopAccounts(limit):
    global counter
    global account_ids
    account_counter = 0
    accounts = []
    pages = int(math.ceil(limit/25.0))
    if account_counter < limit:
        for x in range(1, pages+1):
            if x == 1:
                url = 'https://etherscan.io/accounts'
            else:
                url = 'https://etherscan.io/accounts/' + str(x)

            soup = queryHTML(url)
            table = soup.find('tbody')
            rows = table.find_all('tr')
            for row in rows:
                row_contents = row.contents[1].contents
                if len(row_contents) == 1:
                    account = row_contents[0].string
                    acc_label = row_contents[0].string
                    acc_type = 'Wallet'
                else:
                    if row_contents[0].name == 'i':
                        acc_type = 'Contract'
                        if row_contents[-1].name == 'a':
                            account = row_contents[-1].string
                            acc_label = row_contents[-1].string
                        else:
                            account = row_contents[-2].string
                            acc_label = row_contents[-1].string[3:]
                    else:
                        account = row_contents[-2].string
                        acc_label = row_contents[-1].string[3:]
                        acc_type = 'Wallet'

                #print(account)
                #print(acc_label)
                #print(acc_type)
                if account_counter < limit:
                    storeAccount(counter, acc_label, account, acc_type)

                    accounts.append(account)
                    account_ids[account] = counter
                    counter += 1
                    account_counter += 1

    return accounts

def lookupAccount(account):
    url = 'https://etherscan.io/address/' + account
    soup = queryHTML(url)
    try:
        acc_label = soup.find('font',  attrs={'title': 'NameTage'}).string
    except:
        acc_label = account

    try:
        acc_type_check = soup.find('tr', attrs={'id': 'ContentPlaceHolder1_trContract'})
        acc_type = 'Contract'
    except:
        acc_type = ''

    acc_info = {
        'label' : acc_label,
        'type' : acc_type
    }

    return acc_info

def getAccountTransactions(account):
    global counter
    global account_ids
    global q
    tx_max = 2000 #Accounts aren't added to queue if they connected to
                  #an account with transactions equal to or above tx_max
                  #as they likely indicate an exchange and its unreasonable to crawl every account

    url = 'http://api.etherscan.io/api?module=account&action=txlist&address=' + account + '&startblock=0&endblock=99999999&sort=asc'
    try:
        data = queryAPI(url)
        #print(json.dumps(data, indent=4, separators=(',', ': ')))
        result = data['result']
        txs = len(result)

        for r in result:
            if r['from'] == '':
                from_address = r['contractAddress']
                from_type = 'Contract'
            else:
                from_address = r['from']
                from_type = 'Wallet'

            if r['to'] == '':
                to_address = r['contractAddress']
                to_type = 'Contract'
            else:
                to_address = r['to']
                to_type = 'Wallet'

            if from_address in account_ids:
                from_id = account_ids[from_address]
            else:
                from_id = counter
                #from_info = lookupAccount(from_address)
                #from_label = from_info['label']
                #from_type = from_info['type']
                #storeAccount(from_id, from_label, from_address, from_type)
                storeAccount(from_id, from_address, from_address, from_type)
                #storeAccount(from_id, from_address, from_address, '')
                account_ids[from_address] = from_id
                if from_type != 'Contract' and txs < tx_max:
                    q.put(from_address)
                counter += 1

            if to_address in account_ids:
                to_id = account_ids[to_address]
            else:
                to_id = counter
                #to_info = lookupAccount(to_address)
                #to_label = to_info['label']
                #to_type = to_info['type']
                #storeAccount(to_id, to_label, to_address, to_type)
                storeAccount(to_id, to_address, to_address, to_type)
                #storeAccount(to_id, to_address, to_address, '')
                account_ids[to_address] = counter
                if to_type != 'Contract' and txs < tx_max:
                    q.put(to_address)
                counter += 1

            value = int(r['value']) / 1000000000000000000
            tx = r['hash']
            block = r['blockNumber']
            date = ''
            unix = r['timeStamp']

            storeTx(from_id, to_id, 'Directed', value, tx, block, date, unix)

        return result

    except Exception as e:
        print(e)
        return False

def scrapeAccountTransactions(account, page):
    global counter
    global account_ids
    more_pages = True
    domain = 'https://etherscan.io/'
    url = domain + 'txs?a=' + account + '&p=' + str(page)
    soup = queryHTML(url)
    table = soup.find('tbody')
    rows = table.find_all('tr')

    for row in rows:
    #row = rows[0]
        columns = row.find_all('td')
        try:
            tx = columns[0].find('a').string
        except:
            try:
                tx = columns[0].find('span').string
            except:
                break
        block = columns[1].find('a').string
        date = columns[2].find('span').get('title')
        unix = time.mktime(datetime.datetime.strptime(date, '%b-%d-%Y %I:%M:%S %p').timetuple())
        from_address = columns[3].contents[-1]
        try:
            from_address = from_address.find('a').get('href')[9:]
        except:
            from_address = account

        to_address = columns[5].contents[-1]
        try:
            to_address = to_address.find('a').get('href')[9:]
        except:
            to_address = account

        value_contents = columns[6].contents
        if len(value_contents) == 1:
            value = value_contents[0]
        else:
            value = value_contents[0] + value_contents[1].string + value_contents[2]
        value = str(value[:-6]).strip(',')

        print(tx)
        #print(block)
        #print(date)
        #print(unix)
        #print(from_address)
        #print(to_address)
        #print(value)

        if from_address in account_ids:
            from_id = account_ids[from_address]
        else:
            from_id = counter
            #from_info = lookupAccount(from_address)
            #from_label = from_info['label']
            #from_type = from_info['type']
            #storeAccount(from_id, from_label, from_address, from_type)
            storeAccount(from_id, from_address, from_address, '')
            account_ids[from_address] = from_id
            counter += 1

        if to_address in account_ids:
            to_id = account_ids[to_address]
        else:
            to_id = counter
            #to_info = lookupAccount(to_address)
            #to_label = to_info['label']
            #to_type = to_info['type']
            #storeAccount(to_id, to_label, to_address, to_type)
            storeAccount(to_id, to_address, to_address, '')
            account_ids[to_address] = counter
            counter += 1

        storeTx(from_id, to_id, 'Directed', value, tx, block, date, unix)

    next_page = soup.find(text=re.compile('next', re.IGNORECASE)).parent.get('href')
    if next_page != '#':
        page = page + 1
        return page
    else:
        return 0

def scrapeHTMLTransactions(accounts):
    print(accounts)
    #data = getAccountTransactions(account[0])
    #print(json.dumps(data, indent=4, separators=(',', ': ')))
    for account in accounts:
        vpnswitcher.randomVPN(vpn_list)
        hdr = useragentswitcher.newHeader()
        page = 1
        while page:
            try:
                page = scrapeAccountTransactions(account, page)
            except AttributeError:
                page = 0

def crawlTransactions(accounts):
    global q
    print(accounts)

    for a in accounts:
        q.put(a)

    while not q.empty():
        print('Queue Length: ' + str(q.qsize()))
        # It won;t block if there are no items to pop
        account = q.get(block = False)
        print('Account: ' + account)
        getAccountTransactions(account)

        #if is_item_mature(account):
            #process
        #else:
            #In case your Queue has a maxsize, consider making it non blocking
        #    q.put(account)

createCSVs()
accounts = getTopAccounts(3)
loadIndex()

crawlTransactions(accounts)
