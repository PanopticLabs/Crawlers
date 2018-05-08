import httplib
import urllib
import json
import csv
import sys
import time

#Currently set to Vancouver
lat = 49.2000000000000
lng = -123.00000000000000
rng = 0.1
#####################
page = 1

realtors = {}
postals = {}

#Create empty csv files
with open('csv/data.csv', 'w') as data_csv:
    head_row = ['Listing ID', 'Realtor ID', 'Realtor Name', 'Realtor Phone', 'Price', 'Address', 'Postal Code', 'Latitude', 'Longitude', 'Property Type', 'Size', 'Bedrooms']
    writer = csv.writer(data_csv, delimiter=',')
    writer.writerow(head_row)
with open('csv/realtors.csv', 'w') as realtors_csv:
    head_row = ['Name', 'Phone', 'Total Value', 'Properties']
    writer = csv.writer(realtors_csv, delimiter=',')
    writer.writerow(head_row)
with open('csv/postals.csv', 'w') as postals_csv:
    head_row = ['Postal Code', 'Properties', 'Total Value', 'Average']
    writer = csv.writer(postals_csv, delimiter=',')
    writer.writerow(head_row)

def getListings(lat, lng, rng, pg):
    print 'Scraping page ' + str(pg) + '...'
    max_lat = lat + rng
    min_lat = lat - rng
    max_lng = lng + rng
    min_lng = lng - rng

    data = urllib.urlencode({'ApplicationId' : '1',
            'CultureId' : '1',
            'BathRange' : '0-0',
            'BedRange' : '0-0',
            'CurrentPage' : pg,
            'LatitudeMax' : max_lat,
            'LatitudeMin' : min_lat,
            'LongitudeMax' : max_lng,
            'LongitudeMin' : min_lng,
            'MaximumResults' : '50',
            'PropertySearchTypeId' : '1',
            'PropertyTypeGroupID' : '1',
            'RecordsPerPage' : '50',
            'SortBy' : '1',
            'SortOrder' : 'A',
            'StoreyRange' : '0-0',
            'TransactionTypeId' : '2',
            'viewState' : 'm'})

    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive'}

    server = 'api2.realtor.ca'
    url = '/Listing.svc/PropertySearch_Post'

    conn = httplib.HTTPConnection(server)
    conn.request("POST", url, data, hdr)
    response = conn.getresponse()

    json_data = json.loads(response.read())
    #print(json.dumps(json_data, indent=4, separators=(',', ': ')))

    page_info = json_data['Paging']
    records_showing = page_info['RecordsShowing']
    total_records = page_info['TotalRecords']
    current_page = page_info['CurrentPage']
    total_pages = page_info['TotalPages']

    results = json_data['Results']
    with open('csv/data.csv', 'a') as data_csv:
        writer = csv.writer(data_csv, delimiter=',')

        for result in results:
            listing_id = result['Id']
            try:
                realtor_id = result['Individual'][0]['IndividualID']
            except:
                realtor_id = ''
            try:
                realtor_name = result['Individual'][0]['Name']
            except:
                realtor_name = ''
            try:
                realtor_phone = result['Individual'][0]['Phones'][0]['AreaCode'] + '-' + result['Individual'][0]['Phones'][0]['PhoneNumber']
            except:
                realtor_phone = ''
            try:
                price = result['Property']['Price'].replace('$', '').replace(',', '')
            except:
                price = 0

            if realtor_id in realtors:
                total_value = int(realtors[realtor_id]['Total Value']) + int(price)
                realtors[realtor_id]['Total Value'] = total_value
                realtors[realtor_id]['Properties'] += 1
            else:
                realtors[realtor_id] = {'Name' : realtor_name, 'Phone' : realtor_phone, 'Total Value' : int(price), 'Properties' : 1}

            try:
                address = result['Property']['Address']['AddressText']
            except:
                address = ''
            print address
            try:
                postal_code = result['PostalCode']
            except:
                postal_code = ''

            if postal_code in postals:
                postals[postal_code]['Total Value'] = int(postals[postal_code]['Total Value']) + int(price)
                postals[postal_code]['Properties'] += 1
                postals[postal_code]['Average'] = int(postals[postal_code]['Total Value']) / int(postals[postal_code]['Properties'])
            else:
                postals[postal_code] = {'Total Value' : int(price), 'Properties' : 1, 'Average' : int(price)}

            try:
                latitude = result['Property']['Address']['Latitude']
                longitude = result['Property']['Address']['Longitude']
            except:
                latitude = ''
                longitude = ''
            try:
                property_type = result['Building']['Type']
            except:
                property_type = ''
            try:
                size = result['Building']['SizeInterior']
            except:
                size = ''
            try:
                bedrooms = result['Building']['Bedrooms']
            except:
                bedrooms = ''

            row = [listing_id, realtor_id, realtor_name, realtor_phone, price, address, postal_code, latitude, longitude, property_type, size, bedrooms]
            writer.writerow(row)

    print 'Total Records: ' + str(total_records)
    print 'Current Page: ' + str(current_page)
    print 'Total Pages: ' + str(total_pages)
    if int(current_page) < int(total_pages):
        print 'Waiting 30 seconds...'
        time.sleep(30)
        new_page = int(current_page) + 1
        getListings(lat, lng, rng, new_page)


getListings(lat, lng, rng, page)

with open('csv/realtors.csv', 'a') as realtors_csv:
    writer = csv.writer(realtors_csv, delimiter=',')
    for realtor_id in realtors:
        row = [realtors[realtor_id]['Name'], realtors[realtor_id]['Phone'], realtors[realtor_id]['Total Value'], realtors[realtor_id]['Properties']]
        writer.writerow(row)

with open('csv/postals.csv', 'a') as postals_csv:
    writer = csv.writer(postals_csv, delimiter=',')
    for postal_code in postals:
        head_row = ['Postal Code', 'Properties', 'Total Value', 'Average']
        row = [postal_code, postals[postal_code]['Properties'], postals[postal_code]['Total Value'], postals[postal_code]['Average']]
        writer.writerow(row)

print(json.dumps(realtors, indent=4, separators=(',', ': ')))
