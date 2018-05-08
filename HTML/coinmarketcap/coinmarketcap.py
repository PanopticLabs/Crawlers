#!/usr/local/bin/python

import urllib2
import csv
import time
import re
from bs4 import BeautifulSoup

url = 'http://www.coinmarketcap.com'

hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}

def getCoins(soup):
    table = soup.find('table')
    for coin in table.tbody.find_all('tr'):
        td = coin.find('td',  attrs={'class': 'currency-name'})

        try:
            symbol = td.span.a.string
            name = td.find('a', attrs={'class': 'currency-name-container'}).string
            coinlink = 'http://www.coinmarketcap.com' + td.a.get('href')
            print coinlink
            logo = td.img['src']
            #try:
                #imgreq = urllib2.Request(logo, headers=hdr)
                #imgdata = urllib2.urlopen(imgreq).read()
                #output = open("images/" + symbol.lower() + ".png",'wb')
                #output.write(imgdata)
                #output.close()
            #except Exception as e:
                #print e


            #Find website, then find blog link
            try:
                req = urllib2.Request(coinlink, headers=hdr)
                page = urllib2.urlopen(req)
                htmlcontent = page.read()
                page.close()

                coinsoup = BeautifulSoup(htmlcontent, 'html.parser')
                coinlinks = coinsoup.find_all('a')
                for c in coinlinks:
                    if c.string == 'Website':
                        print '4'
                        website = c.get('href')
                        currentlevel = 0
                        maxlevel = 1
                        try:
                        	blog = findBlog(website, name, currentlevel, maxlevel)
                        except:
                            blog = ''

                        break

            except:
                website = ''
                blog = ''

            row = [name, symbol, logo, website, blog]
            print row
            writer.writerow(row)

        except:
        	pass

def findBlog(url, name, currentlevel, maxlevel):
    print 'searching ' + url + '...'
    name = name.lower()
    name = name.replace(' ', '')
    bloglist = []

    try:
        req = urllib2.Request(url, headers=hdr)
        page = urllib2.urlopen(req)
        htmlcontent = page.read()
        page.close()

        sitesoup = BeautifulSoup(htmlcontent, 'html.parser')

        try:
            blog = soup.select_one("a[href*=medium]").get('href')
            print blog + '(found via medium selector)'
            bloglist.append(blog)

        except:
            pass

        try:
            string = sitesoup.find(text=re.compile('blog', re.IGNORECASE))
            if(string.name == 'a'):
                blog = string.get('href')
                print blog + '(found via blog selector)'
                bloglist.append(blog)
            else:
                for parent in string.parents:
                    if parent.name == 'a':
                        blog = parent.get('href')
                        print blog + '(found via blog selector)'
                        bloglist.append(blog)
                        break
        except:
            pass

        try:
            string = sitesoup.find(text=re.compile('news', re.IGNORECASE))
            if(string.name == 'a'):
                blog = string.get('href')
                print blog + '(found via news selector)'
                bloglist.append(blog)
            else:
                for parent in string.parents:
                    if parent.name == 'a':
                        blog = parent.get('href')
                        print blog + '(found via news selector)'
                        bloglist.append(blog)
                        break
        except:
            pass

        try:
            string = sitesoup.find(text=re.compile('insights', re.IGNORECASE))
            if(string.name == 'a'):
                blog = string.get('href')
                print blog + '(found via insights selector)'
                bloglist.append(blog)
            else:
                for parent in string.parents:
                    if parent.name == 'a':
                        blog = parent.get('href')
                        print blog + '(found via insights selector)'
                        bloglist.append(blog)
                        break
        except:
            pass

        if not bloglist:
            if currentlevel < maxlevel:
                #sitelinks = sitesoup.find_all('a')
                href = '"a[href*=' + name + ']"'
                print href
                sitelinks = sitesoup.select(href)
                for s in sitelinks:
                    nextlevel = currentlevel + 1
                    blog = findBlog(s.get('href'), name, nextlevel, maxlevel)
                    if blog != '':
                        bloglist.append(blog)

    except Exception as e:
        print e

    blog = ''
    first = True
    for b in bloglist:
        if first:
            blog = blog + b
            first = False
        else:
            blog = blog + '\r\n' + b

    return blog


csvfile = open('coinmarketcap.csv','wb')
writer = csv.writer(csvfile, delimiter=',', quotechar='"')
headrow = ['Coin', 'Symbol', 'Logo', 'Website', 'Blog']
writer.writerow(headrow)

req = urllib2.Request(url, headers=hdr)
page = urllib2.urlopen(req)
htmlcontent = page.read()
page.close()

soup = BeautifulSoup(htmlcontent, 'html.parser')

getCoins(soup)

#for p in range(2, 10):
#	nextpage = url + '/' + str(p)
#	page = urllib2.urlopen(nextpage)
#	htmlcontent = page.read()
#	page.close()
#	soup = BeautifulSoup(htmlcontent)
#	getCoins(soup)
