# -*- coding: utf-8 -*-
import tweepy
import json
import csv
import time
#import mysql.connector

#################     Twitter API       ################

########################################################
with open('twitter_cred.json') as json_cred:
    twitter_cred = json.load(json_cred)

with open('mysql_cred.json') as json_cred:
    mysql_cred = json.load(json_cred)

consumer_key = twitter_cred['consumer_key']
consumer_secret = twitter_cred['consumer_secret']
access_token = twitter_cred['access_token']
access_secret = twitter_cred['access_secret']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
# Construct the API instance
#api = tweepy.API(auth,parser=tweepy.parsers.JSONParser())
api = tweepy.API(auth)
########################################################

#################      My SQL      #####################

########################################################
mysql_user = mysql_cred['mysql_user']
mysql_pass = mysql_cred['mysql_pass']
mysql_host = mysql_cred['mysql_host']
mysql_db = 'panoptic_twitter'

def queryMySQL(query, variables=None):
    conn = connection.cursor(dictionary=True, buffered=True)
    try:
        if variables is None:
            conn.execute(query)
        else:
            conn.execute(query, variables)

        try:
            result = conn.fetchall()
            connection.commit()
            return result
        except:
            result = conn.lastrowid
            connection.commit()
            return result
    except:
        e = sys.exc_info()
        print('SQL ERROR')
        print(e)
        return
######################################################

#################      CSV      ######################

######################################################
def createCSVs():
    with open('csv/users.csv', 'w', encoding = 'utf8') as user_csv:
        user_writer = csv.writer(user_csv, delimiter=',')
        head_row = ['id', 'name', 'label', 'tweet_count', 'follower_count', 'friend_count', 'location', 'profile_image', 'banner_image', 'language', 'time_zone', 'utc_offset', 'creation_date', 'protected']
        user_writer.writerow(head_row)

    with open('csv/connections.csv', 'w', encoding = 'utf8') as conn_csv:
        conn_writer = csv.writer(conn_csv, delimiter=',')
        head_row = ['source', 'target', 'type', 'weight']
        conn_writer.writerow(head_row)

    with open('csv/index.csv', 'w', encoding = 'utf8') as index_csv:
        pass

    with open('csv/nolookup.csv', 'w', encoding = 'utf8') as nolookup_csv:
        pass
######################################################

#################    Globals    ######################

######################################################
user_index = {}
nolookup_index = []
list_limit = 60
lookup_limit = 3
start = time.time() - list_limit
elapsed = list_limit
######################################################

######################################################
def loadIndex():
    global user_index
    with open('csv/users.csv', 'r', encoding = 'utf8') as user_csv:
        user_reader = csv.reader(user_csv)
        first = True
        for row in user_reader:
            if first:
                first = False
            else:
                user_index[int(row[0])] = 0

    with open('csv/index.csv', 'r', encoding = 'utf8') as index_csv:
        index_reader = csv.reader(index_csv)
        for row in index_reader:
            user_index[int(row[0])] = 1

    with open('csv/nolookup.csv', 'r', encoding = 'utf8') as nolookup_csv:
        nolookup_reader = csv.reader(nolookup_csv)
        for row in nolookup_reader:
            nolookup_index.append(int(row[0]))

    print(user_index)

def limit_handled(cursor):
    while True:
        try:
            if not timeElapsed(list_limit):
                wait_time = list_limit - elapsed
                print('Not enough time has elapsed...Sleeping ' + str(wait_time) + ' seconds...')
                time.sleep(wait_time)

            yield cursor.next()
            resetTime()

        except tweepy.RateLimitError:
            print('Rate limit...Sleeping...')
            time.sleep(15 * 60)

        except tweepy.TweepError as e:
            print(e)
            break
            time.sleep(2 * 60)


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

def getUserID(username):
    try:
        if not timeElapsed(lookup_limit):
            wait_time = lookup_limit - elapsed
            #print('Not enough time has elapsed...Sleeping ' + str(wait_time) + ' seconds...')
            time.sleep(wait_time)

        user = api.get_user(screen_name=username)
        resetTime()

    except tweepy.RateLimitError:
        print('Rate limit...Sleeping...')
        time.sleep(15 * 60)
        user = api.get_user(screen_name=username)
        resetTime()

    except tweepy.TweepError as e:
        print(e)
        time.sleep(2 * 60)
        user = api.get_user(screen_name=username)
        resetTime()
    #print(json.dumps(user, indent=4, separators=(',', ': ')))
    return user.id

def getUserInfo(userid, min_followers=0, influencer=False, storage=False):
    global user_index
    global nolookup_index
    #print(userid)
    if userid in nolookup_index:
        print('Not looking up ' + str(userid) + '...')
        return False

    user_index[userid] = 0

    try:
        if not timeElapsed(lookup_limit):
            wait_time = lookup_limit - elapsed
            #print('Not enough time has elapsed...Sleeping ' + str(wait_time) + ' seconds...')
            time.sleep(wait_time)

        user = api.get_user(user_id=userid)
        resetTime()

    except tweepy.RateLimitError:
        print('Rate limit...Sleeping...')
        time.sleep(15 * 60)
        user = api.get_user(user_id=userid)
        resetTime()

    except tweepy.TweepError as e:
        print(e)
        time.sleep(2 * 60)
        user = api.get_user(user_id=userid)
        resetTime()

    if influencer:
        if user.followers_count <= user.friends_count:
            nolookup_index.append(userid)
            with open('csv/nolookup.csv', 'a', encoding = 'utf8') as nolookup_csv:
                nolookup_writer = csv.writer(nolookup_csv, delimiter=',')
                nolookup_row = [str(userid)]
                nolookup_writer.writerow(nolookup_row)
            print('Not an influencer...')
            return False

    if user.followers_count < min_followers:
        nolookup_index.append(userid)
        with open('csv/nolookup.csv', 'a', encoding = 'utf8') as nolookup_csv:
            nolookup_writer = csv.writer(nolookup_csv, delimiter=',')
            nolookup_row = [str(userid)]
            nolookup_writer.writerow(nolookup_row)
        print('Under minimum follower count...')
        return False

    #print(json.dumps(user, indent=4, separators=(',', ': ')))
    user_info = {
        'id' : user.id,
        'name' : user.name,
        'screen_name' : user.screen_name,
        'tweet_count' : user.statuses_count,
        'follower_count' : user.followers_count,
        'friend_count' : user.friends_count,
        'location' : user.location,
        'profile_image' : user.profile_image_url_https,
        'banner_image' : user.profile_background_image_url_https,
        'language' : user.lang,
        'time_zone' : user.time_zone,
        'utc_offset' : user.utc_offset,
        'creation_date' : user.created_at,
        'protected' : user.protected
    }
    print(user_info['screen_name'])

    if storage:
        storeUser(user_info, storage)

    return user_info

def getFriendsIDs(userid):
    print('Getting friend ids...')
    #for friend_id in limit_handled(tweepy.Cursor(api.friends_id, user_id=userid).items()):
    friends = []
    for page in limit_handled(tweepy.Cursor(api.friends_ids, user_id=userid, count=5000).pages()):
        friends = friends + page

    return friends

def getFollowersIDs(userid):
    #for follower_id in limit_handled(tweepy.Cursor(api.followers_id, user_id=userid).items()):
    followers = []
    for page in limit_handled(tweepy.Cursor(api.followers_ids, user_id=userid, count=5000).pages()):
        followers = followers + page

    return followers

def getFriends(userid, min_followers, influencer=True, storage=False):
    global user_index
    global nolookup_index

    friends_list = []
    count = 0

    for page in limit_handled(tweepy.Cursor(api.friends, user_id=userid, count=200).pages()):
        for friend in page:
            print(str(count))
            count += 1
            friend_id = friend.id
            if friend_id in nolookup_index:
                print('Not looking up ' + str(friend_id) + '...')
            else:
                if friend_id not in user_index:
                    user_index[friend_id] = 0

                    friend_info = {
                        'id' : friend.id,
                        'name' : friend.name,
                        'screen_name' : friend.screen_name,
                        'tweet_count' : friend.statuses_count,
                        'follower_count' : friend.followers_count,
                        'friend_count' : friend.friends_count,
                        'location' : friend.location,
                        'profile_image' : friend.profile_image_url_https,
                        'banner_image' : friend.profile_background_image_url_https,
                        'language' : friend.lang,
                        'time_zone' : friend.time_zone,
                        'utc_offset' : friend.utc_offset,
                        'creation_date' : friend.created_at,
                        'protected' : friend.protected
                    }
                    friends_list.append(friend_info)
                    print(friend.screen_name + ' added to list')

                    if storage:
                        storeUser(friend_info, storage)
                        storeConnection(userid, friend_id, storage)

                    #if friend.followers_count >= min_followers:
                    #    if not influencer or friend.followers_count > friend.friends_count:
                    #        friend_info = {
                    #            'id' : friend.id,
                    #            'name' : friend.name,
                    #            'screen_name' : friend.screen_name,
                    #            'tweet_count' : friend.statuses_count,
                    #            'follower_count' : friend.followers_count,
                    #            'friend_count' : friend.friends_count,
                    #            'location' : friend.location,
                    #            'profile_image' : friend.profile_image_url_https,
                    #            'banner_image' : friend.profile_background_image_url_https,
                    #            'language' : friend.lang,
                    #            'time_zone' : friend.time_zone,
                    #            'utc_offset' : friend.utc_offset,
                    #            'creation_date' : friend.created_at,
                    #            'protected' : friend.protected
                    #        }
                    #        friends_list.append(friend_info)
                    #        print(friend.screen_name + ' added to list')

                    #        if storage:
                    #            storeConnection(userid, friend_id, storage)
                else:
                    if storage:
                        storeConnection(userid, friend_id, storage)

    return friends_list

def getFollowers(userid, min_followers, influencer=True, storage=False):
    followers_list = []
    count = 0

    for page in limit_handled(tweepy.Cursor(api.followers, user_id=userid, count=200).pages()):
        for follower in page:
            print(str(count))
            count += 1
            follower_id = follower.id
            if follower_id in nolookup_index:
                print('Not looking up ' + str(follower_id) + '...')
            else:
                if follower_id not in user_index:
                    user_index[follower_id] = 0

                    #('User NOT in index:')
                    #follower = getUserInfo(follower_id, min_followers, influencer, storage)
                    follower_info = {
                        'id' : follower.id,
                        'name' : follower.name,
                        'screen_name' : follower.screen_name,
                        'tweet_count' : follower.statuses_count,
                        'follower_count' : follower.followers_count,
                        'friend_count' : follower.friends_count,
                        'location' : follower.location,
                        'profile_image' : follower.profile_image_url_https,
                        'banner_image' : follower.profile_background_image_url_https,
                        'language' : follower.lang,
                        'time_zone' : follower.time_zone,
                        'utc_offset' : follower.utc_offset,
                        'creation_date' : follower.created_at,
                        'protected' : follower.protected
                    }
                    followers_list.append(follower_info)
                    print(follower.screen_name + ' added to list')

                    if storage:
                        storeUser(follower_info, storage)
                        storeConnection(follower_id,userid, storage)

                    #if follower.followers_count >= min_followers:
                    #    if not influencer or follower.followers_count > follower.followers_count:
                    #        follower_info = {
                    #            'id' : follower.id,
                    #            'name' : follower.name,
                    #            'screen_name' : follower.screen_name,
                    #            'tweet_count' : follower.statuses_count,
                    #            'follower_count' : follower.followers_count,
                    #            'friend_count' : follower.friends_count,
                    #            'location' : follower.location,
                    #            'profile_image' : follower.profile_image_url_https,
                    #            'banner_image' : follower.profile_background_image_url_https,
                    #            'language' : follower.lang,
                    #            'time_zone' : follower.time_zone,
                    #            'utc_offset' : follower.utc_offset,
                    #            'creation_date' : follower.created_at,
                    #            'protected' : follower.protected
                    #        }
                    #        followers_list.append(follower_info)
                    #        print(follower.screen_name + ' added to list')

                    #        if storage:
                    #            storeConnection(follower_id,userid, storage)
                else:
                    if storage:
                        storeConnection(follower_id, userid, storage)

    return followers_list

def storeUser(user_info, storage):
    if storage == 'csv':
        #print('Storing in csv...')
        with open('csv/users.csv', 'a', encoding = 'utf8') as user_csv:
            user_writer = csv.writer(user_csv, delimiter=',')
            user_row = [str(user_info['id']), user_info['name'], user_info['screen_name'], user_info['tweet_count'], user_info['follower_count'], user_info['friend_count'], user_info['location'], user_info['profile_image'], user_info['banner_image'], user_info['language'], user_info['time_zone'], user_info['utc_offset'], user_info['creation_date'], user_info['protected']]
            user_writer.writerow(user_row)
    elif storage == 'sql':
        print('Storing in database...')
    else:
        print('Unknown storage type...')

def storeConnection(source_id, target_id, storage):
    if storage == 'csv':
        #print('Storing in csv...')
        with open('csv/connections.csv', 'a', encoding = 'utf8') as conn_csv:
            conn_writer = csv.writer(conn_csv, delimiter=',')
            conn_row = [str(source_id), str(target_id), 'Directed', 1]
            conn_writer.writerow(conn_row)
    elif storage == 'sql':
        print('Storing in database...')
    else:
        print('Unknown storage type...')

def load_and_sort(filename):
    global user_index
    friend_list = []

    with open('csv/' + filename, 'r', encoding = 'utf8') as user_csv:
        user_reader = csv.reader(user_csv)
        first = True
        second = False
        count = 0

        for row in user_reader:
            if first:
                first = False
                second = True
            elif second:
                second = False
                count += 1
            else:
                print(str(count) + ': ' + row[0])
                user_index[int(row[0])] = 0
                friend_list.append({'id' : int(row[0]), 'follower_count' : int(row[4])})
                count += 1

    sort_list = sorted(friend_list, key=lambda k: k['follower_count'], reverse=True)
    return sort_list

def getAssocIDs(user_list, add_all=False, start=0, storage=False):
    global user_index
    if start:
        start_index = start - 1
        count = start_index
        part_list = user_list[start_index:]
    else:
        count = 0
        part_list = user_list


    total = len(user_list)

    for user in part_list:
        count += 1
        print(str(count) + ' of ' + str(total))
        user_id = user['id']
        print(str(user_id))
        assoc_friends_ids = getFriendsIDs(user_id)
        for assoc_friend_id in assoc_friends_ids:
            if add_all:
                if assoc_friend_id not in user_index:
                    user_index[assoc_friend_id] = 0
                    user_info = {
                        'id' : assoc_friend_id,
                        'name' : '',
                        'screen_name' : '',
                        'tweet_count' : '',
                        'follower_count' : '',
                        'friend_count' : '',
                        'location' : '',
                        'profile_image' : '',
                        'banner_image' : '',
                        'language' : '',
                        'time_zone' : '',
                        'utc_offset' : '',
                        'creation_date' : '',
                        'protected' : ''
                    }
                    if storage:
                        storeUser(user_info, storage)
                if storage:
                    storeConnection(user_id, assoc_friend_id, storage)
            else:
                if assoc_friend_id in user_index:
                    print(str(assoc_friend_id) + ' in index')
                    if storage:
                        storeConnection(user_id, assoc_friend_id, storage)

        user_index[user_id] = 1
        with open('csv/index.csv', 'a', encoding = 'utf8') as index_csv:
            index_writer = csv.writer(index_csv, delimiter=',')
            index_row = [str(user_id)]
            index_writer.writerow(index_row)

def getEgoNetwork(userid, influencers=True, storage=False):
    global user_index
    #index user
    if userid not in user_index:
        getUserInfo(userid, 0, False, storage)

    friends = getFriends(userid, 1, influencers, storage)
    print('Friend Unsorted: ' + friends[0]['screen_name'])
    sort_friends = sorted(friends, key=lambda k: k['follower_count'], reverse=True)
    print('Friend Sorted: ' + sort_friends[0]['screen_name'])
    #followers = getFollowers(userid, 1, influencers, storage)
    #print('Follower Unsorted: ' + followers[0]['screen_name'])
    #sort_followers = sorted(followers, key=lambda k: k['follower_count'], reverse=True)
    #print('Follower Sorted: ' + sort_followers[0]['screen_name'])
    #total = len(friends) + len(followers)
    total = len(friends)
    count = 0
    for friend in sort_friends:
        count += 1
        print(str(count) + ' of ' + str(total))
        friend_id = friend['id']
        assoc_friends_ids = getFriendsIDs(friend_id)
        for assoc_friend_id in assoc_friends_ids:
            if assoc_friend_id in user_index:
                print(str(assoc_friend_id) + ' in index')
                if storage:
                    storeConnection(friend_id, assoc_friend_id, storage)
            #else:
            #    print(str(assoc_friend_id) + ' NOT in index')

        #assoc_followers_ids = getFollowersIDs(friend_id)
        #for assoc_follower_id in assoc_followers_ids:
        #    if assoc_follower_id in user_index:
        #        print(str(assoc_follower_id) + ' in index')
        #        if storage:
        #            storeConnection(assoc_follower_id, friend_id, storage)
            #else:
            #    print(str(assoc_follower_id) + ' NOT in index')


    #for follower in sort_followers:
    #    count += 1
    #    print(str(count) + ' of ' + str(total))
    #    follower_id = follower['id']
    #    assoc_friends_ids = getFriendsIDs(follower_id)
    #    for assoc_friend_id in assoc_friends_ids:
    #        if assoc_friend_id in user_index:
    #            print(str(assoc_friend_id) + ' in index')

    #            if storage:
    #                storeConnection(follower_id, assoc_friend_id, storage)
            #else:
            #    print(str(assoc_friend_id) + ' NOT in index')

        #assoc_followers_ids = getFollowersIDs(follower_id)
        #for assoc_follower_id in assoc_followers_ids:
        #    if assoc_follower_id in user_index:
        #        print(str(assoc_follower_id) + ' in index')
        #        if storage:
        #            storeConnection(assoc_follower_id, follower_id, storage)
            #else:
            #    print(str(assoc_follower_id) + ' NOT in index')

def crawl(userid, influencers=False, storage=False):
    global user_index
    print('Crawling ' + str(userid) + '...')
    #index user
    if userid not in user_index:
        getUserInfo(userid, 0, False, 'csv')

    if user_index[userid] != 1:
        #print('User ' + str(userid) + ' has not been crawled...')
        getFriends(userid, 500, influencers, storage)

        if not influencers:
            getFollowers(userid, 500, influencers, storage)

        user_index[userid] = 1
        with open('csv/index.csv', 'a', encoding = 'utf8') as index_csv:
            index_writer = csv.writer(index_csv, delimiter=',')
            index_row = [str(userid)]
            index_writer.writerow(index_row)

        print('Getting next user...')
        #Get unindexed user
        new_user = list(user_index.keys())[list(user_index.values()).index(0)]
        print('New user: ' + str(new_user))
        crawl(new_user, True, 'csv')

    else:
        print('Skipping user ' + str(userid) + '...')
        #Get unindexed user
        new_user = list(user_index.keys())[list(user_index.values()).index(0)]
        print('New user: ' + str(new_user))
        crawl(new_user, True, 'csv')


createCSVs()
loadIndex()
#user_id = getUserID('RichardBSpencer')
#user_id = getUserID('PanopticLabs')
#crawl(user_id, True, 'csv')
#crawl(4469439315, True, 'csv')

#getEgoNetwork(user_id, False, 'csv')

userlist = load_and_sort('Alt-Right.csv')
getAssocIDs(userlist, True, 0, 'csv')
assoclist = []
for user_id in user_index:
    if user_index[user_id] == 0:
        assoclist.append({'id' : int(user_id)})

getAssocIDs(assoclist, False, 0, 'csv')
