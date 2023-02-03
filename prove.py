#insert here acedemic token and query info

Token = "AAAAAAAAAAAAAAAAAAAAANUBXwEAAAAAa6704bKg04Ng8LHjAxj2sjEfFkw%3D0FoPLcNmvSO9YBz5LxK3rITpBW11GaQ9tZbX8XQ0467ymtmjPu" #insert here acedemic token

[lat, lon, radius, start_time, end_time]=['3.420556', '-76.522224', '1', '2014-01-01T00:00:00.00Z', '2015-01-12T00:00:00.00Z']

#import modules

import requests
import pandas as pd
import time
import configparser

#define functions

def getTwitterPost(lat, lon, radius, start_time, end_time):
    query='point_radius:[' + lon + ' ' + lat + ' ' + radius + 'km]'
    baseUrl = "https://api.twitter.com/2/tweets/search/all?max_results=500&tweet.fields=geo&expansions=attachments.media_keys&query=has:geo "+query+"&start_time="+start_time+"&end_time="+end_time
    headers = {"Authorization": "Bearer "+Token}
    tweets=[]
    print(baseUrl)

    # we do first call to get first next_token
    resp = requests.get(
        baseUrl, headers=headers).json()

    data = []
    if('data' in resp):
        data = resp['data']
        tweets = _mergeTweets(tweets, data)
    #print(data)

    # we iterate until no next_token (end of pagination)
    if 'meta' in resp and 'next_token' in resp['meta']:
        next_token = resp['meta']['next_token']
        while True:
            time.sleep(3) #api rate limit 300 per 15 min
            resp = requests.get(baseUrl+"&next_token="+next_token, headers=headers).json()
            if('data' in resp):
                data = resp['data']
                tweets = _mergeTweets(tweets, data)
                if('meta' in resp and 'next_token' in resp['meta']):
                    next_token = resp['meta']['next_token']
                else:
                    break

    df = pd.DataFrame(tweets)
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['date'] = df['created_at'].dt.date
    df['time'] = df['created_at'].dt.time
    df['username'] = df['author_id'].apply(lambda x: x['username'])
    df['gender'] = df['author_id'].apply(lambda x: x['gender'] if 'gender' in x else None)
    df.to_csv('2014calitweets.csv', index=False)


    # split data in two dataframe: one with lat e long and one without
    print('print df')
    print(df.columns)
    print(df)
    dfNOLatLon = df[df['longitude']=='']
    print('\n\nprint df no lat no lon')
    print(dfNOLatLon)
    dfLatLon = df[df['longitude']!='']
    print('\n\nprint df si lat si lon')
    print(dfLatLon)

    # create a distinct of place_ids to find use 1.1 api to get lat and long
    placesToFind=list(set(dfNOLatLon['place_id'].to_list()))
    print('placesToFind')
    print(placesToFind)
    dfPlace = findLatLon(placesToFind, headers)
    print('print df Places')
    print(dfPlace)

    # drop empty columns and join with dataframe without lat and log with dataframe with informations
    dfNOLatLon.drop(columns=['longitude', 'latitude'], inplace=True)
    print(dfNOLatLon.columns)
    print(dfNOLatLon)
    dfNOLatLon=dfNOLatLon.merge(dfPlace, on='place_id', how='left')
    print(dfNOLatLon.columns)
    print(dfNOLatLon)

    # concat and export to csv file
    dfFinal=pd.concat([dfLatLon, dfNOLatLon])
    print(dfFinal)
    dfFinal.to_csv('2014calitweets.csv')

# extract data from response and concat with stored tweets
def _mergeTweets(tweets, data):
    for el in data:
        tweet = {}
        try:
            tweet['created_at'] = el['created_at']
        except:
            tweet['created_at']=''
        try:
            tweet['text']=el['text']
        except:
            tweet['text']=''
        #sometimes has:geo return post with only place_id we assigned empty string to filter in second step
        if('geo' in el and 'coordinates' in el['geo'] and el['geo']['coordinates']['type'] == 'Point' and 'place_id' in el['geo']):
            tweet['longitude'] = str(
                el['geo']['coordinates']['coordinates'][0])
            tweet['latitude'] = str(el['geo']['coordinates']['coordinates'][1])
            tweet['place_id'] = el['geo']['place_id']
        elif('geo' in el and 'place_id' in el['geo']):
            tweet['place_id'] = el['geo']['place_id']
            tweet['longitude'] = ''
            tweet['latitude'] = ''
        tweets.append(tweet)
    return tweets

### create dataframe with place_id, latitude and longitude using twitter 1.1 api from list of place_id
def findLatLon(places, headers):
    placeList=[]
    baseUrl = "https://api.twitter.com/1.1/geo/id/"
    print(places)
    step = int(70)
    print(len(places))
    for i in range(0, len(places), step):
        print(i)
        placesInfo = [requests.get(baseUrl+id+".json", headers=headers).json() for id in places[i*step:(i+1)*step]]
        for place in placesInfo:
            if('geometry' in place and 'type' in place['geometry'] and place['geometry']['type'] == "Point"):
                placeList.append({'place_id': place['id'], 'longitude': place['geometry']['coordinates'][0], 'latitude': place['geometry']['coordinates'][1]})
            else:
                #sometimes it return a polygon so we use centroid coordinates
                placeList.append(
                    {'place_id': place['id'], 'longitude': place['centroid'][0], 'latitude': place['centroid'][1]})
        if len(places) > (i+1)*step:
            time.sleep(900) #api rate limit 70 per 15 mins
    return pd.DataFrame(placeList)

getTwitterPost(lat, lon, radius, start_time, end_time)
