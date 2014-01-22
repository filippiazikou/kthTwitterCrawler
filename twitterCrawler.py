###
# KTH - Royal Institute of Technology
# Twitter Crawler
# Filippia Zikou
#
# Reference: https://github.com/pooria121/TwitterCrawler
#
###


#!/usr/bin/python
# -*- coding: utf-8 -*-
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy.parsers import *
from tweepy import Stream, API
import json
import tweepy
from pymongo import MongoClient
import argparse
import ConfigParser
import sys


# Reading the config file
config = ConfigParser.RawConfigParser()
config.read('crawler.cfg')

# Reading Twitter API settings from config file
consumer_key = config.get('twitter', 'consumer_key')
consumer_secret = config.get('twitter', 'consumer_secret')
access_token = config.get('twitter', 'access_token')
access_secret = config.get('twitter', 'access_secret')
language = config.get('twitter', 'language')


#Reading database settings
client = MongoClient()
db = client[config.get('mongodb', 'db')]
posts = db[config.get('mongodb', 'schema')]


class TweetStreamListener(StreamListener):
    """ A listener that handles the  recived tweets
        using on_status method"""

    def save(self, status):
        user_json = status.user.__getstate__()
        user_json['created_at'] = str(status.user.created_at)
        user_json['type'] = 'user'
        place_json = None
        if status.place != None:
            place_json = status.place.__getstate__()
            if status.place.bounding_box != None:
                place_json['bounding_box'] = \
                    status.place.bounding_box.__getstate__()

        doc = {
            'type': 'Tweet',
            'contributors': status.contributors,
            'coordinates': status.coordinates,
            'created_at': str(status.created_at),
            'entities': status.entities,
            'favorite_count': status.favorite_count,
            'favorited': status.favorited,
            'geo': status.geo,
            'id': status.id,
            'id_str': status.id_str,
            'in_reply_to_screen_name': status.in_reply_to_screen_name,
            'in_reply_to_status_id': status.in_reply_to_status_id,
            'in_reply_to_status_id_str': status.in_reply_to_status_id_str,
            'in_reply_to_user_id': status.in_reply_to_user_id,
            'in_reply_to_user_id_str': status.in_reply_to_user_id_str,
            'lang': status.lang,
            'place': place_json,
            'retweet_count': status.retweet_count,
            'retweeted': status.retweeted,
            'source': status.source,
            'source_url': status.source_url,
            'text': status.text,
            'truncated': status.truncated,
            'user': user_json,
            }

                    # print doc
                    # print status.user.__getstate__()

        print doc['id']
        posts.insert(doc)
        sys.stdout.flush()

    def on_status(self, status):
        """this method will handle and parse recieved tweets"""

        found = posts.find_one({'id': status.id})

        if hasattr(status, 'lang') and status.lang == language \
            and found == None:
            self.save(status)
        return True



def sample():
    '''Get random sample of all public statuses.'''
    listener = TweetStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = Stream(auth, listener)
    stream.sample()

def location(coordinates):
    '''Get tweets within a specific area. Coordinates: 
    <southwestLongitude> <southwestLatitude> <northeastLongitude> <northeastLatitude>'''
    listener = TweetStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = Stream(auth, listener)
    stream.filter(locations=coordinates)

def track(words):
    '''Get tweets that include the keywords specified by the user: 
    <word1> <word2>..'''
    listener = TweetStreamListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = Stream(auth, listener)
    stream.filter(track=words)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Twitter Crawler')
	parser.add_argument('--location', nargs='*', type=float, help='Getting stream of tweets according to location: <southwestLongitude> <southwestLatitude> <northeastLongitude> <northeastLatitude>')
	parser.add_argument('--track', type=str, help='Getting stream of tweets tracking the keywords from the given file')
	parser.add_argument('--db', type=str, help='Choose the name of collection for database Twitter. Default: tweets')
	args = parser.parse_args()
	if args.db:
		posts = db[args.db]
	if args.location:
		location(args.location)
	elif args.track:
		with open(args.track, 'r') as f:
			keywords = [line.strip() for line in f]
		track(keywords)
	else:
		sample()
	
