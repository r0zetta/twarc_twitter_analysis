# -*- coding: utf-8 -*-
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy import Cursor
from datetime import datetime, date, time, timedelta
from authentication_keys import get_account_credentials
import numpy as np
import pygal
import os.path
import requests
import shutil
import random
import json
import time
import sys
import re
import os
import io

# Add a way to figure out if the account "never sleeps"
# Total up the columns in heatmap. If the total is less than a specified amount, consider that "sleep"

count = 1
data = {}
target = "@r0zetta"
output_dir = "captures/users/"

def strip_crap(text):
    if len(text) < 1:
        return
    t = text
    fl = text[0]
    if '#' in fl or '@' in fl or '.' in fl or ':' in fl or ',' in fl or '\"' in fl or '&' in fl or '\'' in fl or '\`' in fl or '(' in fl or ')' in fl or ')' in fl:
        t = t[1:]
    fl = text[-1]
    if '#' in fl or '@' in fl or '.' in fl or ':' in fl or ',' in fl or '\"' in fl or '&' in fl or '\'' in fl or '\`' in fl or '(' in fl or ')' in fl or ')' in fl:
        t = t[:-1]
    return t

def tokenize(text):
    url_match = "^(https?:\/\/)[0-9a-zA-Z]+\.[-_0-9a-zA-Z]+\.?[0-9a-zA-Z]*\/?.*$"
    tokens = text.split()
    ret = []
    for t in tokens:
        if t is not None and len(t) > 3:
            t = t.lower()
            changed = True
            while changed is True:
                new = strip_crap(t)
                if new != t:
                    changed = True
                else:
                    changed = False
                t = new
                if len(t) < 1:
                    break
            if len(t) < 3:
                continue
            elif u"&amp;" in t or u"…" in t or u"htt" in t: 
                continue
            elif re.search(u"^[0-9\.\,%]+$", t):
                continue
            elif re.search(u"\s+", t):
                continue
            elif re.search(u"[\.\,\:\;\?\-\_\!]+", t):
                continue
            elif re.search(u"^rt$", t):
                continue
            elif re.search(url_match, t) is not None:
                continue
            else:
                if len(t) > 0:
                    ret.append(t)
    return ret

def strip_stopwords(raw_data, lang):
    global stopwords
    ret = []
    if lang not in stopwords:
        return raw_data
    for word in raw_data:
        if word not in stopwords[lang]:
            ret.append(word)
    return ret

def time_string_to_object(time_string):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')

def time_object_to_string(time_object):
    return datetime.strftime(time_object, '%Y-%m-%d %H:%M:%S')

def time_object_to_month(time_object):
    return datetime.strftime(time_object, '%Y-%m')

def time_object_to_week(time_object):
    return datetime.strftime(time_object, '%Y-%U')

def time_object_to_day(time_object):
    return datetime.strftime(time_object, '%Y-%m-%d')

def time_object_to_hour(time_object):
    return datetime.strftime(time_object, '%Y-%m-%d-%H')

def increment_counter(label, name):
    global data
    if label not in data:
        data[label] = {}
    if name not in data[label]:
        data[label][name] = 0
    data[label][name] += 1

def output_data():
    output_string = ""
    for label, stuff in sorted(data.iteritems()):
        m = re.search("^per_\w+$", label)
        if m is None:
            output_string += u"\n" + label.encode('utf-8') + u":\n\n"
            for item, count in sorted(stuff.iteritems()):
                output_string += unicode(count) + u": " + unicode(item) + u"\n"
    return output_string

def output_top_data():
    output_string = ""
    for label, stuff in sorted(data.iteritems()):
        m = re.search("^per_\w+$", label)
        if m is None:
            output_string += u"\n" + label.encode('utf-8') + u":\n\n"
            output_count = 0
            for item, count in sorted(stuff.iteritems(), key=lambda x:x[1], reverse=True):
                if count > 1:
                    output_string += unicode(count) + u": " + unicode(item) + u"\n"
                output_count += 1
                if output_count > 10:
                    break
    return output_string

def dump_pie_chart(dirname, filename, title, chart_data):
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    filepath = dirname + filename
    total = 0
    for n, c in chart_data.iteritems():
        total += c
    pie_chart = pygal.Pie(truncate_legend=-1)
    pie_chart.title = title
    output_count = 0
    for n, c in sorted(chart_data.iteritems(), key=lambda x:x[1], reverse = True):
        percent = float(float(c)/float(total))*100.00
        label = n + " (" + "%.2f" % percent + "%)"
        pie_chart.add(label, c)
        output_count += 1
        if output_count > 15:
            break
    pie_chart.render_to_file(filepath)

def dump_line_chart(dirname, filename, title, x_labels, chart_data):
    if len(x_labels) < 5:
        return
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    filepath = dirname + filename
    chart = pygal.Line(show_y_guides=True, show_dots=False, x_labels_major_count=5, show_minor_x_labels=False, show_minor_y_labels=False, x_label_rotation=20)
    chart.title = title
    chart.x_labels = x_labels
    for name, stuff in chart_data.iteritems():
        chart.add(name, stuff)
    chart.render_to_file(filepath)

def dump_bar_chart(dirname, filename, title, x_labels, chart_data):
    if len(x_labels) < 5:
        return
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    filepath = dirname + filename
    chart = pygal.Bar(show_y_guides=True, x_labels_major_count=5, show_minor_x_labels=False, show_minor_y_labels=False, x_label_rotation=20)
    chart.title = title
    chart.x_labels = x_labels
    for name, stuff in chart_data.iteritems():
        chart.add(name, stuff)
    chart.render_to_file(filepath)

def dump_chronology():
    global data
    types = ["per_hour", "per_day", "per_week", "per_month"]
    labels = ["own_tweets", "retweets", "all_tweets", "replies"]
    dirname = output_dir
    for t in types:
        for l in labels:
            x_axis_labels = []
            plot_data = {}
            title = t + "_" + l
            y_axis_label = "tweets/minute"
            filename = title + ".svg"
            if title in data:
                plot_data[l] = []
                seen = 0
                for name, count in sorted(data[title].items(), key=lambda x:x[0], reverse=False):
                    x_axis_labels.append(name)
                    plot_data[l].append(count)
                    seen += 1
                if seen > 100:
                    dump_line_chart(dirname, filename, title, x_axis_labels, plot_data)
                else:
                    dump_bar_chart(dirname, filename, title, x_axis_labels, plot_data)

def record_chronology(label, tweet_time):
    global data
    timestamps = {}
    timestamps["per_hour"] = time_object_to_hour(tweet_time)
    timestamps["per_day"] = time_object_to_day(tweet_time)
    timestamps["per_week"] = time_object_to_week(tweet_time)
    timestamps["per_month"] = time_object_to_month(tweet_time)
    types = ["per_hour", "per_day", "per_week", "per_month"]
    for t in types:
        l = t + "_" + label
        if l not in data:
            data[l] = {}
        if timestamps[t] not in data[l]:
            data[l][timestamps[t]] = 1
        else:
            data[l][timestamps[t]] += 1

def create_blocks(id_list):
    id_count = 0
    block_count = 0
    id_blocks = {}
    print "List had " + str(len(id_list)) + " items."
    for twid in id_list:
        if block_count in id_blocks:
            id_blocks[block_count].append(twid)
        else:
            id_blocks[block_count] = []
            id_blocks[block_count].append(twid)
        id_count += 1
        if id_count >= 99:
            id_count = 0
            block_count += 1
    print "Found " + str(block_count) + " data blocks."
    return id_blocks

def query_blocks(id_blocks):
    details = []
    block_count = len(id_blocks.items())
    print "Found " + str(block_count) + " blocks."
    for block, data in id_blocks.iteritems():
        print "Iterating block:" + str(block) + "/" + str(block_count)
        users_list = auth_api.lookup_users(user_ids=data)
        for item in users_list:
            screen_name = item.screen_name
            description = item.description.replace('\n', ' ').replace("\t", " ")
            location = item.location
            if screen_name is not None and screen_name != "":
                entry = {}
                entry["name"] = screen_name
                entry["desc"] = description
                if entry["desc"] == "":
                    entry["desc"] = "[no description]"
                entry["location"] = location
                if entry["location"] == "":
                    entry["location"] = "[no location]"
                details.append(entry)
    return details

def print_user_list_details(filename, details):
    handle = io.open(filename, "w", encoding='utf-8')
    handle.write(u"Name\tDescription\tLocation\n")
    for item in details:
        name = item["name"]
        desc = item["desc"]
        location = item["location"]
        handle.write(unicode(name) + u"\t" + unicode(desc) + "\t" + unicode(location) + "\n")
    handle.close


def get_user_list_details(id_list):
    id_blocks = create_blocks(id_list)
    details = query_blocks(id_blocks)
    return details

if __name__ == '__main__':
    acct_name, consumer_key, consumer_secret, access_token, access_token_secret = get_account_credentials()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    auth_api = API(auth)

    if (len(sys.argv) > 1):
            target = str(sys.argv[1])

    print "Signing in as: " + auth_api.me().name
    print "Target: " + target.encode('utf-8')

    if os.path.exists("corpus/stopwords-iso.json"):
        handle = open("corpus/stopwords-iso.json", "r")
        stopwords = json.load(handle)
        handle.close()

    tweet_count = 0

    heatmap = []
    interarrivals = {}
    tweet_texts = []
    tweet_info = []
    sources = {}

    heatmap = [[0 for j in range(24)] for i in range(7)]

    item = auth_api.get_user(target)
# Get year and month of account creation
    name = item.name
    screen_name = item.screen_name
    user_id = item.id_str
    tweets = item.statuses_count
    likes = item.favourites_count
    lists = item.listed_count
    following_count = item.friends_count
    followers_count = item.followers_count
    description = item.description
    account_created_date = item.created_at
    account_created_date_readable = time_object_to_string(account_created_date)

    delta = datetime.utcnow() - account_created_date
    account_age_days = delta.days
    tweets_per_day = 0
    tweets_per_hour = 0
    if account_age_days > 0:
        tweets_per_day = float(tweets)/float(account_age_days)
        tweets_per_hour = float(tweets)/float(account_age_days * 24)

    previous_tweet_time = None
    retweets = 0
    replies = 0
    quote_tweets = 0
    own_tweets = 0
    pictures = []
    for status in Cursor(auth_api.user_timeline, id=target).items():
        tweet_count = tweet_count + 1

# create heatmap
        tweet_time = status.created_at
        weekday = tweet_time.weekday()
        hour = tweet_time.hour
        heatmap[weekday][hour] = heatmap[weekday][hour] + 1

# get interarrival map
        if previous_tweet_time is not None:
            delta = previous_tweet_time - tweet_time
            delta_seconds = int(delta.total_seconds())
            if delta_seconds not in interarrivals:
                interarrivals[delta_seconds] = 1
            else:
                interarrivals[delta_seconds] += 1
        previous_tweet_time = tweet_time

# get all tweet texts
        date_string = time_object_to_string(tweet_time)
        text = status.text
        text = text.replace('\n', ' ').replace('\r', '')
        entry = date_string + " | " + status.text + "\n"
        details = {}
        details["text"] = text
        details["source"] = status.source
        tweet_texts.append(entry)
        tweet_info.append(details)

        replied = False
        replied_user = ""
        if hasattr(status, 'in_reply_to_screen_name'):
            if status.in_reply_to_screen_name is not None:
                replied_user = status.in_reply_to_screen_name
                increment_counter("replied_to", status.in_reply_to_screen_name)
                replies += 1
                replied = True

        lang = ""
        if hasattr(status, 'lang'):
            lang = status.lang
            increment_counter("languages", status.lang)

        retweeted = False
        retweeted_user = ""
        if hasattr(status, 'retweeted_status'):
            orig_tweet = status.retweeted_status
            if hasattr(orig_tweet, 'user'):
                if orig_tweet.user is not None:
                    if hasattr(orig_tweet.user, "screen_name"):
                        if orig_tweet.user.screen_name is not None:
                            retweeted_user = orig_tweet.user.screen_name
                            increment_counter("retweeted", retweeted_user)
                            retweeted = True

        quoted_user = ""
        if hasattr(status, 'quoted_status'):
            orig_tweet = status.quoted_status
            if 'user' in orig_tweet:
                if orig_tweet['user'] is not None:
                    if "screen_name" in orig_tweet['user']:
                        if orig_tweet['user']['screen_name'] is not None:
                            quoted_user = orig_tweet['user']['screen_name']
                            increment_counter("quoted", quoted_user)
                            quote_tweets += 1

        if replied is True:
            record_chronology("replies", tweet_time)

        if retweeted is True:
            retweets += 1
            record_chronology("retweets", tweet_time)
        else:
            own_tweets += 1
            record_chronology("own_tweets", tweet_time)

        record_chronology("all_tweets", tweet_time)


        if hasattr(status, 'entities'):
            entities = status.entities
            if 'hashtags' in entities:
                for item in entities['hashtags']:
                    if item is not None:
                        tag = item['text']
                        if tag is not None:
                            if retweeted is True:
                                increment_counter("retweeted_hashtags", tag.lower())
                            else:
                                increment_counter("hashtags", tag.lower())
            if 'urls' in entities:
                for item in entities['urls']:
                    if item is not None:
                        url = item['expanded_url']
                        if url is not None:
                            if retweeted is True:
                                increment_counter("retweeted_urls", url)
                            else:
                                increment_counter("urls", url)
            if 'user_mentions' in entities:
                for item in entities['user_mentions']:
                    if item is not None:
                        mention = item['screen_name']
                        if mention is not None:
                            if mention != replied_user and mention != quoted_user:
                                if retweeted is True:
                                    increment_counter("retweeted_mentions", mention)
                                else:
                                    increment_counter("mentions", mention)
            if "media" in entities:
                for item in entities["media"]:
                    if item is not None:
                        if "media_url" in item:
                            murl = item["media_url"]
                            if murl not in pictures:
                                pictures.append(murl)


# get user agents
        increment_counter("sources", status.source)

        tokens = strip_stopwords(tokenize(text), lang)
        for t in tokens:
            increment_counter("words", t)

        sys.stdout.write("#")
        sys.stdout.flush()

    print "Getting followers for " + target
    followers = auth_api.followers_ids(target)
    follower_details = get_user_list_details(followers)

    print "Getting followed for " + target
    friends = auth_api.friends_ids(target)
    friend_details = get_user_list_details(friends)

    reciprocal_names = []
    follower_names = []
    friend_names = []
    for item in follower_details:
        if item["name"] not in follower_names:
            follower_names.append(item["name"])
    for item in friend_details:
        if item["name"] not in friend_names:
            friend_names.append(item["name"])
    for n in follower_names:
        if n in friend_names:
            if n not in reciprocal_names:
                reciprocal_names.append(n)
    print
    print "All done. Processed " + str(tweet_count) + " tweets."
    print

    output_dir += target.encode('utf-8') + "/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    filename = output_dir + target.encode('utf-8') + "-followers.txt"
    print_user_list_details(filename, follower_details)

    filename = output_dir + target.encode('utf-8') + "-following.txt"
    print_user_list_details(filename, friend_details)

    filename = output_dir + target.encode('utf-8') + "-reciprocal.txt"
    handle = open(filename, "w")
    for n in reciprocal_names:
        handle.write(n + "\n")
    handle.close()

    filename = output_dir + target.encode('utf-8') + "-interarrivals.txt"
    print "Writing file: " + filename
    handle = open(filename, 'w')
    std = np.std(interarrivals.values())
    handle.write("Standard deviation: " + str(std) + "\n")
    for key in sorted(interarrivals.iterkeys()):
        outstring = str(key) + " | " + str(interarrivals[key]) + "\n"
        handle.write(outstring.encode('utf-8'))
    handle.close()

    filename = output_dir + target.encode('utf-8') + "-tweets.txt"
    print "Writing file: " + filename
    handle = open(filename, 'w')
    for text in tweet_texts:
        handle.write(text.encode('utf-8'))
    handle.close()

    #filename = output_dir + target.encode('utf-8') + "-details.txt"
    #print "Writing file: " + filename
    #handle = io.open(filename, 'w', encoding='utf-8')
    #for entry in tweet_info:
        #t = entry["text"]
        #s = entry["source"]
        #handle.write(s.encode('utf-8') + ":\t" + t.encode('utf-8') + "\n")
    #handle.close()

    filename = output_dir + target.encode('utf-8') + "-heatmap.csv"
    print "Writing file: " + filename
    handle = open(filename, 'w')
    handle.write("Hour, 00, 01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23\n")
    handle.write("Mon, " + ','.join(map(str, heatmap[0])) + "\n")
    handle.write("Tue, " + ','.join(map(str, heatmap[1])) + "\n")
    handle.write("Wed, " + ','.join(map(str, heatmap[2])) + "\n")
    handle.write("Thu, " + ','.join(map(str, heatmap[3])) + "\n")
    handle.write("Fri, " + ','.join(map(str, heatmap[4])) + "\n")
    handle.write("Sat, " + ','.join(map(str, heatmap[5])) + "\n")
    handle.write("Sun, " + ','.join(map(str, heatmap[6])) + "\n")
    handle.close()

    filename = output_dir + target.encode('utf-8') + "-digest.txt"
    print "Writing file: " + filename
    handle = io.open(filename, 'w', encoding='utf-8')
#    handle.write(u"User name: " + name.encode('utf-8') + u"\n")
    handle.write(u"Screen name: @" + screen_name.encode('utf-8') + u"\n")
    handle.write(u"User id: " + unicode(user_id) + u"\n")
    handle.write(u"Tweets: " + unicode(tweets) + u"\n")
    handle.write(u"Likes: " + unicode(likes) + u"\n")
    handle.write(u"Lists: " + unicode(lists) + u"\n")
    handle.write(u"Following: " + unicode(following_count) + u"\n")
    handle.write(u"Followers: " + unicode(followers_count) + u"\n")
    handle.write(u"Created: " + unicode(account_created_date_readable) + u"\n")
    handle.write(u"Description: " + unicode(description) + u"\n")
    handle.write(u"Tweets per hour: " + unicode(tweets_per_hour) + u"\n")
    handle.write(u"Tweets per day: " + unicode(tweets_per_day) + u"\n")
    handle.write(u"Tweets analyzed: " + unicode(tweet_count) + u"\n")
    handle.write(u"Retweets: " + unicode(retweets) + u"\n")
    handle.write(u"Quote tweets: " + unicode(quote_tweets) + u"\n")
    handle.write(u"Own tweets: " + unicode(own_tweets) + u"\n")
    handle.write(u"Replies: " + unicode(replies) + u"\n")
    data_string = output_top_data()
    handle.write(data_string)
    handle.close()

    filename = output_dir + target.encode('utf-8') + "-full.txt"
    print "Writing file: " + filename
    handle = io.open(filename, 'w', encoding='utf-8')
    data_string = output_data()
    handle.write(data_string)
    handle.close()

    dump_chronology()

    pictures_dir = os.path.join(output_dir, "images")
    if not os.path.exists(pictures_dir):
        os.makedirs(pictures_dir)
    for p in pictures:
        m = re.search("^http:\/\/pbs\.twimg\.com\/media\/(.+)$", p)
        if m is not None:
            filename = m.group(1)
            print("Getting picture from: " + p)
            save_path = os.path.join(pictures_dir, filename)
            response = requests.get(p, stream=True)
            with open(save_path, 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
            del response


