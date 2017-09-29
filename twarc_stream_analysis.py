# -*- coding: utf-8 -*-
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from twarc import Twarc
from authentication_keys import get_account_credentials
from datetime import datetime, date, time, timedelta
import pygal
import numpy as np
import Queue
import threading
import sys
import traceback
import time
import os
import json
import io
import re
import shutil
import base64
import hashlib
import string

# record retweets of tweets made by suspicious accounts
# Count accounts that make suspicious retweets
# Record actual tweet URLs

##################
# Global variables
##################
stopping = False
restart = False
threaded = True
debug = False
test = False
collect_only = False
exit_correctly = False
searches = []
tweet_queue = None
analyzer = None
targets = []
data = {}
conf = {}
acct_name = ""
script_start_time_str = ""

def init_params():
    global conf
    conf["params"] = {}
    conf["params"]["default_dump_interval"] = 60
    conf["params"]["config_reload_interval"] = 5
    conf["params"]["serialization_interval"] = 900
    conf["params"]["graph_dump_interval"] = 60
    conf["params"]["min_top_score"] = 10
    conf["params"]["min_tweets_for_suspicious"] = 10
    conf["params"]["data_handling"] = "purge"
    conf["params"]["purge_interval"] = 300
    conf["params"]["retweet_spike_window"] = 120
    conf["params"]["retweet_spike_minimum"] = 100
    conf["params"]["retweet_spike_per_second_minimum"] = 0.4
    conf["params"]["tweet_spike_minimum"] = 1.8
    conf["params"]["time_to_live"] = 8 * 60 * 60
    conf["params"]["max_to_output"] = 250
    if test is True:
        conf["params"]["graph_dump_interval"] = 15
        conf["params"]["serialization_interval"] = 60
        conf["params"]["purge_interval"] = 30
        conf["params"]["time_to_live"] = 60
    return

def init_config():
    global conf
    conf["config"] = {}
    conf["config"]["log_words"] = True
    conf["config"]["log_network_data"] = True
    conf["config"]["log_all_userinfo"] = False
    conf["config"]["log_per_hour_data"] = False
    conf["config"]["log_user_data"] = False
    conf["config"]["log_metadata"] = False
    conf["config"]["log_interarrivals"] = True
    conf["config"]["log_timeline_data"] = False
    conf["config"]["dump_raw_data"] = False
    conf["config"]["dump_graphs"] = True
    conf["config"]["sanitize_text"] = False
    return

#################
# In-mem storage
#################
def increment_storage(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = False
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if name not in data[variable][category]:
        data[variable][category][name] = 1
        ret = True
    else:
        data[variable][category][name] += 1
    return ret

def decrement_storage(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = False
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if name not in data[variable][category]:
        data[variable][category][name] = 1
        ret = True
    else:
        data[variable][category][name] -= 1
    return ret

def add_to_storage(variable, category, name, value):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = False
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if name not in data[variable][category]:
        data[variable][category][name] = 1
        ret = True
    else:
        data[variable][category][name] += value
    return ret

def get_storage(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    if variable in data:
        if category in data[variable]:
            if name in data[variable][category]:
                return data[variable][category][name]

def exists_storage(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    ret = False
    if variable in data:
        if category in data[variable]:
            if name in data[variable][category]:
                ret = True
    return ret

def set_storage(variable, category, name, value):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    data[variable][category][name] = value

def del_from_storage(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if variable in data:
        if category in data[variable]:
            if name in data[variable][category]:
                del data[variable][category][name]

def del_category_from_storage(variable, category):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if variable in data:
        if category in data[variable]:
            del data[variable][category]

def get_category_storage(variable, category):
    debug_print(sys._getframe().f_code.co_name)
    if variable in data:
        if category in data[variable]:
            return data[variable][category]

def get_all_storage(variable):
    debug_print(sys._getframe().f_code.co_name)
    if variable in data:
        return data[variable]

def get_all_storage_names(variable, category):
    debug_print(sys._getframe().f_code.co_name)
    ret = []
    if variable in data:
        if category in data[variable]:
            for name, value in data[variable][category].iteritems():
                if name not in ret:
                    ret.append(name)
    return ret

def get_categories_from_storage(variable):
    debug_print(sys._getframe().f_code.co_name)
    ret = []
    if variable in data:
        for cat, stuff in data[variable].iteritems():
            if cat not in ret:
                ret.append(cat)
    return ret

###############
# Large storage
###############
def increment_storage_large(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = False
    fl = get_first_letters(name)
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if fl not in data[variable][category]:
        data[variable][category][fl] = {}
    if name not in data[variable][category][fl]:
        data[variable][category][fl][name] = 1
        ret = True
    else:
        data[variable][category][fl][name] += 1
    return ret

def decrement_storage_large(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = False
    fl = get_first_letters(name)
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if fl not in data[variable][category]:
        data[variable][category][fl] = {}
    if name not in data[variable][category][fl]:
        data[variable][category][fl][name] = 1
        ret = True
    else:
        data[variable][category][fl][name] -= 1
    return ret

def add_to_storage_large(variable, category, name, value):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = False
    fl = get_first_letters(name)
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if fl not in data[variable][category]:
        data[variable][category][fl] = {}
    if name not in data[variable][category][fl]:
        data[variable][category][fl][name] = 1
        ret = True
    else:
        data[variable][category][fl][name] += value
    return ret

def get_storage_large(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    fl = get_first_letters(name)
    if variable in data:
        if category in data[variable]:
            if fl in data[variable][category]:
                if name in data[variable][category][fl]:
                    return data[variable][category][fl][name]

def exists_storage_large(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    ret = False
    fl = get_first_letters(name)
    if variable in data:
        if category in data[variable]:
            if fl in data[variable][category]:
                if name in data[variable][category][fl]:
                    ret = True
    return ret

def set_storage_large(variable, category, name, value):
    debug_print(sys._getframe().f_code.co_name)
    global data
    fl = get_first_letters(name)
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if fl not in data[variable][category]:
        data[variable][category][fl] = {}
    data[variable][category][fl][name] = value

def del_from_storage_large(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    fl = get_first_letters(name)
    if variable in data:
        if category in data[variable]:
            if fl in data[variable][category]:
                if name in data[variable][category][fl]:
                    del data[variable][category][fl][name]

def get_category_storage_large(variable, category):
    debug_print(sys._getframe().f_code.co_name)
    ret = {}
    if variable in data:
        if category in data[variable]:
            for fl, stuff in data[variable][category].iteritems():
                for name, value in stuff.iteritems():
                    ret[name] = value
    return ret

def get_all_storage_large(variable):
    debug_print(sys._getframe().f_code.co_name)
    ret = {}
    cats = get_categories_from_storage(variable)
    for c in cats:
        ret[c] = get_category_storage_large(variable, c)
    return ret

def get_all_storage_names_large(variable, category):
    debug_print(sys._getframe().f_code.co_name)
    ret = []
    if variable in data:
        if category in data[variable]:
            for fl, stuff in data[variable][category]:
                for name, value in stuff.iteritems():
                    if name not in ret:
                        ret.append(name)
    return ret

##############
# List storage
##############
def add_to_list_data_large(variable, category, name, item):
    debug_print(sys._getframe().f_code.co_name)
    global data
    fl = get_first_letters(name)
    ret = False
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if fl not in data[variable][category]:
        data[variable][category][fl] = {}
    if name not in data[variable][category][fl]:
        data[variable][category][fl][name] = []
    if item not in data[variable][category][fl][name]:
        data[variable][category][fl][name].append(item)
        ret = True
    return ret

def get_from_list_data_large(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    fl = get_first_letters(name)
    ret = []
    if variable in data:
        if category in data[variable]:
            if fl in data[variable][category]:
                if name in data[variable][category][fl]:
                    ret = data[variable][category][fl][name]
    return ret

def add_to_list_data(variable, category, name, item):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = False
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if name not in data[variable][category]:
        data[variable][category][name] = []
    if item not in data[variable][category][name]:
        data[variable][category][name].append(item)
        ret = True
    return ret

def get_from_list_data(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = []
    if variable in data:
        if category in data[variable]:
            if name in data[variable][category]:
                ret = data[variable][category][name]
    return ret

#############################
# Custom storage and wrappers
#############################

def record_bot_list(name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if "bot_list" not in data:
        data["bot_list"] = []
    if name not in data["bot_list"]:
        data["bot_list"].append(name)

def get_bot_list():
    debug_print(sys._getframe().f_code.co_name)
    if "bot_list" in data:
        return data["bot_list"]

def dump_bot_list():
    debug_print(sys._getframe().f_code.co_name)
    if "bot_list" in data:
        filename = "data/custom/bots.txt"
        handle = io.open(filename, "w", encoding='utf-8')
        for n in data["bot_list"]:
            handle.write(n + u"\n")
        handle.close

def record_demographic(name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if "demographic" not in data:
        data["demographic"] = []
    if name not in data["demographic"]:
        data["demographic"].append(name)

def get_demographic():
    debug_print(sys._getframe().f_code.co_name)
    if "demographic" in data:
        return data["demographic"]

def dump_demographic_list():
    debug_print(sys._getframe().f_code.co_name)
    if "demographic" in data:
        filename = "data/custom/demographics.txt"
        handle = io.open(filename, "w", encoding='utf-8')
        for n in data["demographic"]:
            handle.write(n + u"\n")
        handle.close

def record_demographic_detail(name, desc_words, tweet_words):
    debug_print(sys._getframe().f_code.co_name)
    desc = []
    tweet = []
    if "demographic_detail" in data:
        if name in data["demographic_detail"]:
            desc = data["demographic_detail"][name]["desc_words"]
            tweet = data["demographic_detail"][name]["tweet_words"]
    else:
        data["demographic_detail"] = {}
    for w in desc_words:
        if w not in desc:
            desc.append(w)
    for w in tweet_words:
        if w not in tweet:
            tweet.append(w)
    data["demographic_detail"][name] = {}
    data["demographic_detail"][name]["desc_words"] = desc
    data["demographic_detail"][name]["tweet_words"] = tweet

def exists_demographic_detail(name):
    debug_print(sys._getframe().f_code.co_name)
    ret = False
    if "demographic_detail" in data:
        if name in data["demographic_detail"]:
            ret = True
    return ret

def get_demographic_detail(name):
    debug_print(sys._getframe().f_code.co_name)
    desc = []
    tweet = []
    if "demographic_detail" in data:
        if name in data["demographic_detail"]:
            desc = data["demographic_detail"][name]["desc_words"]
            tweet = data["demographic_detail"][name]["tweet_words"]
    return desc, tweet

def dump_demographic_detail():
    debug_print(sys._getframe().f_code.co_name)
    if "demographic_detail" in data:
        filename = "data/custom/demographic_detail.txt"
        handle = io.open(filename, "w", encoding='utf-8')
        for name, stuff in data["demographic_detail"].iteritems():
            desc = ""
            tweet = ""
            if len(stuff["desc_words"]) > 0:
                desc = "[" + u", ".join(map(unicode, stuff["desc_words"])) + "]"
            else:
                desc = "[None]"
            if len(stuff["tweet_words"]) > 0:
                tweet = "[" + u", ".join(map(unicode, stuff["tweet_words"])) + "]"
            else:
                tweet = "[None]"
            tweet = stuff["tweet_words"]
            handle.write(unicode(name) + u"\t" + unicode(desc) + u"\t" + unicode(tweet) + u"\n")
        handle.close()


def record_sentiment(label, timestamp, value):
    debug_print(sys._getframe().f_code.co_name)
    sentiment_value = 0
    if exists_counter("sentiment_" + label):
        old_val = get_counter("sentiment_" + label)
        sentiment_value = old_val + value
    else:
        sentiment_value =  value
    set_counter("sentiment_" + label, sentiment_value)
    current_time = int(time.time())
    prev_label = "previous_sentiment_" + label
    last_recorded = get_counter(prev_label)
    if last_recorded is None:
        set_counter(prev_label, current_time)
    else:
        if current_time > int(last_recorded) + 10:
            record_sentiment_volume(label, timestamp, sentiment_value)
            set_counter(prev_label, current_time)

def record_suspicious_retweet(text, url, timestamp, id_str, name, retweeted_name, account_age, followers, tweets, retweet_count):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if "suspicious_retweets" not in data:
        data["suspicious_retweets"] = {}
    if text not in data["suspicious_retweets"]:
        data["suspicious_retweets"][text] = {}
        increment_counter("suspiciously_retweeted")
    if "twtid" not in data["suspicious_retweets"][text]:
        data["suspicious_retweets"][text]["twtid"] = id_str
    if "url" not in data["suspicious_retweets"][text]:
        data["suspicious_retweets"][text]["url"] = url
    if "retweet_count" not in data["suspicious_retweets"][text]:
        data["suspicious_retweets"][text]["retweet_count"] = retweet_count
    if "retweeted_name" not in data["suspicious_retweets"][text]:
        data["suspicious_retweets"][text]["retweeted_name"] = retweeted_name
    if "account_age" not in data["suspicious_retweets"][text]:
        data["suspicious_retweets"][text]["account_age"] = account_age
    if "followers" not in data["suspicious_retweets"][text]:
        data["suspicious_retweets"][text]["followers"] = followers
    if "tweets" not in data["suspicious_retweets"][text]:
        data["suspicious_retweets"][text]["tweets"] = tweets
    if "names" not in data["suspicious_retweets"][text]:
        data["suspicious_retweets"][text]["names"] = []
    if "timestamps" not in data["suspicious_retweets"][text]:
        data["suspicious_retweets"][text]["timestamps"] = []
    if name not in data["suspicious_retweets"][text]["names"]:
        data["suspicious_retweets"][text]["names"].append(name)
    if timestamp not in data["suspicious_retweets"][text]["timestamps"]:
        data["suspicious_retweets"][text]["timestamps"].append(timestamp)

def dump_suspicious_retweets():
    debug_print(sys._getframe().f_code.co_name)
    if "suspicious_retweets" in data:
        filename = "data/custom/suspicious_retweets.txt"
        handle = io.open(filename, "w", encoding='utf-8')
        for tweet, stuff in data["suspicious_retweets"].iteritems():
            handle.write(unicode(tweet) + u"\n")
            handle.write(u"Tweet ID: " + unicode(stuff["twtid"]) + "\n")
            handle.write(u"URL: " + unicode(stuff["url"]) + "\n")
            handle.write(u"Retweeted: " + unicode(stuff["retweeted_name"]) + "\n")
            handle.write(u"Retweet count: " + unicode(stuff["retweet_count"]) + "\n")
            handle.write(u"Account age: " + unicode("%.2f"%float(stuff["account_age"]/(60*60*24))) + " days\n")
            handle.write(u"Followers: " + unicode(stuff["followers"]) + "\n")
            handle.write(u"Tweets: " + unicode(stuff["tweets"]) + "\n")
            handle.write(u"Names:\n")
            handle.write(u", ".join(map(unicode, stuff["names"])) + "\n")
            handle.write(u"\n")
        handle.close()

def record_retweet_frequency(text, url, timestamp, id_str, name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if "retweet_frequency" not in data:
        data["retweet_frequency"] = {}
    if "first_seen_retweet" not in data["retweet_frequency"]:
        data["retweet_frequency"]["first_seen_retweet"] = {}
    if text not in data["retweet_frequency"]["first_seen_retweet"]:
        data["retweet_frequency"]["first_seen_retweet"][text] = timestamp
    if "previous_seen_retweet" not in data["retweet_frequency"]:
        data["retweet_frequency"]["previous_seen_retweet"] = {}
    data["retweet_frequency"]["previous_seen_retweet"][text] = timestamp
    if "retweet_counter" not in data["retweet_frequency"]:
        data["retweet_frequency"]["retweet_counter"] = {}
    if text not in data["retweet_frequency"]["retweet_counter"]:
        data["retweet_frequency"]["retweet_counter"][text] = 1
        increment_counter("tracked_retweets")
    else:
        data["retweet_frequency"]["retweet_counter"][text] += 1
    if "retweet_metadata" not in data["retweet_frequency"]:
        data["retweet_frequency"]["retweet_metadata"] = {}
    if text not in data["retweet_frequency"]["retweet_metadata"]:
        data["retweet_frequency"]["retweet_metadata"][text] = {}
    data["retweet_frequency"]["retweet_metadata"][text]["id_str"] = id_str
    data["retweet_frequency"]["retweet_metadata"][text]["url"] = url
    if "names" not in data["retweet_frequency"]["retweet_metadata"][text]:
        data["retweet_frequency"]["retweet_metadata"][text]["names"] = []
    data["retweet_frequency"]["retweet_metadata"][text]["names"].append(name)

def delete_retweet_frequency(delete_list):
    debug_print(sys._getframe().f_code.co_name)
    global data
    purged = 0
    for text in delete_list:
        purged += 1
        decrement_counter("tracked_retweets")
        del data["retweet_frequency"]["first_seen_retweet"][text]
        del data["retweet_frequency"]["previous_seen_retweet"][text]
        del data["retweet_frequency"]["retweet_counter"][text]
        del data["retweet_frequency"]["retweet_metadata"][text]

def set_retweet_spike_data(text, url, id_str, first_seen, last_seen, count, names):
    debug_print(sys._getframe().f_code.co_name)
    global data
    new_count = count
    real_first_seen = first_seen
    if "retweet_spikes" not in data:
        data["retweet_spikes"] = {}
    if "retweet_spikes" in data:
        if text not in data["retweet_spikes"]:
            data["retweet_spikes"][text] = {}
            print
            print "New retweet spike detected!"
            print text
    spike_record = {}
    spike_record["first_seen"] = real_first_seen
    spike_record["last_seen"] = last_seen
    spike_record["count"] = new_count
    spike_record["url"] = url
    spike_record["id_str"] = id_str
    spike_record["names"] = names
    data["retweet_spikes"][text] = spike_record

def dump_retweet_spikes():
    debug_print(sys._getframe().f_code.co_name)
    global data
    if "retweet_spikes" in data:
        filename = "data/custom/retweet_spikes.txt"
        handle = io.open(filename, "w", encoding='utf-8')
        spike_count = 0
        for text, stuff in data["retweet_spikes"].iteritems():
            spike_count += 1
            text = text.replace('\n', ' ')
            handle.write(u"Tweet:\t" + unicode(text) + u"\n")
            handle.write(u"URL:\t" + unicode(stuff["url"]) + u"\n")
            handle.write(u"Id:\t" + unicode(stuff["id_str"]) + u"\n")
            handle.write(u"Start time:\t" + unicode(unix_time_to_readable(stuff["first_seen"])) + u"\n")
            handle.write(u"End time:\t" + unicode(unix_time_to_readable(stuff["last_seen"])) + u"\n")
            handle.write(u"Count:\t" + unicode(stuff["count"]) + u"\n")
            duration = stuff["last_seen"] - stuff["first_seen"]
            handle.write(u"Duration:\t" + unicode(duration) + u" seconds.\n")
            if duration > 0:
                tweets_per_second = float(stuff["count"]/float(duration))
                handle.write(u"Tweets per second:\t" + "%.2f" % tweets_per_second + "\n")
            if "names" in stuff:
                namelist = u",".join(map(unicode, stuff["names"]))
                handle.write(u"Users:\n")
                handle.write(namelist + u"\n")
            handle.write(u"\n")

        handle.close()
        set_counter("retweet_spikes", spike_count)

def process_retweet_frequency():
    debug_print(sys._getframe().f_code.co_name)
    global data
    timestamp = get_utc_unix_time()
    delete_list = []
    if "retweet_frequency" in data:
        if "retweet_counter" in data["retweet_frequency"]:
            for text, count in data["retweet_frequency"]["retweet_counter"].iteritems():
                previous_seen = data["retweet_frequency"]["previous_seen_retweet"][text]
                first_seen = data["retweet_frequency"]["first_seen_retweet"][text]
                time_since_last_seen = 0
                if previous_seen > 0:
                    time_since_last_seen = timestamp - previous_seen
                total_time_seen = 0
                if first_seen > 0:
                    total_time_seen = timestamp - first_seen
                elif previous_seen > 0:
                    total_time_seen = timestamp - previous_seen
                tweets_per_second = 0
                min_tps = conf["params"]["retweet_spike_per_second_minimum"] 
                min_count = conf["params"]["retweet_spike_minimum"]
                if total_time_seen > 0:
                    tweets_per_second = float(float(count)/float(total_time_seen))
                if time_since_last_seen >= conf["params"]["retweet_spike_window"]:
                        delete_list.append(text)
                        continue
                if tweets_per_second >= min_tps and count >= min_count:
                        start = data["retweet_frequency"]["first_seen_retweet"][text]
                        end = data["retweet_frequency"]["previous_seen_retweet"][text]
                        count = data["retweet_frequency"]["retweet_counter"][text]
                        metadata = data["retweet_frequency"]["retweet_metadata"][text]
                        id_str = metadata["id_str"]
                        names = metadata["names"]
                        url = metadata["url"]
                        set_retweet_spike_data(text, url, id_str, start, end, count, names)
    if len(delete_list) > 0:
        delete_retweet_frequency(delete_list)

def record_highly_retweeted(text, count):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if "highly_retweeted" not in data:
        data["highly_retweeted"] = {}
    data["highly_retweeted"][text] = count

def get_highly_retweeted():
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = {}
    if "highly_retweeted" in data:
        ret = data["highly_retweeted"]
    return ret


def record_volume_data(category, label, timestamp, value):
    debug_print(sys._getframe().f_code.co_name)
    global data
    entry = {}
    if category not in data:
        data[category] = {}
    if label not in data[category]:
        data[category][label] = []
    entry[timestamp] = value
    data[category][label].append(entry)

def get_volume_data(category, label):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = {}
    if category in data:
        if label in data[category]:
            ret = data[category][label]
    return ret

def get_volume_labels(category):
    debug_print(sys._getframe().f_code.co_name)
    global data
    ret = []
    if category in data:
        for label, stuff in data[category].iteritems():
            if label not in ret:
                ret.append(label)
    return ret

def record_sentiment_volume(label, timestamp, value):
    debug_print(sys._getframe().f_code.co_name)
    record_volume_data("sentiment_volumes", label, timestamp, value)

def get_sentiment_volumes(label):
    debug_print(sys._getframe().f_code.co_name)
    return get_volume_data("sentiment_volumes", label)

def get_sentiment_volume_labels():
    debug_print(sys._getframe().f_code.co_name)
    return get_volume_labels("sentiment_volumes")

def record_tweet_volume(label, timestamp, value):
    debug_print(sys._getframe().f_code.co_name)
    record_volume_data("tweet_volumes", label, timestamp, value)

def get_tweet_volumes(label):
    debug_print(sys._getframe().f_code.co_name)
    return get_volume_data("tweet_volumes", label)

def get_tweet_volume_labels():
    debug_print(sys._getframe().f_code.co_name)
    return get_volume_labels("tweet_volumes")

def add_userinfo(category, name, user_data):
    debug_print(sys._getframe().f_code.co_name)
    return set_storage("userinfo", category, name, user_data)

def get_userinfo_value(category, name, field):
    debug_print(sys._getframe().f_code.co_name)
    info = get_storage("userinfo", category, name)
    ret = 0
    if info is not None:
        if field in info:
            ret = info[field]
    return ret

def get_all_userinfo():
    debug_print(sys._getframe().f_code.co_name)
    return get_all_storage("userinfo")

def exists_userinfo(category, name):
    debug_print(sys._getframe().f_code.co_name)
    return exists_storage("userinfo", category, name)

def del_userinfo(category, name):
    debug_print(sys._getframe().f_code.co_name)
    del_from_storage("userinfo", category, name)

def add_network_data_link(link_type, src, dest):
    debug_print(sys._getframe().f_code.co_name)
    return add_to_list_data("network_data", src, link_type, dest)

def get_network_data_link(link_type, name):
    debug_print(sys._getframe().f_code.co_name)
    return get_from_list_data("network_data", name, link_type)

def del_network_data(name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if "network_data" in data:
        if name in data["network_data"]:
            del data["network_data"][name]

def add_user_source_data(name, value):
    debug_print(sys._getframe().f_code.co_name)
    return add_to_list_data_large("user_source_data", "sources", name, value)

def get_user_source_data(name):
    debug_print(sys._getframe().f_code.co_name)
    return get_from_list_data_large("user_source_data", "sources", name)

def add_user_hashtag_data(name, value):
    debug_print(sys._getframe().f_code.co_name)
    return add_to_list_data_large("user_hashtag_data", "hashtags", name, value)

def get_user_hashtag_data(name):
    debug_print(sys._getframe().f_code.co_name)
    return get_from_list_data_large("user_hashtag_data", "hashtags", name)

def del_association(name):
    debug_print(sys._getframe().f_code.co_name)
    global data
    if "associations" in data:
        if name in data["associations"]:
            del data["associations"][name]

def set_associations(link_type, name, value):
    debug_print(sys._getframe().f_code.co_name)
    return set_storage("associations", name, link_type, value)

def get_associations(link_type, name):
    debug_print(sys._getframe().f_code.co_name)
    ret = get_storage("associations", name, link_type)
    if ret is None:
        return 0
    return ret

def get_all_associations(link_type):
    debug_print(sys._getframe().f_code.co_name)
    ret = {}
    if "associations" in data:
        for name, stuff in data["associations"].iteritems():
            if name not in ret:
                if link_type in stuff:
                    ret[name] = stuff[link_type]
    return ret

def increment_heatmap(name, tweet_time_object):
    debug_print(sys._getframe().f_code.co_name)
    global data
    week = tweet_time_object.strftime("%Y%W")
    weekday = tweet_time_object.weekday()
    hour = tweet_time_object.hour
    category = "heatmaps"
    if category not in data:
        data[category] = {}
    if name not in data[category]:
        data[category][name] = {}
    if week not in data[category][name]:
        data[category][name][week] = [[0 for j in range(24)] for i in range(7)]
    data[category][name][week][weekday][hour] += 1

def get_heatmap(category):
    debug_print(sys._getframe().f_code.co_name)
    if "heatmaps" in data:
        if category in data["heatmaps"]:
            return data["heatmaps"][category]

def get_all_heatmaps():
    debug_print(sys._getframe().f_code.co_name)
    if "heatmaps" in data:
        return data["heatmaps"]

def calculate_interarrival_statistics(interarrival_data):
    debug_print(sys._getframe().f_code.co_name)
    std = 0.0
    av = 0.0
    counts = []
    values = []
    if interarrival_data is not None:
        for value, count in interarrival_data.iteritems():
            count = int(count)
            value = int(value)
            counts.append(count)
            for x in range(count):
                values.append(value)
        if len(counts) > 0:
            std = float(np.std(counts))
        if len(values) > 0:
            av = float(np.mean(values))
    return std, av

def record_interarrival(category, name, delta):
    debug_print(sys._getframe().f_code.co_name)
    variable = "interarrivals"
    global data
    ret = False
    fl = get_first_letters(name)
    if variable not in data:
        data[variable] = {}
    if category not in data[variable]:
        data[variable][category] = {}
    if fl not in data[variable][category]:
        data[variable][category][fl] = {}
    if name not in data[variable][category][fl]:
        data[variable][category][fl][name] = {}
    if delta not in data[variable][category][fl][name]:
        data[variable][category][fl][name][delta] = 1
        ret = True
    else:
        data[variable][category][fl][name][delta] += 1
    return ret

def get_interarrival(category, name):
    debug_print(sys._getframe().f_code.co_name)
    fl = get_first_letters(name)
    variable = "interarrivals"
    ret = {}
    if variable in data:
        if category in data[variable]:
            if fl in data[variable][category]:
                if name in data[variable][category][fl]:
                    ret = data[variable][category][fl][name]
    return ret

def del_interarrival(category, name):
    debug_print(sys._getframe().f_code.co_name)
    fl = get_first_letters(name)
    variable = "interarrivals"
    ret = {}
    if variable in data:
        if category in data[variable]:
            if fl in data[variable][category]:
                if name in data[variable][category][fl]:
                    del data[variable][category][fl][name]

def add_interarrival(category, name, tweet_time):
    debug_print(sys._getframe().f_code.co_name)
    if conf["config"]["log_interarrivals"] == False:
        return
    if exists_storage_large("interarrivals", "previous_tweeted", name):
        previous_tweet_time = int(get_storage_large("interarrivals", "previous_tweeted", name))
        delta = tweet_time - previous_tweet_time
        if delta > 0:
            return record_interarrival(category, name, delta)
    set_storage_large("interarrivals", "previous_tweeted", name, str(tweet_time))

def add_interarrival_stdev(category, name, value):
    debug_print(sys._getframe().f_code.co_name)
    if conf["config"]["log_interarrivals"] == False:
        return
    return set_storage("interarrival_stdevs", category, name, value)

def get_all_interarrival_stdevs():
    debug_print(sys._getframe().f_code.co_name)
    return get_all_storage("interarrival_stdevs")

def set_previous_seen(category, item):
    debug_print(sys._getframe().f_code.co_name)
    timestamp = int(time.time())
    set_storage_large("previous_seen", category, item, timestamp)
    if not exists_storage_large("first_seen", category, item):
        set_storage_large("first_seen", category, item, timestamp)

def get_first_seen(category, item):
    debug_print(sys._getframe().f_code.co_name)
    get_storage_large("first_seen", category, item)

def get_previous_seen(category, item):
    debug_print(sys._getframe().f_code.co_name)
    get_storage_large("previous_seen", category, item)

def del_previous_seen(category, item):
    debug_print(sys._getframe().f_code.co_name)
    del_from_storage_large("previous_seen", category, item)

def get_categories_previous_seen():
    debug_print(sys._getframe().f_code.co_name)
    return get_categories_from_storage("previous_seen")

def get_category_previous_seen(category):
    debug_print(sys._getframe().f_code.co_name)
    return get_category_storage_large("previous_seen", category)

def add_data(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    if variable == "users":
        if conf["config"]["log_user_data"] == False:
            return
    if variable == "metadata":
        if conf["config"]["log_metadata"] == False:
            return
    ret = increment_storage_large(variable, category, name)
    handling = "keep"
    if "params" in conf:
        if "data_handling" in conf["params"]:
            handling = conf["params"]["data_handling"]
    if "keep" not in handling:
        set_previous_seen(variable, name)
    if ret is True:
        increment_counter(variable + "_" + category)
    return ret

def get_data(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    return get_storage_large(variable, category, name)

def get_category_data(variable, category):
    debug_print(sys._getframe().f_code.co_name)
    return get_category_storage_large(variable, category)

def get_all_data(variable):
    debug_print(sys._getframe().f_code.co_name)
    return get_all_storage_large(variable)

def set_data(variable, category, name, value):
    debug_print(sys._getframe().f_code.co_name)
    set_storage_large(variable, category, name, value)

def exists_data(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    return exists_storage_large(variable, category, name)

def del_data(variable, category, name):
    debug_print(sys._getframe().f_code.co_name)
    del_from_storage_large(variable, category, name)
    handling = "keep"
    if "params" in conf:
        if "data_handling" in conf["params"]:
            handling = conf["params"]["data_handling"]
    if "keep" not in handling:
        del_previous_seen(variable, name)

def increment_per_hour(category, datestring, name):
    debug_print(sys._getframe().f_code.co_name)
    if conf["config"]["log_per_hour_data"] == False:
        return
    increment_storage("per_day_data", category + "_" + datestring[:-2], name)
    return increment_storage("per_hour_data", category + "_" + datestring, name)

def get_categories_from_periodic_data(data_type):
    debug_print(sys._getframe().f_code.co_name)
    return get_categories_from_storage(data_type)

def get_top_data_entries(data_set, category, threshold):
    debug_print(sys._getframe().f_code.co_name)
    variable = get_category_data(data_set, category)
    c = 0
    ret = {}
    for tag, count in sorted(variable.items(), key=lambda x:x[1], reverse=True):
        if c > threshold:
            break
        if tag not in ret:
            ret[tag] = count
            c += 1
    return ret

def get_top_periodic_data_entries(data_type, label, threshold):
    debug_print(sys._getframe().f_code.co_name)
    variable = get_category_from_periodic_data(data_type, label)
    c = 0
    ret = {}
    for tag, count in sorted(variable.items(), key=lambda x:x[1], reverse=True):
        if c > threshold:
            break
        if tag not in ret:
            ret[tag] = count
            c += 1
    return ret

def get_category_names_from_periodic_data(data_type):
    debug_print(sys._getframe().f_code.co_name)
    categories = get_categories_from_periodic_data(data_type)
    ret = []
    for cat in categories:
        m = re.search("^(.+)_[0-9]+$", cat)
        if m is not None:
            n = m.group(1)
            if n not in ret:
                ret.append(n)
    return ret

def get_category_from_periodic_data(data_type, category):
    debug_print(sys._getframe().f_code.co_name)
    return get_category_storage(data_type, category)

def del_from_periodic_data(data_type, category):
    debug_print(sys._getframe().f_code.co_name)
    del_category_from_storage(data_type, category)

def add_graphing_data(category, username, item):
    debug_print(sys._getframe().f_code.co_name)
    if conf["config"]["log_network_data"] == True:
        if "hashtags" in category:
            add_user_hashtag_data(username, item)
        elif "sources" in category:
            add_user_source_data(username, item)
        else:
            add_network_data(username, item)

def add_network_data(src, dest):
    debug_print(sys._getframe().f_code.co_name)
    add_network_data_link("links_out", src, dest)
    add_network_data_link("links_in", dest, src)
    add_two_way(src)
    add_two_way(dest)
    record_associations(src)
    record_associations(dest)

def add_two_way(name):
    debug_print(sys._getframe().f_code.co_name)
    links_out = []
    links_in = []
    links_out = get_network_data_link("links_out", name)
    links_in = get_network_data_link("links_in", name)
    if len(links_out) > 0 and len(links_in) > 0:
        for n in links_out:
            if n in links_in:
                if n != name:
                    add_network_data_link("two_way", name, n)

def record_associations(name):
    debug_print(sys._getframe().f_code.co_name)
    links_out = []
    links_in = []
    two_way = []
    links_out = get_network_data_link("links_out", name)
    links_in = get_network_data_link("links_in", name)
    two_way = get_network_data_link("two_way", name)
    set_associations("links_out", name, len(links_out))
    set_associations("links_in", name, len(links_in))
    set_associations("two_way", name, len(two_way))

##########
# Counters
##########
def increment_counter(name):
    debug_print(sys._getframe().f_code.co_name)
    increment_storage("statistics", "counters", name)

def decrement_counter(name):
    debug_print(sys._getframe().f_code.co_name)
    decrement_storage("statistics", "counters", name)

def add_to_counter(name, value):
    debug_print(sys._getframe().f_code.co_name)
    add_to_storage("statistics", "counters", name, value)

def get_counter(name):
    debug_print(sys._getframe().f_code.co_name)
    return get_storage("statistics", "counters", name)

def set_counter(name, value):
    debug_print(sys._getframe().f_code.co_name)
    set_storage("statistics", "counters", name, value)

def get_all_counters():
    debug_print(sys._getframe().f_code.co_name)
    return get_category_storage("statistics", "counters")

def exists_counter(name):
    debug_print(sys._getframe().f_code.co_name)
    return exists_storage("statistics", "counters", name)


##################
# Helper functions
##################
def debug_print(string):
    if debug == True:
        print string

def twarc_time_to_readable(time_string):
    twarc_format = "%a %b %d %H:%M:%S %Y"
    match_expression = "^(.+)\s(\+[0-9][0-9][0-9][0-9])\s([0-9][0-9][0-9][0-9])$"
    match = re.search(match_expression, time_string)
    if match is not None:
        first_bit = match.group(1)
        second_bit = match.group(2)
        last_bit = match.group(3)
        new_string = first_bit + " " + last_bit
        date_object = datetime.strptime(new_string, twarc_format)
        return date_object.strftime("%Y-%m-%d %H:%M:%S")

def twarc_time_to_object(time_string):
    twarc_format = "%a %b %d %H:%M:%S %Y"
    match_expression = "^(.+)\s(\+[0-9][0-9][0-9][0-9])\s([0-9][0-9][0-9][0-9])$"
    match = re.search(match_expression, time_string)
    if match is not None:
        first_bit = match.group(1)
        second_bit = match.group(2)
        last_bit = match.group(3)
        new_string = first_bit + " " + last_bit
        date_object = datetime.strptime(new_string, twarc_format)
        return date_object

def twarc_time_to_unix(time_string):
    return time_object_to_unix(twarc_time_to_object(time_string))

def seconds_since_twarc_time(time_string):
    input_time_unix = int(twarc_time_to_unix(time_string))
    current_time_unix = int(get_utc_unix_time())
    return current_time_unix - input_time_unix

def time_object_to_readable(time_object):
    return time_object.strftime("%Y-%m-%d %H:%M:%S")

def time_string_to_object(time_string):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')

def time_object_to_unix(time_object):
    return int(time_object.strftime("%s"))

def get_utc_unix_time():
    dts = datetime.utcnow()
    epochtime = time.mktime(dts.timetuple())
    return epochtime

def unix_time_to_readable(time_string):
    return datetime.fromtimestamp(int(time_string)).strftime('%Y-%m-%d %H:%M:%S')

def get_datestring(data_type, offset):
    debug_print(sys._getframe().f_code.co_name)
    if data_type is "per_hour_data":
        time_here = datetime.utcnow() - timedelta(hours = offset)
        ymd = time_here.strftime("%Y%m%d")
        hour = int(time_here.strftime("%H"))
        hour_string = "%02d" % hour
        return ymd + hour_string
    else:
        time_here = datetime.utcnow() - timedelta(days = offset)
        ymd = time_here.strftime("%Y%m%d")
        return ymd

def sanitize_string(raw_data):
    debug_print(sys._getframe().f_code.co_name)
    if "config" in conf:
        if "sanitize_text" in conf["config"]:
            if conf["config"]["sanitize_text"] == True:
                return ''.join(x for x in raw_data if x in string.printable)
    else:
        return raw_data

def strip_quotes(string):
    debug_print(sys._getframe().f_code.co_name)
    if string[1] == "\"" and string[-1] == "\"":
        return string[1:-1]
    else:
        return string

def get_first_letters(string):
    debug_print(sys._getframe().f_code.co_name)
    fl = ""
    if len(string) > 1:
        fl = string[0] + string[1]
    else:
        fl = string[0]
    return fl

def get_average(list):
    debug_print(sys._getframe().f_code.co_name)
    num_items = len(list)
    sum_items = sum(list)
    return float(sum_items/num_items)

def strip_crap(text):
    debug_print(sys._getframe().f_code.co_name)
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
    debug_print(sys._getframe().f_code.co_name)
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
            elif re.search(u"^[0-9\.\,%]+$", t) is not None:
                continue
            elif re.search(u"\s+", t) is not None:
                continue
            elif re.search(u"[\.\,\:\;\?\-\_\!]+", t) is not None:
                continue
            elif re.search(u"^rt$", t) is not None:
                continue
            elif re.search(url_match, t) is not None:
                continue
            else:
                if len(t) > 0:
                    ret.append(t)
    return ret

def strip_stopwords(raw_data, lang):
    debug_print(sys._getframe().f_code.co_name)
    debug_print("Stripping stopwords from: " + str(raw_data))
    ret = []
    if lang not in conf["corpus"]["stopwords"]:
        return raw_data
    for word in raw_data:
        if word not in conf["corpus"]["stopwords"][lang]:
            ret.append(word)
    return ret

def get_word_count(wordlist, reference_list):
    debug_print(sys._getframe().f_code.co_name)
    ret = []
    for word in wordlist:
        if word.encode('utf-8') in reference_list:
            ret.append(word)
    return ret

def get_positive_words(wordlist):
    debug_print(sys._getframe().f_code.co_name)
    return get_word_count(wordlist, conf["corpus"]["positive_words"])

def get_negative_words(wordlist):
    debug_print(sys._getframe().f_code.co_name)
    return get_word_count(wordlist, conf["corpus"]["negative_words"])

def get_positive_hashtags(taglist):
    debug_print(sys._getframe().f_code.co_name)
    return get_word_count(taglist, conf["corpus"]["positive_hashtags"])

def get_negative_hashtags(taglist):
    debug_print(sys._getframe().f_code.co_name)
    return get_word_count(taglist, conf["corpus"]["negative_hashtags"])

def is_source_legit(source):
    debug_print(sys._getframe().f_code.co_name)
    ret = False
    if "corpus" in conf:
        if "legit_sources" in conf["corpus"]:
            if source in conf["corpus"]["legit_sources"]:
                ret = True
    return ret

#########################
# Init and config reading
#########################
def read_settings(filename):
    debug_print(sys._getframe().f_code.co_name)
    config = {}
    if os.path.exists(filename):
        with open(filename, "r") as file:
            for line in file:
                if line is not None:
                    line = line.strip()
                    if len(line) > 0:
                        name, value = line.split("=")
                        name = name.strip()
                        value = int(value)
                        if value == 1:
                            config[name] = True
                        elif value == 0:
                            config[name] = False
    return config

def print_settings(config):
    debug_print(sys._getframe().f_code.co_name)
    for var, val in config.iteritems():
        print var + " = " + str(val)

def read_config_unicode(filename):
    debug_print(sys._getframe().f_code.co_name)
    ret_array = []
    if os.path.exists(filename):
        with io.open(filename, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                line = line.lower()
                ret_array.append(line)
    return ret_array

def read_config(filename):
    debug_print(sys._getframe().f_code.co_name)
    ret_array = []
    if os.path.exists(filename):
        with open(filename, "r") as file:
            for line in file:
                line = line.strip()
                line = line.lower()
                ret_array.append(line)
    return ret_array

def read_config_preserve_case(filename):
    debug_print(sys._getframe().f_code.co_name)
    ret_array = []
    if os.path.exists(filename):
        with open(filename, "r") as file:
            for line in file:
                line = line.strip()
                ret_array.append(line)
    return ret_array

def cleanup():
    debug_print(sys._getframe().f_code.co_name)
    global dump_file_handle, stopping
    if threaded == True:
        if get_active_threads > 1:
            print "Waiting for queue to empty..."
            stopping = True
            tweet_queue.join()
    dump_file_handle.close()
    volume_file_handle.close()
    print "Serializing data..."
    serialize_data()

def log_stacktrace():
    debug_print(sys._getframe().f_code.co_name)
    exc_type, exc_value, exc_traceback = sys.exc_info()
    timestr = str(int(time.time()))
    filename = "errors/" + timestr + "trace.txt"
    if not os.path.exists("errors/"):
        os.makedirs("errors/")
    handle = open(filename, "w")
    traceback.print_exc(file=handle)
    traceback.print_stack(file=handle)
    traceback.print_tb(exc_traceback, file=handle)
    handle.close

def init_tweet_processor():
    debug_print(sys._getframe().f_code.co_name)
    directories = ["serialized", "data", "data/heatmaps", "data/custom", "data/raw"]
    for dir in directories:
        if not os.path.exists(dir):
            os.makedirs(dir)
    deserialize_data()
    init_params()
    init_config()
    read_corpus()
    reload_settings()
    set_counter("dump_interval", conf["params"]["default_dump_interval"])
    set_counter("previous_dump_time", int(time.time()))
    set_counter("previous_graph_dump_time", int(time.time()))
    set_counter("script_start_time", int(time.time()))
    set_counter("previous_serialize", int(time.time()))
    set_counter("previous_config_reload", int(time.time()))
    set_counter("previous_purge", int(time.time()))

def read_corpus():
    debug_print(sys._getframe().f_code.co_name)
    global conf
    conf["corpus"] = {}
    if os.path.exists("corpus/stopwords-iso.json"):
        handle = open("corpus/stopwords-iso.json", "r")
        conf["corpus"]["stopwords"] = json.load(handle)
        handle.close()
    conf["corpus"]["legit_sources"] = read_config_preserve_case("corpus/legit_sources.txt")
    conf["corpus"]["negative_words"] = read_config("corpus/negative_words.txt")
    conf["corpus"]["positive_words"] = read_config("corpus/positive_words.txt")
    conf["corpus"]["negative_hashtags"] = read_config("corpus/negative_hashtags.txt")
    conf["corpus"]["positive_hashtags"] = read_config("corpus/positive_hashtags.txt")
    conf["corpus"]["fake_news_sources"] = read_config("corpus/fake_news_sources.txt")

def reload_settings():
    debug_print(sys._getframe().f_code.co_name)
    global conf
    if "settings" not in conf:
        conf["settings"] = {}
    conf["settings"]["monitored_hashtags"] = read_config_unicode("config/monitored_hashtags.txt")
    conf["settings"]["keywords"] = read_config_unicode("config/keywords.txt")
    conf["settings"]["monitored_users"] = read_config("config/monitored_users.txt")
    conf["settings"]["url_keywords"] = read_config("config/url_keywords.txt")
    conf["settings"]["monitored_langs"] = read_config("config/languages.txt")
    conf["settings"]["description_keywords"] = read_config("config/description_keywords.txt")
    conf["settings"]["tweet_identifiers"] = read_config("config/tweet_identifiers.txt")
    conf["config"] = read_settings("config/settings.txt")

def get_description_matches(description):
    debug_print(sys._getframe().f_code.co_name)
    desc = description.lower()
    ret = []
    if "settings" in conf:
        if "description_keywords" in conf["settings"]:
            for d in conf["settings"]["description_keywords"]:
                if d in desc:
                    ret.append(d)
    return ret

def get_tweet_identifier_matches(tweet_text):
    debug_print(sys._getframe().f_code.co_name)
    text = tweet_text.lower()
    ret = []
    if "settings" in conf:
        if "tweet_identifiers" in conf["settings"]:
            for t in conf["settings"]["tweet_identifiers"]:
                if t in text:
                    ret.append(t)
    return ret


###############
# Serialization
###############
def serialize_variable(variable, filename):
    debug_print(sys._getframe().f_code.co_name)
    serialize_dir = "serialized"
    filename = serialize_dir + "/" + filename + ".json"
    handle = open(filename, 'w')
    json.dump(variable, handle, indent=4)
    handle.close()
    return

def unserialize_variable(varname):
    debug_print(sys._getframe().f_code.co_name)
    serialize_dir = "serialized"
    if(os.path.exists(serialize_dir)):
        filename = serialize_dir + "/" + varname + ".json"
        if(os.path.exists(filename)):
            handle = open(filename, 'r')
            variable = json.load(handle)
            handle.close()
            return variable
        else:
            return
    else:
        return

def serialize_data():
    debug_print(sys._getframe().f_code.co_name)
    debug_print("Serializing...")
    print "Serializing..."
    serialize_dir = "serialized"
    tmp_dir = "serialized.tmp"
    old_dir = "serialized.old"
    if not os.path.exists(old_dir):
        if os.path.exists(tmp_dir):
            os.rename(tmp_dir, old_dir)
        if os.path.exists(serialize_dir):
            os.rename(serialize_dir, tmp_dir)
            os.makedirs(serialize_dir)
    serialize_variable(data, "data")
    debug_print("Serialization finished...")
    print "Serialization finished."
    if os.path.exists(old_dir):
        shutil.rmtree(old_dir)
    return

def deserialize_data():
    debug_print(sys._getframe().f_code.co_name)
    print "Deserializing data..."
    global data
    data = unserialize_variable("data")
    if data is None:
        data = {}
    return

def dump_tweet_to_disk(item):
    debug_print(sys._getframe().f_code.co_name)
    global dump_file_handle
    if "dump_raw_data" in conf["config"]:
        if conf["config"]["dump_raw_data"] == True:
            json.dump(item, dump_file_handle)
            dump_file_handle.write(u"\n")
    return

def add_timeline_data(date, name, action, item, twt_id):
    debug_print(sys._getframe().f_code.co_name)
    if name in conf["settings"]["monitored_users"]:
        filename = "data/custom/timeline_" + name + ".csv"
        write_timeline(filename, date, name, action, item, twt_id)
    if conf["config"]["log_timeline_data"] is True:
        filename = "data/custom/timeline.csv"
        write_timeline(filename, date, name, action, item, twt_id)

def write_timeline(filename, date, name, action, item, twt_id):
    debug_print(sys._getframe().f_code.co_name)
    if not os.path.exists(filename):
        handle = io.open(filename, 'w', encoding='utf-8')
        handle.write(u"Date, Username, Action, Item, Tweet Id\n")
        handle.close
    item = item.replace(",", "")
    handle = io.open(filename, 'a', encoding='utf-8')
    outstr = date.encode('utf-8') + u", " + name.encode('utf-8') + u", " + action.encode('utf-8') + u", " + item + u", " + twt_id.encode('utf-8') + u"\n"
    handle.write(outstr)
    handle.close()

###########################
# Graph generation routines
###########################

def dump_pie_chart(dirname, filename, title, chart_data):
    debug_print(sys._getframe().f_code.co_name)
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
    debug_print(sys._getframe().f_code.co_name)
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

def dump_sentiment_volume_graphs():
    debug_print(sys._getframe().f_code.co_name)
    labels = get_sentiment_volume_labels()
    for l in labels:
        volume_data = get_sentiment_volumes(l)
        dates = []
        volumes = []
        for items in volume_data:
            date = items.keys()
            volume = items.values()
            dates.append(date[0])
            volumes.append(volume[0])
        chart_data = {}
        chart_data["sentiment"] = volumes
        dirname = "data/sentiment/"
        filename = "sentiment_" + l + ".svg"
        title = "Sentiment (" + l + ")"
        dump_line_chart(dirname, filename, title, dates, chart_data)

def dump_tweet_volume_graphs():
    debug_print(sys._getframe().f_code.co_name)
    labels = get_tweet_volume_labels()
    for l in labels:
        volume_data = get_tweet_volumes(l)
        dates = []
        volumes = []
        for items in volume_data:
            date = items.keys()
            volume = items.values()
            dates.append(date[0])
            volumes.append(volume[0])
        chart_data = {}
        chart_data["tweets/sec"] = volumes
        dirname = "data/"
        filename = "_tweet_volumes_" + l + ".svg"
        title = "Tweet Volumes (" + l + ")"
        dump_line_chart(dirname, filename, title, dates, chart_data)

def dump_languages_graph():
    debug_print(sys._getframe().f_code.co_name)
    counter_data = get_all_counters()
    if counter_data is not None:
        chart_data = {}
        for name, value in sorted(counter_data.iteritems(), key=lambda x:x[1], reverse= True):
            m = re.search("^tweets_([a-z][a-z][a-z]?)$", name)
            if m is not None:
                item = m.group(1)
                chart_data[item] = value
        dirname = "data/"
        filename = "_lang_breakdown.svg"
        title = "Language breakdown"
        dump_pie_chart(dirname, filename, title, chart_data)

def dump_captured_languages_graph():
    debug_print(sys._getframe().f_code.co_name)
    counter_data = get_all_counters()
    if counter_data is not None:
        chart_data = {}
        for name, value in sorted(counter_data.iteritems(), key=lambda x:x[1], reverse= True):
            m = re.search("^captured_tweets_([a-z][a-z][a-z]?)$", name)
            if m is not None:
                item = m.group(1)
                chart_data[item] = value
        dirname = "data/"
        filename = "_captured_lang_breakdown.svg"
        title = "Language breakdown"
        dump_pie_chart(dirname, filename, title, chart_data)

def dump_targets_graph():
    debug_print(sys._getframe().f_code.co_name)
    counter_data = get_all_counters()
    if counter_data is not None:
        chart_data = {}
        for name, value in sorted(counter_data.iteritems(), key=lambda x:x[1], reverse= True):
            m = re.search("^target_(.+)$", name)
            if m is not None:
                item = m.group(1)
                chart_data[item] = value
        dirname = "data/"
        filename = "_target_breakdown.svg"
        title = "Targets breakdown"
        dump_pie_chart(dirname, filename, title, chart_data)

def dump_keywords_graph():
    debug_print(sys._getframe().f_code.co_name)
    counter_data = get_all_counters()
    if counter_data is not None:
        chart_data = {}
        for name, value in sorted(counter_data.iteritems(), key=lambda x:x[1], reverse= True):
            m = re.search("^keyword_(.+)$", name)
            if m is not None:
                item = m.group(1)
                chart_data[item] = value
        dirname = "data/"
        filename = "_keyword_breakdown.svg"
        title = "Keywords breakdown"
        dump_pie_chart(dirname, filename, title, chart_data)

def is_graph_printable(name):
    debug_print(sys._getframe().f_code.co_name)
    ret = True
    if "settings" in conf:
        if "monitored_langs" in conf["settings"]:
            if len(conf["settings"]["monitored_langs"]) == 0:
                if re.search("^words_.+$", name) is not None:
                    ret = False
    if re.search("^monitored_.+$", name) is not None:
        ret = False
    if re.search("^keyword_.+$", name) is not None:
        ret = False
    if re.search("^url_keyword_.+$", name) is not None:
        ret = False
    if "tweets" in name:
        ret = False
    if "description" in name:
        ret = False
    if "fake_news" in name:
        ret = False
    if "source" in name:
        ret = False
    if "url" in name:
        ret = False
    if name == "monitored_hashtags":
        ret = True
    if name == "keywords":
        ret = True
    return ret

def dump_overall_object_graphs(data_type):
    debug_print(sys._getframe().f_code.co_name)
    data_sets = ["users", "metadata"]
    current_datestring = get_datestring(data_type, 0)
    for ds in data_sets:
        for category in get_categories_from_storage(ds):
            if is_graph_printable(category) == False:
                continue
            top_data = get_top_data_entries(ds, category, 10)
            top_data_names = []
            for n, c in sorted(top_data.iteritems()):
                top_data_names.append(n)
            for name in top_data_names:
                chart_labels = []
                chart_values = []
                offset = 10
                while offset >= 0:
                    chart_item = {}
                    datestring = get_datestring(data_type, offset)
                    offset -= 1
                    label = category + "_" + datestring
                    variable = get_category_from_periodic_data(data_type, label)
                    if variable is not None:
                        chart_labels.append(datestring[-2:])
                        if name in variable:
                            chart_values.append(variable[name])
                        else:
                            chart_values.append(0)
                chart = pygal.Line(show_y_guides=False)
                chart.title = name
                chart.x_labels = chart_labels
                chart.add(name, chart_values)
                dirname = "data/graphs/overall/" + category + "/objects/" + data_type + "/"
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                name = name.replace("/", "")
                filename = dirname + name + "_" + current_datestring + ".svg"
                chart.render_to_file(filename)

def dump_overall_data_graphs(data_type):
    debug_print(sys._getframe().f_code.co_name)
    data_sets = ["users", "metadata"]
    current_datestring = get_datestring(data_type, 0)
    for ds in data_sets:
        for category in get_categories_from_storage(ds):
            if is_graph_printable(category) == False:
                continue
            top_data = get_top_data_entries(ds, category, 10)
            top_data_names = []
            for n, c in sorted(top_data.iteritems()):
                top_data_names.append(n)
            dirname = "data/graphs/overall/" + category + "/pie/" + data_type + "/"
            filename = category + "_pie_" + current_datestring + ".svg"
            dump_pie_chart(dirname, filename, category, top_data)
            trend_data = []
            offset = 10
            while offset >= 0:
                trend_item = {}
                datestring = get_datestring(data_type, offset)
                label = category + "_" + datestring
                offset -= 1
                variable = get_category_from_periodic_data(data_type, label)
                if variable is None:
                    continue
                trend_item["date"] = datestring[-2:]
                for name in top_data_names:
                    if name in variable:
                        trend_item[name] = variable[name]
                    else:
                        trend_item[name] = 0
                trend_data.append(trend_item)
            dirname = "data/graphs/overall/" + category + "/" + data_type + "/"
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            filename = dirname + category + "_" + datestring + ".svg"
            chart = pygal.Bar(show_y_guides=False)
            chart.x_labels = [x['date'] for x in trend_data]
            for name in sorted(top_data_names):
                mark_list = [x[name] for x in trend_data]
                chart.add(name, mark_list)
            chart.render_to_file(filename)

def dump_periodic_data_graphs(data_type):
    debug_print(sys._getframe().f_code.co_name)
    categories = get_categories_from_periodic_data(data_type)
    top_level_cats = get_category_names_from_periodic_data(data_type)
    current_datestring = get_datestring(data_type, 0)
    for cat in top_level_cats:
        if is_graph_printable(cat) == False:
            continue
        current_label = cat + "_" + current_datestring
        if current_label in categories:
            top_data = get_top_periodic_data_entries(data_type, current_label, 10)
            top_data_names = []
            for n, c in top_data.iteritems():
                top_data_names.append(n)
            dirname = "data/graphs/raw/" + cat + "/" + data_type + "/"
            filename = cat + "_pie_" + current_datestring + ".svg"
            dump_pie_chart(dirname, filename, current_label, top_data)
            trend_data = []
            offset = 10
            while offset >= 0:
                trend_item = {}
                datestring = get_datestring(data_type, offset)
                label = cat + "_" + datestring
                offset -= 1
                if label in categories:
                    variable = get_category_from_periodic_data(data_type, label)
                    trend_item["date"] = datestring[-2:]
                    for name in top_data_names:
                        if name in variable:
                            trend_item[name] = variable[name]
                        else:
                            trend_item[name] = 0
                    trend_data.append(trend_item)
            dirname = "data/graphs/raw/" + cat + "/" + data_type + "/"
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            filename = dirname + cat + "_" + datestring + ".svg"
            chart = pygal.Bar(show_y_guides=False)
            chart.x_labels = [x['date'] for x in trend_data]
            for name in sorted(top_data_names):
                mark_list = [x[name] for x in trend_data]
                chart.add(name, mark_list)
            chart.render_to_file(filename)

def dump_periodic_data_trends(data_type):
    debug_print(sys._getframe().f_code.co_name)
    top_level_cats = get_category_names_from_periodic_data(data_type)
    current_datestring = get_datestring(data_type, 0)
    for cat in top_level_cats:
        if is_graph_printable(cat) == False:
            continue
        trend_data = calculate_trends(data_type, cat, 10)
        num_values = 0
        graph_chart_labels = []
        graph_chart_items = {}
        for j in reversed(range(0, 10)):
            ds = get_datestring(data_type, j)
            graph_chart_labels.append(ds[-2:])
        for name, values in sorted(trend_data.iteritems()):
            num_values = len(values)
            graph_chart_item = []
            index = num_values - 1
            if num_values > 2:
                plot_data = []
                while index >= 0:
                    plot_item = {}
                    ds = get_datestring(data_type, index)
                    plot_item["date"] = ds[-2:]
                    plot_item["value"] = values[index]
                    if index < 10:
                        graph_chart_item.append(values[index])
                    plot_data.append(plot_item)
                    index -= 1
                graph_chart_items[name] = graph_chart_item
                dirname = "data/graphs/trends/" + cat + "/objects/" + data_type + "/"
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                name = name.replace("/", "")
                filename = dirname + name + "_" + current_datestring + ".svg"
                chart = pygal.Line(show_y_guides=False)
                chart.x_labels = [x['date'] for x in plot_data]
                chart.add(name, [x['value'] for x in plot_data])
                chart.render_to_file(filename)
        dirname = "data/graphs/trends/" + cat + "/" + data_type + "/"
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        filename = dirname + cat + "_" + current_datestring + ".svg"
        graph_chart = pygal.Dot()
        graph_chart.x_labels = graph_chart_labels
        for name, vals in sorted(graph_chart_items.iteritems()):
            graph_chart.add(name, vals)
        graph_chart.render_to_file(filename)


def calculate_trends(data_type, category, threshold):
    debug_print(sys._getframe().f_code.co_name)
    categories = get_categories_from_periodic_data(data_type)
    current_datestring = get_datestring(data_type, 1)
    current_label = category + "_" + current_datestring
    trend_values = {}
    change_percentages = {}
    if current_label in categories:
        top_data_names = get_top_periodic_data_entries(data_type, current_label, threshold)
        baseline_values = {}
        variable = get_category_from_periodic_data(data_type, current_label)
        for name in top_data_names:
            if name in variable:
                baseline_values[name] = variable[name]
        for offset in reversed(range(1, 49)):
            datestring = get_datestring(data_type, offset)
            label = category + "_" + datestring
            if label in categories:
                variable = get_category_from_periodic_data(data_type, label)
                for name in top_data_names:
                    if name in variable:
                        val = variable[name]
                        base = baseline_values[name]
                        percent = (float(val)/float(base)) * 100.00
                        percent = int(percent)
                        if name not in change_percentages:
                            change_percentages[name] =[]
                        change_percentages[name].append(percent)
        for name in top_data_names:
            if name in change_percentages:
                average_percent = get_average(change_percentages[name])
                trend_values[name] = average_percent
    return change_percentages

############################
# Caching/purging mechanisms
############################
def purge_data():
    debug_print(sys._getframe().f_code.co_name)
    ret = 0
    handling = "keep"
    if "params" in conf:
        if "data_handling" in conf["params"]:
            handling = conf["params"]["data_handling"]
    if "keep" in handling:
        return ret
    increment_counter("purges")
    cats = get_categories_previous_seen()
    if cats is not None:
        for category in cats:
            psdata = get_category_previous_seen(category)
            for name, value in psdata.iteritems():
                if is_item_purgeable(category, name, value):
                    if "cache" in handling:
                        cache_item(category, name)
                    if "purge" in handling:
                        purge_item(category, name)
                        ret += 1
    return ret

def is_item_out_of_date(category, name, prev_seen):
    debug_print(sys._getframe().f_code.co_name)
    ret = False
    ttl = 0
    if "params" in conf:
        if "time_to_live" in conf["params"]:
            ttl = conf["params"]["time_to_live"]
    if ttl < 1:
        return ret
    if prev_seen > 0:
        purge_timestamp = int(time.time()) - ttl
        if prev_seen < purge_timestamp:
            ret = True
    return ret

def is_item_purgeable(category, name, prev_seen):
    debug_print(sys._getframe().f_code.co_name)
    ret = False
    out_of_date = is_item_out_of_date(category, name, prev_seen)
    if out_of_date == True:
        ret = True
        if "users" in category:
            links_in = get_associations("links_in", name)
            links_out = get_associations("links_out", name)
            two_way = get_associations("two_way", name)
            suspicious = exists_userinfo("suspicious", name)
            if links_in > 5 or links_out > 5 or two_way > 5 or suspicious == True:
                ret = False
    return ret

def purge_item(category, name):
    debug_print(sys._getframe().f_code.co_name)
    if "users" in category:
        del_association(name)
        del_network_data(name)
        del_data("users", category, name)
        del_interarrival(category, name)
    elif "metadata" in category:
        del_data("metadata", category, name)

def cache_item(category, name):
    debug_print(sys._getframe().f_code.co_name)
    # XXX implement

########################
# Specific dump routines
########################
def dump_counters():
    debug_print(sys._getframe().f_code.co_name)
    counter_dump = get_all_counters()
    val_output = ""
    date_output = ""
    if counter_dump is not None:
        for n, c in sorted(counter_dump.iteritems()):
            val = None
            if type(c) is float:
                val = "%.2f"%c
                val_output += unicode(val) + u"\t" + unicode(n) + u"\n"
            elif len(str(c)) > 9:
                val = unix_time_to_readable(int(c))
                date_output += unicode(val) + u"\t" + unicode(n) + u"\n"
            else:
                val = c
                val_output += unicode(val) + u"\t" + unicode(n) + u"\n"
    handle = io.open("data/_counters.txt", "w", encoding='utf-8')
    handle.write(unicode(val_output))
    handle.write(u"\n")
    handle.write(unicode(date_output))
    handle.close

def dump_heatmap(name):
    debug_print(sys._getframe().f_code.co_name)
    heatmap = get_heatmap(name)
    weekdays = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    if heatmap is not None:
        filename = "data/heatmaps/" + name.encode('utf-8') + "_heatmap.csv"
        handle = io.open(filename, 'w', encoding='utf-8')
        for yrwk, table in sorted(heatmap.iteritems()):
            handle.write(u"Details for " + unicode(name) + u":\n")
            handle.write(unicode(yrwk) + u"\n")
            handle.write(u"Hour, 00, 01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23" + u"\n")
            for x in range (0, 7):
                handle.write(unicode(weekdays[x]) + u", " + u','.join(map(unicode, table[x])) + u"\n")
            handle.write(u"\n")
        handle.close()
    return

def include_in_heatmap_comparison_graph(title):
    debug_print(sys._getframe().f_code.co_name)
    ret = True
    if re.search("^target_.+$", title) is not None:
        ret = False
    if re.search("^tweets_([a-z][a-z])$", title) is not None:
        ret = False
    if "keyword" in title:
        ret = False
    if "monitored" in title:
        ret = False
    if "tweets_und" in title:
        ret = False
    return ret

def dump_heatmap_comparison():
    debug_print(sys._getframe().f_code.co_name)
    filename = "data/custom/global_heatmaps"
    graph_titles = []
    graph_data = {}
    graph_x_data = []
    titles = []
    weeks = []
    output_string = u"Date"
    heatmaps = get_all_heatmaps()
    for title, stuff in sorted(heatmaps.iteritems()):
        if include_in_heatmap_comparison_graph(title):
            graph_titles.append(title)
        titles.append(title)
        output_string += u", " + unicode(title)
    output_string += u"\n"
    for yearweek, weekdata in sorted(heatmaps[titles[0]].iteritems()):
        weeks.append(yearweek)
    weekdays = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    for week in weeks:
        output_string += unicode(week) + u"\n"
        for day in range(0, 7):
            day_name = weekdays[day]
            for hour in range (0, 24):
                graph_item = {}
                this_row = []
                row_sum = 0
                for title in titles:
                    if title in heatmaps:
                        if week in heatmaps[title]:
                            val = heatmaps[title][week][day][hour]
                            date_string = week + " " + day_name + " " + str(hour) + ":00"
                            if title in graph_titles:
                                if date_string not in graph_x_data:
                                    graph_x_data.append(date_string)
                                if title not in graph_data:
                                    graph_data[title] = []
                                graph_data[title].append(val)
                            this_row.append(val)
                            row_sum += val
                if row_sum > 0:
                    output_string += unicode(day_name) + u" " + unicode(hour) + u":00"
                    output_string += u", " + u','.join(map(unicode, this_row)) + u"\n"
    debug_print("Writing custom file: " + filename)
    handle = io.open(filename + ".csv", 'w', encoding='utf-8')
    handle.write(output_string)
    handle.close()
    ordered_x_data = list(reversed(graph_x_data))
    ordered_x_data = ordered_x_data[:10]
    ordered_data = {}
    for t, d in graph_data.iteritems():
        ordered_data[t] = list(reversed(graph_data[t]))
        ordered_data[t] = ordered_data[t][:10]
    #output_bar_chart(filename + ".svg", "heatmap trends", ordered_x_data, ordered_data)

def output_bar_chart(filename, title, x_data, graph_data):
    debug_print(sys._getframe().f_code.co_name)
    chart = pygal.Bar(show_y_guides=False)
    chart.title = title
    chart.x_labels = x_data
    for name, array in graph_data.iteritems():
        chart.add(name, array)
    chart.render_to_file(filename)

def determine_dump_filename(data_type, category, label):
    debug_print(sys._getframe().f_code.co_name)
    dirname = "data/"
    if data_type != "":
        dirname += data_type + "/"
    if category != "":
        if category.startswith("url_keyword_"):
            dirname += "url_keyword/"
        if category.startswith("keyword_"):
            dirname += "keyword/"
        if category != "monitored_hashtags":
            if category.startswith("monitored_"):
                dirname += "monitored_hashtag/"
        dirname += category + "/"
    if category == "":
        if label.startswith("url_keyword_"):
            dirname += "url_keyword/"
        if label.startswith("keyword_"):
            dirname += "keyword/"
        if label != "monitored_hashtags":
            if label.startswith("monitored_"):
                dirname += "monitored/"
    filename = dirname + label + ".txt"
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return filename

def dump_dicts(raw_data, data_type, category, label):
    debug_print(sys._getframe().f_code.co_name)
    if len(raw_data) < 1:
        return
    filename = determine_dump_filename(data_type, category, label)
    debug_print("dump_dicts: filename: " + filename)

    if type(raw_data) is not dict:
        print var_name
        print raw_data
        sys.exit(0)

    handle = io.open(filename, 'w', encoding='utf-8')
    for tag, count in sorted(raw_data.items(), key=lambda x:x[1], reverse=True):
        handle.write(unicode(count) + "\t" + unicode(tag) +  u"\n")
    handle.close()

def dump_associations():
    debug_print(sys._getframe().f_code.co_name)
    filename = "data/custom/associations.csv"
    handle = io.open(filename, "w", encoding='utf-8')
    handle.write(u"Name, Links Out, Links In, Two Way\n")
    links_out = get_all_associations("links_out")
    links_in = get_all_associations("links_in")
    two_way = get_all_associations("two_way")
    if links_in is not None:
        links_in_written = 0
        links_out_written = 0
        two_way_written = 0
        for name, count in sorted(links_in.items(), key=lambda x:x[1], reverse=True):
            if links_in_written > 200 and links_out_written > 200 and two_way_written > 200:
                break
            links_in_count = count
            links_out_count = 0
            two_way_count = 0
            if name in links_out:
                links_out_count = links_out[name]
            if name in two_way:
                two_way_count = two_way[name]
            if links_in_count > conf["params"]["min_top_score"]:
                if links_in_written <= 200:
                    handle.write(unicode(name) + u", " + unicode(links_out_count) + u", " + unicode(links_in_count) + u", " + unicode(two_way_count) + u"\n")
                    links_in_written += 1
                    continue
            if links_out_count > conf["params"]["min_top_score"]:
                if links_out_written <= 200:
                    handle.write(unicode(name) + u", " + unicode(links_out_count) + u", " + unicode(links_in_count) + u", " + unicode(two_way_count) + u"\n")
                    links_out_written += 1
                    continue
            if two_way_count > conf["params"]["min_top_score"]:
                if two_way_written <= 200:
                    handle.write(unicode(name) + u", " + unicode(links_out_count) + u", " + unicode(links_in_count) + u", " + unicode(two_way_count) + u"\n")
                    two_way_written += 1
                    continue
    handle.close

def dump_interarrivals():
    debug_print(sys._getframe().f_code.co_name)
    if conf["config"]["log_interarrivals"] == False:
        return
    s = get_all_interarrival_stdevs()
    if s is not None:
        for category, raw_data in s.iteritems():
            filename = "data/custom/interarrivals_" + category + ".txt"
            output_string = u""
            output_data = {}
            for name, value in raw_data.iteritems():
                output_data[name] = str(value)
            for name, count in sorted(output_data.items(), key=lambda x:x[1], reverse=True):
                count_str = "%.2f" % float(count)
                output_string += unicode(count_str) + u"\t" + unicode(name) + u"\n"
            debug_print("Writing custom file: " + filename)
            handle = io.open(filename, 'w', encoding='utf-8')
            handle.write(output_string)
            handle.close()

def dump_userinfo():
    debug_print(sys._getframe().f_code.co_name)
    userinfo_data = get_all_userinfo()
    all_users_data = get_category_data("users", "all_users")
    if userinfo_data is None:
        return
    userinfo_order = ["suspiciousness_reasons", "suspiciousness_score", "account_created_at", "num_tweets", "tweets_per_day", "tweets_per_hour", "favourites_count", "listed_count", "friends_count", "followers_count", "follower_ratio", "source", "default_profile", "default_profile_image", "protected", "verified", "links_out", "links_in", "two_way", "interarrival_stdev", "interarrival_av", "reply_stdev", "reply_av", "retweet_stdev", "retweet_av", "tweets_seen", "replies_seen", "reply_percent", "retweets_seen", "retweet_percent", "mentions_seen", "mentioned", "fake_news_seen", "fake_news_percent", "used_hashtags", "description_matched", "identifiers_matched", "positive_words", "negative_words", "positive_hashtags", "negative_hashtags", "user_id_str"]
    num_suspicious = 0
    bot_tweets = 0
    num_bots = 0
    num_demographic = 0
    demographic_tweets = 0
    num_all_users = 0
    debug_print("dumping userinfo data")
    for category, raw_data in userinfo_data.iteritems():
        if category == "all_users":
            num_all_users += 1
        filename = "data/custom/userinfo_" + category + ".csv"
        debug_print("Writing userinfo: " + filename)
        handle = io.open(filename, 'w', encoding='utf-8')
        handle.write(u"screen_name, ")
        handle.write(u", ".join(map(unicode, userinfo_order)) + u"\n")
        for name, data in raw_data.iteritems():
            num_suspicious += 1
            handle.write(unicode(name))
            for key in userinfo_order:
                handle.write(u", ")
                element = ""
                if key in data:
                    if key == "suspiciousness_reasons":
                        if "suspicious" in data[key]:
                            num_demographic += 1
                            record_demographic(name)
                            if name in all_users_data:
                                demographic_tweets += int(all_users_data[name])
                        if "high activity" in data[key]:
                            num_bots += 1
                            record_bot_list(name)
                            if name in all_users_data:
                                bot_tweets += int(all_users_data[name])
                    data_type = type(data[key])
                    if data_type is int:
                        element = "%.2f" % data[key]
                    elif data_type is float:
                        element = "%.2f" % data[key]
                    else:
                        element = data[key]
                    if element is None:
                        element = 0.00
                    handle.write(unicode(element))
            handle.write(u"\n")
        handle.close
    debug_print("calculating bot influence")
    set_counter("userinfo_suspicious", num_suspicious)
    set_counter("userinfo_all_users", num_all_users)
    set_counter("bot_count", num_bots)
    set_counter("demographic_count", num_demographic)
    set_counter("bot_tweets", bot_tweets)
    set_counter("demographic_tweets", demographic_tweets)
    tweets_processed = get_counter("tweets_processed")
    bot_percent = float(float(bot_tweets)/float(tweets_processed))*100
    demographic_percent = float(float(demographic_tweets)/float(tweets_processed))*100
    set_counter("bot_tweet_percentage", bot_percent)
    set_counter("demographic_tweet_percentage", demographic_percent)

def dump_highly_retweeted():
    debug_print(sys._getframe().f_code.co_name)
    filename = "data/custom/highly_retweeted.txt"
    handle = io.open(filename, 'w', encoding='utf-8')
    rt_data = get_highly_retweeted()
    total = 0
    highest = 0
    for text, count in sorted(rt_data.items(), key=lambda x:x[1], reverse=True):
        total += 1
        if count > highest:
            highest = count
        handle.write(unicode(count) + u"\t" + unicode(text) + u"\n")
    handle.close()
    set_counter("highly_retweeted", total)
    set_counter("highest_retweeted", highest)


###################
# Data dump routine
###################
def dump_data():
    debug_print(sys._getframe().f_code.co_name)
    debug_print("Starting dump...")

    debug_print("Dumping periodic data")
    data_types = ["per_hour_data", "per_day_data"]
    for d in data_types:
        datestring = get_datestring(d, 0)
        for label in get_categories_from_periodic_data(d):
            if label is not None:
                if datestring in label:
                    raw_data = get_category_from_periodic_data(d, label)
                    m = re.search("^(.+)_[0-9]+$", label)
                    if m is not None:
                        category = m.group(1)
                        dirname = d + "/" + category
                        dump_dicts(raw_data, d, category, label)

    debug_print("Dumping users")
    dump_type = ""
    for category in get_categories_from_storage(dump_type + "users"):
        raw_data = get_category_data(dump_type + "users", category)
        dump_dicts(raw_data, "overall", "", category)

    debug_print("Dumping metadata")
    for category in get_categories_from_storage(dump_type + "metadata"):
        raw_data = get_category_data(dump_type + "metadata", category)
        dump_dicts(raw_data, "overall", "", category)

    debug_print("Dumping heatmaps")
    heatmaps = get_all_heatmaps()
    for name, value in heatmaps.iteritems():
        dump_heatmap(name)

    debug_print("Dumping interarrivals.")
    dump_interarrivals()

    debug_print("Dumping heatmap comparison.")
    dump_heatmap_comparison()

    debug_print("Dumping userinfo.")
    dump_userinfo()

    debug_print("Dumping associations.")
    dump_associations()

    debug_print("Dumping counters.")
    dump_highly_retweeted()
    dump_counters()
    dump_languages_graph()
    dump_captured_languages_graph()
    dump_targets_graph()
    dump_keywords_graph()
    dump_tweet_volume_graphs()
    dump_sentiment_volume_graphs()
    process_retweet_frequency()
    dump_retweet_spikes()
    dump_suspicious_retweets()
    dump_bot_list()
    dump_demographic_list()
    dump_demographic_detail()

    debug_print("Completed dump...")
    return

def dump_graphs():
    debug_print(sys._getframe().f_code.co_name)
    if "config" in conf:
        if "dump_graphs" in conf["config"]:
            if conf["config"]["dump_graphs"] == True:
                debug_print("Dumping trends and graphs.")
                data_types = ["per_hour_data", "per_day_data"]
                for d in data_types:
                    dump_overall_data_graphs(d)
                    #dump_periodic_data_trends(d)
                    #dump_periodic_data_graphs(d)
                    #dump_overall_object_graphs(d)


###############################
# Tweet processing main routine
###############################
def process_tweet(status):
    debug_print(sys._getframe().f_code.co_name)
    info = {}
    info["processing_start_time"] = int(time.time())
    increment_counter("tweets_processed")

    if "created_at" not in status:
        return
    if status["created_at"] is None:
        return

    info["tweet_time_readable"] = status["created_at"]
    tweet_time_object = time_string_to_object(info["tweet_time_readable"])
    info["tweet_time_unix"] = time_object_to_unix(tweet_time_object)
    info["datestring"] = tweet_time_object.strftime("%Y%m%d%H")

    info["lang"] = status["lang"]
    info["tweet_id"] = status["id_str"]
    info["text"] = status["text"]
    info["name"] = status["screen_name"]
    if info["lang"] is None or info["name"] is None or info["text"] is None or info["tweet_id"] is None:
        return

    increment_heatmap("all_tweets", tweet_time_object)
    increment_heatmap("tweets_" + info["lang"], tweet_time_object)
    add_timeline_data(info["tweet_time_readable"], info["name"], "tweeted", info["text"], info["tweet_id"])
    add_data("users", "all_users", info["name"])
    increment_per_hour("all_users", info["datestring"], info["name"])
    add_interarrival("all_tweets", info["name"], info["tweet_time_unix"])

    fields = ["user_id_str", "source", "account_created_at", "screen_name", "statuses_count", "favourites_count", "followers_count", "listed_count", "friends_count", "default_profile", "default_profile_image", "protected", "verified"]
    for f in fields:
        if f in status:
            if type(status[f]) is bool:
                if status[f] is True:
                    info[f] = "Yes"
                else:
                    info[f] = "No"
            info[f] = status[f]
        else:
            info[f] = "Unknown"
    if "source" in info:
        info["source"] = info["source"].replace(",", " ")
        add_data("metadata", "source", info["source"])
        add_graphing_data("sources", info["name"], info["source"])
    if "retweet_count" in status:
        info["retweet_count"] = status["retweet_count"]
        if "retweet_text" in status:
            info["retweet_text"] = status["retweet_text"]
        if int(info["retweet_count"]) > 500 and "retweet_text" in info:
            printable_text = info["retweet_text"].replace("\n", " ")
            record_highly_retweeted(printable_text, info["retweet_count"])
    info["account_age_days"] = 0
    info["num_tweets"] = 0
    if "account_created_at" in info:
        if "statuses_count" in info:
            created_date = time_string_to_object(info["account_created_at"])
            info["num_tweets"] = info["statuses_count"]
            delta = datetime.today() - created_date
            if delta is not None:
                info["account_age_days"] = delta.days
                if info["num_tweets"] > 0 and info["account_age_days"] > 0:
                    info["tweets_per_day"] = float(info["num_tweets"])/float(info["account_age_days"])
                    info["tweets_per_hour"] = float(info["num_tweets"])/float(info["account_age_days"] * 24)
                else:
                    info["tweets_per_day"] = 0
                    info["tweets_per_hour"] = 0
    if "followers_count" in info and "friends_count" in info:
        if info["friends_count"] > 0 and info["followers_count"] > 0:
            info["follower_ratio"] = float(info["friends_count"])/float(info["followers_count"])
        else:
            info["follower_ratio"] = 0.0
    info["interarrival_stdev"], info["interarrival_av"] = calculate_interarrival_statistics(get_interarrival("all_tweets", info["name"]))

# Replies
    info["replied_to"] = ""
    info["reply_stdev"] = 0.0
    info["reply_av"] = 0
    if "in_reply_to_screen_name" in status:
        info["replied_to"] = status["in_reply_to_screen_name"]
        if info["replied_to"] is not None:
            add_graphing_data("replies", info["name"], info["replied_to"])
            add_timeline_data(info["tweet_time_readable"], info["name"], "replied to", info["replied_to"], info["tweet_id"])
            add_data("users", "repliers", info["name"])
            increment_per_hour("repliers", info["datestring"], info["name"])
            add_interarrival("replies", info["name"], info["tweet_time_unix"])
            info["reply_stdev"], info["reply_av"] = calculate_interarrival_statistics(get_interarrival("replies", info["name"]))
            if info["reply_stdev"] > 0.0:
                add_interarrival_stdev("replies", info["name"], info["reply_stdev"])

# Retweets
    info["retweeted_name"] = ""
    info["retweet_stdev"] = 0.0
    info["retweet_av"] = 0
    if "retweeted_screen_name" in status:
        info["retweeted_name"] = status["retweeted_screen_name"]
        if info["retweeted_name"] is not None:
            add_data("users", "retweeters", info["name"])
            retweet_id = ""
            retweet_text = ""
            retweet_url = ""
            if "retweet_text" in status:
                retweet_text = status["retweet_text"]
            if "retweet_id_str" in status:
                retweet_id = status["retweet_id_str"]
            if "retweeted_tweet_url" in status:
                retweet_url = status["retweeted_tweet_url"]
            record_retweet_frequency(retweet_text, retweet_url, info["tweet_time_unix"], retweet_id, info["name"])
            if "retweeted_user" in status:
                retweeted_user = status["retweeted_user"]
                min_account_age = 60*60*24*20
                retweet_unworthiness = 0
                account_age = None
                followers = None
                tweets = None
                if "created_at" in retweeted_user:
                    c = retweeted_user["created_at"]
                    account_age = seconds_since_twarc_time(c)
                if "followers_count" in retweeted_user:
                    followers = retweeted_user["followers_count"]
                if "statuses_count" in retweeted_user:
                    tweets = retweeted_user["statuses_count"]
                if account_age is not None and account_age < min_account_age:
                    retweet_unworthiness += 1
                if followers is not None and followers < 50:
                    retweet_unworthiness += 1
                if tweets is not None and tweets < 50:
                    retweet_unworthiness += 1
                if retweet_unworthiness >= 2:
                    if "retweet_count" in info:
                        if info["retweet_count"] > 10:
                            info["retweeted_suspicious"] = True
                            if "quote_tweet" not in status:
                                record_suspicious_retweet(retweet_text, retweet_url, info["tweet_time_unix"], retweet_id, info["name"], info["retweeted_name"], account_age, followers, tweets, info["retweet_count"])
                                increment_counter("suspicious_retweets")
            increment_heatmap("retweets", tweet_time_object)
            increment_per_hour("retweeters", info["datestring"], info["name"])
            add_graphing_data("retweets", info["name"], info["retweeted_name"])
            add_timeline_data(info["tweet_time_readable"], info["name"], "retweeted", info["retweeted_name"], info["tweet_id"])
            add_interarrival("retweets", info["name"], info["tweet_time_unix"])
            info["retweet_stdev"], info["retweet_av"] = calculate_interarrival_statistics(get_interarrival("retweets", info["name"]))
            if info["retweet_stdev"] > 0.0:
                add_interarrival_stdev("retweets", info["name"], info["retweet_stdev"])

# Quote tweets
    info["quote_tweeted_name"] = ""
    if "quoted_screen_name" in status:
        info["quote_tweeted_name"] = status["quoted_screen_name"]
        if info["quote_tweeted_name"] is not None:
            add_graphing_data("quote_tweets", info["name"], info["quote_tweeted_name"])
            add_timeline_data(info["tweet_time_readable"], info["name"], "quote_tweeted", info["quote_tweeted_name"], info["tweet_id"])

# Tweet words, description words
    info["positive_words"] = 0
    info["negative_words"] = 0
    info["tweet_identifier_matches"] = get_tweet_identifier_matches(info["text"])
    if conf["config"]["log_words"] is True:
        tokens = strip_stopwords(tokenize(info["text"]), info["lang"])
        if len(tokens) > 0:
            label = "words_" + info["lang"]
            for t in tokens:
                increment_per_hour(label, info["datestring"], t)
                for ta in targets:
                    if t == ta:
                        increment_counter("target_" + ta)
                        add_data("metadata", "targets", ta)
                        increment_heatmap("target_" + ta, tweet_time_object)
                        increment_per_hour("targets", info["datestring"], ta)
                        if "sentiment" in status:
                            record_sentiment(t, info["tweet_time_readable"], status["sentiment"])
                if add_data("metadata", label, t) is True:
                    increment_counter("tweet_words_seen")
        pos_words = get_positive_words(tokens)
        info["positive_words"] = len(pos_words)
        if len(pos_words) > 0:
            for w in pos_words:
                add_data("metadata", "positive_words", w)
                increment_per_hour("positive_words", info["datestring"], w)
                increment_heatmap("positive_words", tweet_time_object)
                increment_counter("positive_words")
        neg_words = get_negative_words(tokens)
        info["negative_words"] = len(neg_words)
        if len(neg_words) > 0:
            for w in neg_words:
                add_data("metadata", "negative_words", w)
                increment_per_hour("negative_words", info["datestring"], w)
                increment_heatmap("negative_words", tweet_time_object)
                increment_counter("negative_words")
        info["negative_words"] += get_userinfo_value("all_users", info["name"], "negative_words")
        info["positive_words"] += get_userinfo_value("all_users", info["name"], "positive_words")
        if "description" in status:
            tokens = strip_stopwords(tokenize(status["description"]), info["lang"])
            if len(tokens) > 0:
                for t in tokens:
                    if add_data("metadata", "description_words", t) is True:
                        increment_counter("description_words_seen")
            info["description_matches"] = get_description_matches(status["description"])

# Hashtags
    info["hashtags"] = []
    info["positive_hashtags"] = 0
    info["negative_hashtags"] = 0
    if "hashtags" in status:
        tags = status["hashtags"]
        if len(tags) > 0:
            for t in tags:
                if t is not None:
                    info["hashtags"].append(t)
                    add_data("metadata", "all_hashtags", t)
                    add_graphing_data("hashtags", info["name"], t)
                    add_timeline_data(info["tweet_time_readable"], info["name"], "used hashtag", t, info["tweet_id"])
                    increment_per_hour("all_hashtags", info["datestring"], t)
                    for h in conf["settings"]["monitored_hashtags"]:
                        if h == t:
                            label = "monitored_" + h + "_hashtag"
                            increment_heatmap(label, tweet_time_object)
                            increment_per_hour("monitored_hashtags", info["datestring"], h)
                            add_data("metadata", "monitored_hashtags", t)
                            add_data("users", label + "_tweeters", info["name"])
                            increment_per_hour(label + "_tweeters", info["datestring"], info["name"])
                            increment_per_hour(label + "_tweets", info["datestring"], info["text"])
                            increment_counter(label + "_tweets")
                            if "sentiment" in status:
                                record_sentiment(h, info["tweet_time_readable"], status["sentiment"])
            pos_tags = get_positive_hashtags(info["hashtags"])
            info["positive_hashtags"] = len(pos_tags)
            if len(pos_tags) > 0:
                for w in pos_tags:
                    add_data("metadata", "positive_hashtags", w)
                    increment_per_hour("positive_hashtag_tweeters", info["datestring"], info["name"])
                    increment_per_hour("positive_hashtag_tweets", info["datestring"], info["text"])
                    increment_counter("positive_hashtag_tweets")
            neg_tags = get_negative_hashtags(info["hashtags"])
            info["negative_hashtags"] = len(neg_tags)
            if len(neg_tags) > 0:
                for w in neg_tags:
                    add_data("metadata", "negative_hashtags", w)
                    increment_per_hour("negative_hashtag_tweeters", info["datestring"], info["name"])
                    increment_per_hour("negative_hashtag_tweets", info["datestring"], info["text"])
                    increment_counter("negative_hashtag_tweets")

            info["positive_hashtags"] += get_userinfo_value("all_users", info["name"], "positive_hashtags")
            info["negative_hashtags"] += get_userinfo_value("all_users", info["name"], "negative_hashtags")

# URLs
    info["urls"] = []
    if "urls" in status:
        info["urls"] = status["urls"]
        if len(info["urls"]) > 0:
            increment_counter("tweets_with_urls")
            for u in info["urls"]:
                if u is not None:
                    add_data("metadata", "all_urls", u)
                    increment_per_hour("all_urls", info["datestring"], u)
                    add_timeline_data(info["tweet_time_readable"], info["name"], "tweeted url", u, info["tweet_id"])
                    if "twitter.com" not in u:
                        increment_heatmap("urls_not_twitter", tweet_time_object)
                        add_data("metadata", "urls_not_twitter", u)
                        increment_per_hour("urls_not_twitter", info["datestring"], u)
                    for k in conf["settings"]["url_keywords"]:
                        if k in u:
                            label = "url_keyword_" + k
                            add_data("metadata", label, u)
                            increment_per_hour("url_keywords", info["datestring"], k)
                            add_data("metadata", "url_keywords", k)
                            increment_per_hour(label, info["datestring"], u)
                            increment_per_hour(label + "_tweeter", info["datestring"], info["name"])
                            increment_per_hour(label + "_tweets", info["datestring"], info["text"])
                            increment_counter(label + "_tweets")
                            add_data("users", label + "_tweeter", info["name"])
                            add_timeline_data(info["tweet_time_readable"], info["name"], "tweeted keyword url about", k, info["tweet_id"])
                            increment_heatmap("keyword_urls", tweet_time_object)
                    fake_news_found = False
                    for f in conf["corpus"]["fake_news_sources"]:
                        if f in u:
                            fake_news_found = True
                            increment_heatmap("fake_news", tweet_time_object)
                            increment_counter("fake_news_tweets")
                            increment_per_hour("fake_news_urls", info["datestring"], u)
                            increment_per_hour("fake_news_tweeters", info["datestring"], info["name"])
                            add_data("users", "fake_news_tweeters", info["name"])
                            add_data("metadata", "fake_news_urls", u)
                            add_timeline_data(info["tweet_time_readable"], info["name"], "tweeted fake news link", u, info["tweet_id"])
                            for h in info["hashtags"]:
                                increment_per_hour("fake_news_hashtags", info["datestring"], h)
                                add_data("metadata", "fake_news_hashtags", h)
                    if fake_news_found == True:
                        increment_counter("tweets_with_fake_news_urls")

# Mentioned
    if "mentioned" in status:
        mentions = []
        mentions = status["mentioned"]
        if len(mentions) > 0:
            for m in mentions:
                if m is not None:
                    add_data("users", "mentioned", m)
                    add_graphing_data("mentions", info["name"], m)
                    if m not in info["retweeted_name"] and m not in searches:
                        add_timeline_data(info["tweet_time_readable"], info["name"], "mentioned", m, info["tweet_id"])
                    increment_per_hour("mentioners", info["datestring"], info["name"])
                    increment_per_hour("mentioned", info["datestring"], m)

# Keywords
    if len(conf["settings"]["keywords"]) > 0:
        for k in conf["settings"]["keywords"]:
            if k in info["text"]:
                label = "keyword_" + k
                increment_heatmap(label, tweet_time_object)
                increment_counter(label + "_tweets")
                add_data("metadata", label + "_tweeters", info["name"])
                increment_per_hour("keywords", info["datestring"], k)
                add_data("metadata", "keywords", k)
                increment_per_hour(label + "_tweeters", info["datestring"], info["name"])
                increment_per_hour(label + "_tweets", info["datestring"], info["text"])
                add_timeline_data(info["tweet_time_readable"], info["name"], "used monitored keyword:", k, info["tweet_id"])
                add_data("metadata", "keyword_tweets", info["text"])
                if "sentiment" in status:
                    record_sentiment(k, info["tweet_time_readable"], status["sentiment"])
                for h in info["hashtags"]:
                    increment_per_hour(label + "_hashtags", info["datestring"], h)

# Suspiciousness processing here
    info["links_out"] = get_associations("links_out", info["name"])
    info["links_in"] = get_associations("links_in", info["name"])
    info["two_way"] = get_associations("two_way", info["name"])
    info["fake_news_seen"] = get_data("users", "fake_news_tweeters", info["name"])
    info["tweets_seen"] = get_data("users", "all_users", info["name"])
    info["replies_seen"] = get_data("users", "repliers", info["name"])
    info["retweets_seen"] = get_data("users", "retweeters", info["name"])
    info["mentions_seen"] = get_data("users", "mentioners", info["name"])
    info["mentioned"] = get_data("users", "mentioned", info["name"])
    hts = get_user_hashtag_data(info["name"])
    if len(hts) > 0:
        info["used_hashtags"] = "[" + "|".join(hts) + "]"
    info["reply_percent"] = 0
    info["retweet_percent"] = 0
    info["fake_news_percent"] = 0
    if info["replies_seen"] > 0 and info["tweets_seen"] > 0:
        info["reply_percent"] = float(info["replies_seen"])/float(info["tweets_seen"]) * 100.00
    if info["retweets_seen"] > 0 and info["tweets_seen"] > 0:
        info["retweet_percent"] = float(info["retweets_seen"])/float(info["tweets_seen"]) * 100.00
    if info["fake_news_seen"] > 0 and info["tweets_seen"] > 0:
        info["fake_news_percent"] = float(info["fake_news_seen"])/float(info["tweets_seen"]) * 100.00

    if get_counter("average_high_users_all_users") > 0.0:
        count_mod = float(get_counter("tweets_processed")/get_counter("average_high_users_all_users"))
    else:
        count_mod = float(1.0)

    min_tweets = int(get_counter("tweets_processed")/count_mod)
    if min_tweets < conf["params"]["min_tweets_for_suspicious"]:
        min_tweets = conf["params"]["min_tweets_for_suspicious"]
    min_tweets_per_day = 100
    stdev_multiplier = 10
    generic_multiplier = 100
    min_percentage = 90
    min_follow_ratio = 1.2
    follow_ratio_multiplier = 5
    min_followers = 150
    min_account_age_days = 30
    min_stdev = 3
    crazy_threshold = 500
    suspiciousness_threshold = 800

    debug_print("Calculating suspiciousness...")
    debug_print("Average tweets from top 10 most active users: " + str(get_counter("average_high_users_all_users")))
    debug_print("Minimum tweets to analyze: " + str(min_tweets))

    info["suspiciousness_score"] = 0
    record_user = False
    info["suspiciousness_reasons"] = ""

# Look for accounts with no description
    if "description" not in status:
        info["suspiciousness_score"] += generic_multiplier
        info["suspiciousness_reasons"] += "[no description]"

# Did this user publish a suspicious retweet
    if "retweeted_suspicious" in info:
        info["suspiciousness_score"] += generic_multiplier
        info["suspiciousness_reasons"] += "[suspicious retweet]"

# Record demographic data
    current_descs = []
    current_tweet_idents = []
    if "description_matches" in info:
        current_descs = info["description_matches"] 
    if "tweet_identifier_matches" in info:
        current_tweet_idents = info["tweet_identifier_matches"]
    if len(current_descs) > 0  or len(current_tweet_idents) > 0:
        record_demographic_detail(info["name"], current_descs, current_tweet_idents)
    if exists_demographic_detail(info["name"]):
        current_descs, current_tweet_idents = get_demographic_detail(info["name"])

# Count suspicious description words
    if len(current_descs) > 0:
        info["description_matched"] = "[" + "|".join(current_descs) + "]"
        info["suspiciousness_score"] += len(current_descs) * generic_multiplier * 3
        info["suspiciousness_reasons"] += "[suspicious description words]"

# Count suspicious words in tweet text
    if len(current_tweet_idents) > 0:
        info["identifiers_matched"] = "[" + "|".join(current_tweet_idents) + "]"
        info["suspiciousness_score"] += len(current_tweet_idents) * generic_multiplier * 3
        info["suspiciousness_reasons"] += "[suspicious words in tweets]"

# Look for extremely heavy account activity
    if info["tweets_per_day"] > min_tweets_per_day:
        info["suspiciousness_score"] += info["tweets_per_day"] - min_tweets_per_day
        info["suspiciousness_reasons"] += "[high activity]"

    if info["account_age_days"] > 0:
        if info["account_age_days"] < min_account_age_days:
            if info["tweets_per_day"] > min_tweets_per_day:
                info["suspiciousness_score"] += (info["tweets_per_day"] - min_tweets_per_day) * (min_account_age_days - info["account_age_days"])
                info["suspiciousness_reasons"] += "[high activity on new account]"

# Look for high activity and low follower count
    if info["followers_count"] < min_followers:
        if info["tweets_per_day"] > min_tweets_per_day:
            info["suspiciousness_score"] += info["tweets_per_day"] - min_tweets_per_day
            info["suspiciousness_reasons"] += "[high activity and low follower count]"

# Look for bot-like tweet patterns
    if info["interarrival_stdev"] > min_stdev:
        info["suspiciousness_score"] += (info["interarrival_stdev"] * stdev_multiplier)
        info["suspiciousness_reasons"] += "[interarrival pattern]"
        if info["tweets_seen"] > min_tweets:
            record_user = True
            info["suspiciousness_reasons"] += "[frequent tweets]"
    if info["reply_stdev"] > min_stdev:
        info["suspiciousness_score"] += (info["reply_stdev"] * stdev_multiplier)
        info["suspiciousness_reasons"] += "[interarrival pattern]"
        if info["tweets_seen"] > min_tweets:
            record_user = True
            info["suspiciousness_reasons"] += "[frequent replies]"
    if info["retweet_stdev"] > min_stdev:
        info["suspiciousness_score"] += (info["retweet_stdev"] * stdev_multiplier)
        info["suspiciousness_reasons"] += "[interarrival pattern]"

# Look for high percentage of replies (often used by porn bots, or to hide timeline)
    if info["reply_percent"] > min_percentage:
        if info["tweets_seen"] > min_tweets:
            info["suspiciousness_score"] += info["reply_percent"]
            info["suspiciousness_reasons"] += "[high reply percent]"

# Look for high retweet percentages
    if info["retweet_percent"] > min_percentage:
        info["suspiciousness_score"] += info["retweet_percent"]
        info["suspiciousness_reasons"] += "[high percentage of retweets]"

# Look for an abundance of fake news posts
    if info["fake_news_percent"] > min_percentage:
        info["suspiciousness_score"] += info["fake_news_percent"]
        if info["tweets_seen"] > min_tweets:
            record_user = True
            info["suspiciousness_reasons"] += "[frequent fake news tweets]"

# Look for high following, low followers ratios
    if info["followers_count"] > min_followers:
        if info["follower_ratio"] > min_follow_ratio:
            info["suspiciousness_score"] += (info["follower_ratio"] * follow_ratio_multiplier)
            if info["tweets_seen"] > min_tweets:
                record_user = True
                info["suspiciousness_reasons"] += "[follow ratio]"

# Look for old accounts with zero followers
    if info["account_age_days"] > min_account_age_days:
        if info["followers_count"] == 0:
            info["suspiciousness_score"] += generic_multiplier

    info["suspiciousness_score"] = int(info["suspiciousness_score"])
    debug_print("Suspiciousness: " + str(info["suspiciousness_score"]))

# Look for non-standard source (twitter client)
    if "source" in info:
        if is_source_legit(info["source"]) is False:
            info["suspiciousness_score"] += 300
            info["suspiciousness_reasons"] += "[non-legit Twitter client]"

# Look for patterns in username
    if "real" in info["name"].lower():
            info["suspiciousness_score"] += 300
            info["suspiciousness_reasons"] += "[real in username]"

    if info["suspiciousness_score"] > suspiciousness_threshold:
        record_user = True

# Look for users who's been seen a lot while analysis was running
    if info["tweets_seen"] > crazy_threshold:
        record_user = True
        info["suspiciousness_reasons"] += "[crazy threshold]"

    if info["suspiciousness_score"] < 1:
        record_user = False

# Not suspicious, but log users with large numbers of interactions
    if info["two_way"] > 0:
        info["suspiciousness_reasons"] += "[non-zero two_way]"

    debug_print("Preparing userinfo for " + info["name"])

    if conf["config"]["log_all_userinfo"] == True:
        add_userinfo("all_users", info["name"], info)

    if record_user == True:
        debug_print("Recording suspiciousness for " + info["name"])
        add_userinfo("suspicious", info["name"], info)
        increment_counter("suspicious_users")

    info["processing_end_time"] = int(time.time())
    processing_duration = info["processing_end_time"] - info["processing_start_time"]
    debug_print("Processing " + info["name"] + " took " + str(processing_duration))
    return

############################
# Tweet data capture routine
############################
def capture_status_items(status):
    debug_print(sys._getframe().f_code.co_name)
    global analyzer
    captured_status = {}
    if "created_at" in status:
        captured_status["created_at"] = twarc_time_to_readable(status["created_at"])
    else:
        return
    if "id_str" in status:
        captured_status["id_str"] = status["id_str"]
    else:
        return
    if "full_text" in status:
        captured_status["text"] = ' '.join(status["full_text"].split())
    elif "text" in status:
        captured_status["text"] = ' '.join(status["text"].split())
    else:
        return
    if "lang" in status:
        captured_status["lang"] = status["lang"]
        if status["lang"] == "en":
            vs = analyzer.polarity_scores(captured_status["text"])
            captured_status["sentiment"] = vs["compound"]
    else:
        return
    if "source" in status:
        source_url = status["source"]
        m = re.search("^\<.+\>(.+)\<\/a\>$", source_url)
        if m is not None:
            captured_status["source"] = m.group(1)
    if "in_reply_to_screen_name" in status:
        captured_status["in_reply_to_screen_name"] = status["in_reply_to_screen_name"]
# retweet data
    if "retweeted_status" in status:
        orig_tweet = status["retweeted_status"]
        if "id_str" in orig_tweet:
            captured_status["retweeted_tweet_id"] = orig_tweet["id_str"]
        if "user" in orig_tweet:
            if orig_tweet["user"] is not None:
                retweeted_user = orig_tweet["user"]
                captured_status["retweeted_user"] = retweeted_user
                if "retweet_count" in orig_tweet:
                    captured_status["retweet_count"] = orig_tweet["retweet_count"]
                if "full_text" in orig_tweet:
                    captured_status["retweet_text"] = orig_tweet["full_text"]
                elif "text" in orig_tweet:
                    captured_status["retweet_text"] = orig_tweet["text"]
                if "id_str" in orig_tweet:
                    captured_status["retweet_id_str"] = orig_tweet["id_str"]
                if "screen_name" in orig_tweet["user"]:
                    if retweeted_user["screen_name"] is not None:
                        captured_status["retweeted_screen_name"] = retweeted_user["screen_name"]
                        if "retweeted_tweet_id" in captured_status:
                            captured_status["retweeted_tweet_url"] = "https://twitter.com/" + captured_status["retweeted_screen_name"] + "/status/" + captured_status["retweeted_tweet_id"]
                        captured_status["retweet"] = True
# XXX Removed this functionality, since it doesn't properly return retweeted object
    """
    if "retweeted_screen_name" not in captured_status:
        rt_name = re.search("^RT @(\w+)\W", captured_status["text"])
        if rt_name is not None:
            captured_status["retweeted_screen_name"] = rt_name.group(1)
            captured_status["retweet"] = True
    """

# quote tweet data
    if "quoted_status" in status:
        orig_tweet = status["quoted_status"]
        if "id_str" in orig_tweet:
            captured_status["quoted_tweet_id"] = orig_tweet["id_str"]
        if "user" in orig_tweet:
            if orig_tweet["user"] is not None:
                captured_status["quoted_user"] = orig_tweet["user"]
                if "retweet_count" in orig_tweet:
                    captured_status["retweet_count"] = orig_tweet["retweet_count"]
                if "full_text" in orig_tweet:
                    captured_status["retweet_text"] = orig_tweet["full_text"]
                elif "text" in orig_tweet:
                    captured_status["retweet_text"] = orig_tweet["text"]
                if "screen_name" in orig_tweet["user"]:
                    if orig_tweet["user"]["screen_name"] is not None:
                        captured_status["quoted_screen_name"] = orig_tweet["user"]["screen_name"]
                        if "quoted_tweet_id" in captured_status:
                            captured_status["quoted_tweet_url"] = "https://twitter.com/" + captured_status["quoted_screen_name"] + "/status/" + captured_status["quoted_tweet_id"]
                        captured_status["quote_tweet"] = True

# entities data (hashtags, urls, mentions)
    if "entities" in status:
        entities = status["entities"]
        if "hashtags" in entities:
            for item in entities["hashtags"]:
                if item is not None:
                    if "hashtags" not in captured_status:
                        captured_status["hashtags"] = []
                    tag = item['text']
                    if tag is not None:
                        captured_status["hashtags"].append(tag.lower())
        if "urls" in entities:
            for item in entities["urls"]:
                if item is not None:
                    if "urls" not in captured_status:
                        captured_status["urls"] = []
                    url = item['expanded_url']
                    if url is not None:
                        captured_status["urls"].append(url)
        if "user_mentions" in entities:
            for item in entities["user_mentions"]:
                if item is not None:
                    if "mentions" not in captured_status:
                        captured_status["mentioned"] = []
                    mention = item['screen_name']
                    if mention is not None:
                        captured_status["mentioned"].append(mention)
# User data from tweet object
    if "user" in status:
        user_data = status["user"]
        if "id_str" in user_data:
            captured_status["user_id_str"] = user_data["id_str"]
        else:
            return
        if "screen_name" in user_data:
            captured_status["screen_name"] = user_data["screen_name"]
        else:
            return
        if "created_at" in user_data:
            captured_status["account_created_at"] = twarc_time_to_readable(user_data["created_at"])
        if "statuses_count" in user_data:
            captured_status["statuses_count"] = user_data["statuses_count"]
        if "favourites_count" in user_data:
            captured_status["favourites_count"] = user_data["favourites_count"]
        if "listed_count" in user_data:
            captured_status["listed_count"] = user_data["listed_count"]
        if "friends_count" in user_data:
            captured_status["friends_count"] = user_data["friends_count"]
        if "followers_count" in user_data:
            captured_status["followers_count"] = user_data["followers_count"]
        if "default_profile" in user_data:
            captured_status["default_profile"] = user_data["default_profile"]
        if "default_profile_image" in user_data:
            captured_status["default_profile_image"] = user_data["default_profile_image"]
        if "protected" in user_data:
            captured_status["protected"] = user_data["protected"]
        if "verified" in user_data:
            captured_status["verified"] = user_data["verified"]
        if "description" in user_data:
            desc = user_data["description"]
            if desc is not None:
                captured_status["description"] = ' '.join(desc.split())
        if "screen_name" in captured_status and "id_str" in captured_status:
            captured_status["tweet_url"] = "https://twitter.com/" + captured_status["screen_name"] + "/status/" + captured_status["id_str"]
    return captured_status


########################
# Periodically dump data
########################
def dump_event():
    debug_print(sys._getframe().f_code.co_name)
    global data, volume_file_handle
    if stopping == True:
        return
    if int(time.time()) > get_counter("previous_dump_time") + get_counter("dump_interval"):
        start_time = int(time.time())
        gathering_time = start_time - get_counter("previous_dump_time") - get_counter("dump_interval")
        output = "\n\n"
        output += "Gathering took: " + str(gathering_time) + " seconds.\n"
        if collect_only == False:
            dump_data()
        end_time = int(time.time())
        dump_time = end_time - start_time
        output += "Data dump took: " + str(dump_time) + " seconds.\n"
        graph_dump_time = 0
        if threaded == True:
            if int(time.time()) > int(get_counter("previous_graph_dump_time") + conf["params"]["graph_dump_interval"]):
                start_time = int(time.time())
                dump_graphs()
                set_counter("previous_graph_dump_time", int(time.time()))
                end_time = int(time.time())
                graph_dump_time = end_time - start_time
                output += "Graph dump took: " + str(graph_dump_time) + " seconds.\n"
        serialize_time = 0
        if threaded == True:
            if int(time.time()) > int(get_counter("previous_serialize") + conf["params"]["serialization_interval"]):
                start_time = int(time.time())
                serialize_data()
                set_counter("previous_serialize", int(time.time()))
                end_time = int(time.time())
                serialize_time = end_time - start_time
                output += "Serialization took: " + str(serialize_time) + " seconds.\n"
        current_time = int(time.time())
        processing_time = current_time - get_counter("previous_dump_time")
        if threaded == True:
            queue_length = tweet_queue.qsize()
            output += str(queue_length) + " items in the queue.\n"
            active_threads = get_active_threads()
            output += str(active_threads) + " threads active.\n"
        output += "Processed " + str(get_counter("tweets_processed_this_interval")) + " tweets during the last " + str(processing_time) + " seconds.\n"
        output += "Tweets encountered: " + str(get_counter("tweets_encountered")) + ", captured: " + str(get_counter("tweets_captured")) + ", processed: " + str(get_counter("tweets_processed")) + "\n"
        tps = float(float(get_counter("tweets_processed_this_interval"))/float(processing_time))
        output += "Tweets per second: " + str("%.2f" % tps) + "\n"
        set_counter("tweets_processed_this_interval", 0)
        set_counter("previous_dump_time", int(time.time()))
        set_counter("processing_time", processing_time)
        #set_counter("dump_interval", conf["params"]["default_dump_interval"] + processing_time)
        increment_counter("successful_loops")
        output += "Executed " + str(get_counter("successful_loops")) + " successful loops.\n"
        total_running_time = end_time - get_counter("script_start_time")
        set_counter("total_running_time", total_running_time)
        output += "Running as " + acct_name + " since " + script_start_time_str + " (" + str(total_running_time) + " seconds)\n"
        current_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
        output += "Current time is: " + current_time_str + "\n\n"
        record_tweet_volume("all_tweets", current_time_str, tps)
        if exists_counter("average_tweets_per_second"):
            old_average = get_counter("average_tweets_per_second")
            new_average = float((float(tps) + float(old_average)) / 2)
            set_counter("average_tweets_per_second", new_average)
            if tps > old_average * conf["params"]["tweet_spike_minimum"]:
                record_volume_spike(old_average, tps)
        else:
            set_counter("average_tweets_per_second", tps)
        print output
        volume_file_handle.write(current_time_str + "\t" + str("%.2f" % tps) + "\n")
        return

def record_volume_spike(av, tps):
    filename = "data/custom/tweet_spikes.txt"
    current_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    percent_change = 0
    if av > 0:
        percent_change = ((tps-av)/av)*100
    handle = io.open(filename, "a", encoding="utf-8")
    handle.write(u"Tweet spike at:\t" + unicode(current_time_str) + u"\n")
    handle.write(u"Average tweets per second:\t" + unicode("%.2f"%av) + u"\n")
    handle.write(u"Recorded tweets per second:\t" + unicode("%.2f"%tps) + u"\n")
    handle.write(u"Percent change:\t" + unicode("%.2f"%percent_change) + u"%\n")
    handle.write(u"\n")
    handle.close()

############################
# Periodically reload config
############################
def reload_config_event():
    debug_print(sys._getframe().f_code.co_name)
    if int(time.time()) > get_counter("previous_config_reload") + conf["params"]["config_reload_interval"]:
        reload_settings()
        set_counter("previous_config_reload", int(time.time()))

###############################
# Periodically cache/purge data
###############################
def purge_event():
    debug_print(sys._getframe().f_code.co_name)
    if int(time.time()) > get_counter("previous_purge") + conf["params"]["purge_interval"]:
        purged = purge_data()
        add_to_counter("items_purged", purged)
        print
        print "Purge mode: " + conf["params"]["data_handling"]
        print "Current TTL: " + str(conf["params"]["time_to_live"])
        print "Purged " + str(purged) + " items."
        set_counter("previous_purge", int(time.time()))

############################
# Manage all periodic events
############################
def periodic_events():
    dump_event()
    reload_config_event()
    purge_event()

#########################
# Tweet processing thread
#########################
def tweet_processing_thread():
    debug_print(sys._getframe().f_code.co_name)
    while True:
        item = tweet_queue.get()
        dump_tweet_to_disk(item)
        if collect_only == False:
            process_tweet(item)
        periodic_events()
        tweet_queue.task_done()
    return

##########################################
# Callback when a new tweet is encountered
##########################################
def process_status(status):
    debug_print(sys._getframe().f_code.co_name)
    global tweets_captured, tweets_encountered
    captured_status = {}
    increment_counter("tweets_encountered")
    lang = ""
    datestring = None
    if "created_at" in status:
        datestring = twarc_time_to_object(status["created_at"]).strftime("%Y%m%d%H")
    if "lang" in status:
        lang = status["lang"]
        if datestring is not None:
            increment_per_hour("lang", datestring, lang)
        increment_counter("tweets_" + lang)
        add_data("metadata", "lang", lang)
        if len(conf["settings"]["monitored_langs"]) > 0:
            if lang not in conf["settings"]["monitored_langs"]:
                debug_print("Skipping tweet of lang: " + lang)
                sys.stdout.write("-")
                sys.stdout.flush()
                return
    captured_status = capture_status_items(status)
    if captured_status is not None:
        increment_counter("captured_tweets_" + lang)
        increment_counter("tweets_captured")
        increment_counter("tweets_processed_this_interval")
        if datestring is not None:
            increment_per_hour("captured_lang", datestring, lang)
        add_data("metadata", "captured_lang", lang)
        if threaded == True:
            tweet_queue.put(captured_status)
            if get_active_threads() < 2:
                if restart == True:
                    increment_counter("worker_thread_restarts")
                    start_thread()
                else:
                    cleanup()
                    sys.exit(0)
        else:
            dump_tweet_to_disk(captured_status)
            if collect_only == False:
                process_tweet(captured_status)
            periodic_events()
    sys.stdout.write("#")
    sys.stdout.flush()
    return

##############
# Process args
##############
def process_args(args):
    debug_print(sys._getframe().f_code.co_name)
    global restart, threaded, debug, test, collect_only
    for arg in sys.argv:
        print "Got arg: " + arg
        if "restart" in arg:
            restart = True
        if "threaded" in arg:
            threaded = True
        if "not_threaded" in arg:
            threaded = False
        if "debug" in arg:
            debug = True
        if "test" in arg:
            test = True
        if "collect" in arg:
            collect_only = True

def get_active_threads():
    debug_print(sys._getframe().f_code.co_name)
    return len(threading.enumerate())

def start_thread():
    debug_print(sys._getframe().f_code.co_name)
    global tweet_queue
    print "Starting processing thread..."
    tweet_queue = Queue.Queue()
    t = threading.Thread(target=tweet_processing_thread)
    t.daemon = True
    t.start()
    return

def get_tweet_stream(query):
    debug_print(sys._getframe().f_code.co_name)
    if query != "":
        print "Search: " + query
        for tweet in t.filter(track = query):
            process_status(tweet)
    else:
        print "Getting 1% sample."
        for tweet in t.sample():
            process_status(tweet)

#########################################
# Main routine, called when script starts
#########################################
if __name__ == '__main__':
    restart = False
    if len(sys.argv) > 1:
        process_args(sys.argv)

    if debug == True:
        exit_correctly = True
        restart = False
    if exit_correctly == True:
        threaded = False
        restart = False

    init_tweet_processor()
    dump_file_handle = open("data/raw/raw.json", "a")
    volume_file_handle = open("data/_tweet_volumes.txt", "a")
    analyzer = SentimentIntensityAnalyzer()

# Start a thread to process incoming tweets
    if threaded == True:
        start_thread()
    else:
        print "Threading disabled"

# Start twitter stream
    acct_name, consumer_key, consumer_secret, access_token, access_token_secret = get_account_credentials()
    t = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)
    print "Signing in as: " + acct_name
    print "Preparing stream"
    targets = read_config_unicode("config/targets.txt")
    if len(targets) > 0:
        searches = targets
    query = ""
    if len(searches) > 0:
        query = ",".join(searches)
    script_start_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    while True:
        set_counter("successful_loops", 0)
        if(restart == True):
            print "Restart mode: on"
        else:
            print "Restart mode: off"
        if exit_correctly is True:
            get_tweet_stream(query)
        else:
            try:
                get_tweet_stream(query)
            except KeyboardInterrupt:
                print "Keyboard interrupt..."
                cleanup()
                sys.exit(0)
            except:
                if restart == True:
                    print
                    print "Something exploded..."
                    increment_counter("main_thread_restarts")
                    log_stacktrace()
                    cleanup()
                    pass
                else:
                    cleanup()
                    sys.exit(0)
