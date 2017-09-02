# -*- coding: utf-8 -*-
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream, API, Cursor
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


# Split into collector and processor
# Collector appends each tweet json to a file
# Thread for appending data should keep file handle open
# Should also "rotate" data after a specified time interval
# Processor reads json line by line and "plays back" tweet history
# Dumps data and graphs and then exits
# Remember to record start and end processing time
# Also alter the script for outputting "per hour data" over the range of hours seen
# Also: remember to record the first time an object is seen

##################
# Global variables
##################
restart = False
debug = False
test = False
exit_correctly = False
searches = []
tweet_queue = None
targets = []
data = {}
conf = {}
acct_name = ""
script_start_time_str = ""

def init_params():
    global conf
    conf["params"] = {}
    conf["params"]["default_dump_interval"] = 10
    conf["params"]["config_reload_interval"] = 5
    conf["params"]["serialization_interval"] = 900
    conf["params"]["graph_dump_interval"] = 60
    conf["params"]["min_top_score"] = 10
    conf["params"]["min_tweets_for_suspicious"] = 10
    conf["params"]["data_handling"] = "purge"
    conf["params"]["purge_interval"] = 300
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
    conf["config"]["dump_interarrivals"] = True
    conf["config"]["dump_timeline_data"] = False
    conf["config"]["dump_raw_data"] = False
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

def increment_heatmap(name, week, weekday, hour):
    debug_print(sys._getframe().f_code.co_name)
    global data
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
    if exists_storage_large("interarrivals", "previous_tweeted", name):
        previous_tweet_time = int(get_storage_large("interarrivals", "previous_tweeted", name))
        delta = tweet_time - previous_tweet_time
        if delta > 0:
            return record_interarrival(category, name, delta)
    set_storage_large("interarrivals", "previous_tweeted", name, str(tweet_time))

def add_interarrival_stdev(category, name, value):
    debug_print(sys._getframe().f_code.co_name)
    return set_storage("interarrival_stdevs", category, name, value)

def get_all_interarrival_stdevs():
    debug_print(sys._getframe().f_code.co_name)
    return get_all_storage("interarrival_stdevs")

def set_previous_seen(category, item):
    debug_print(sys._getframe().f_code.co_name)
    timestamp = int(time.time())
    set_storage_large("previous_seen", category, item, timestamp)

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

def get_from_users(category, name):
    debug_print(sys._getframe().f_code.co_name)
    return get_data("users", category, name)

def get_from_metadata(category, name):
    debug_print(sys._getframe().f_code.co_name)
    return get_data("metadata", category, name)

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


##################
# Helper functions
##################
def debug_print(string):
    if debug == True:
        print string

def time_object_to_readable(time_object):
    return time_object.strftime("%Y-%m-%d %H:%M:%S")

def time_string_to_object(time_string):
    return datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S')

def time_object_to_unix(time_object):
    return int(time_object.strftime("%s"))

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
    conf["config"] = read_settings("config/settings.txt")

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
    if "dump_raw_data" in conf["config"]:
        if conf["config"]["dump_raw_data"] == True:
            if "id_str" in item:
                filename = "data/raw/" + item["id_str"] + ".json"
                handle = open(filename, 'w')
                json.dump(item, handle, indent=4)
                handle.close()
    return

def add_timeline_data(date, name, action, item, twt_id):
    debug_print(sys._getframe().f_code.co_name)
    if name in conf["settings"]["monitored_users"]:
        filename = "data/custom/timeline_" + name + ".csv"
        write_timeline(filename, date, name, action, item, twt_id)
    if conf["config"]["dump_timeline_data"] is True:
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

def dump_pie_chart(dirname, filename, title, data):
    debug_print(sys._getframe().f_code.co_name)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    filepath = dirname + filename
    total = 0
    for n, c in data.iteritems():
        total += c
    pie_chart = pygal.Pie(truncate_legend=-1)
    pie_chart.title = title
    for n, c in sorted(data.iteritems()):
        percent = float(float(c)/float(total))*100.00
        label = n + " (" + "%.2f" % percent + "%)"
        pie_chart.add(label, c)
    pie_chart.render_to_file(filepath)


def dump_languages_graph():
    debug_print(sys._getframe().f_code.co_name)
    counter_data = get_all_counters()
    if counter_data is not None:
        chart_data = {}
        for name, value in sorted(counter_data.iteritems(), key=lambda x:x[0], reverse= False):
            m = re.search("^tweets_([a-z][a-z][a-z]?)$", name)
            if m is not None:
                item = m.group(1)
                chart_data[item] = value
        dirname = "data/"
        filename = "_lang_breakdown.svg"
        title = "Language breakdown"
        dump_pie_chart(dirname, filename, title, chart_data)

def dump_targets_graph():
    debug_print(sys._getframe().f_code.co_name)
    counter_data = get_all_counters()
    if counter_data is not None:
        chart_data = {}
        for name, value in sorted(counter_data.iteritems(), key=lambda x:x[0], reverse= False):
            m = re.search("^target_(.+)$", name)
            if m is not None:
                item = m.group(1)
                chart_data[item] = value
        dirname = "data/"
        filename = "_target_breakdown.svg"
        title = "Targets breakdown"
        dump_pie_chart(dirname, filename, title, chart_data)

def dump_overall_object_graphs(data_type):
    debug_print(sys._getframe().f_code.co_name)
    data_sets = ["users", "metadata"]
    current_datestring = get_datestring(data_type, 0)
    for ds in data_sets:
        for category in get_categories_from_storage(ds):
            if "keyword" in category or "url" in category or "tweets" in category:
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
            if "url" in category or "tweets" in category or "keyword" in category or "tweeters" in category:
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
        if "url" in cat or "tweets" in cat or "keyword" in cat or "tweeters" in cat:
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
        if "keyword" in cat or "url" in cat or "tweets" in cat or "keyword" in cat or "tweeters" in cat:
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

########################
# Specific dump routines
########################
def dump_counters():
    debug_print(sys._getframe().f_code.co_name)
    handle = io.open("data/_counters.txt", "w", encoding='utf-8')
    counter_dump = get_all_counters()
    if counter_dump is not None:
        for n, c in sorted(counter_dump.iteritems(), key=lambda x:x[0], reverse=False):
            handle.write(unicode(n) + u" = " + unicode(c) + u"\n")
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

def dump_heatmap_comparison():
    debug_print(sys._getframe().f_code.co_name)
    filename = "data/custom/global_heatmaps"
    titles = []
    weeks = []
    output_string = u"Date"
    heatmaps = get_all_heatmaps()
    for title, stuff in sorted(heatmaps.iteritems()):
        if "keyword" not in title and "monitored" not in title:
            m = re.search("^tweets_([a-z][a-z])$", title)
            if m is None:
                if "tweets_und" not in title:
                    titles.append(title)
        output_string += u", " + unicode(title)
    output_string += u"\n"
    for yearweek, weekdata in sorted(heatmaps[titles[0]].iteritems()):
        weeks.append(yearweek)
    weekdays = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    graph_data = []
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
                            graph_item["date"] = date_string
                            graph_item[title] = val
                            this_row.append(val)
                            row_sum += val
                if row_sum > 0:
                    output_string += unicode(day_name) + u" " + unicode(hour) + u":00"
                    output_string += u", " + u','.join(map(unicode, this_row)) + u"\n"
                    graph_data.append(graph_item)
    debug_print("Writing custom file: " + filename)
    handle = io.open(filename + ".csv", 'w', encoding='utf-8')
    handle.write(output_string)
    handle.close()
    chart = pygal.Bar(show_y_guides=False)
    chart.x_labels = [x['date'] for x in graph_data]
    for name in titles:
        if name in graph_data:
            mark_list = [x[name] for x in graph_data]
            chart.add(name, mark_list)
    chart.render_to_file(filename + ".svg")

def dump_dicts(var_name, variable, threshold, dir_name):
    debug_print(sys._getframe().f_code.co_name)
    if len(variable) < 1:
        return
    target_dir = "data"
    if var_name.startswith("url_keyword") or dir_name.startswith("url_keyword"):
        target_dir += "/url_keyword"
    elif var_name.startswith("monitored") or dir_name.startswith("monitored"):
        target_dir += "/monitored_hashtags"
    elif var_name.startswith("keyword") or dir_name.startswith("keyword"):
        target_dir += "/keyword"

# XXX This is definitely broken
    if len(dir_name) < 1:
        if var_name.startswith("top_user"):
            target_dir += "/top_users"
        elif var_name.startswith("top_metadata"):
            target_dir += "/top_metadata"
        elif var_name.startswith("words"):
            target_dir += "/all_words"
        else:
            target_dir += "/overall"

    if len(dir_name) > 0:
        target_dir += "/" + dir_name

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    filename = target_dir + "/" + var_name + ".txt"
    if debug == True:
        print "dump_dicts: filename: " + filename

    if type(variable) is not dict:
        print var_name
        print variable
        sys.exit(0)

    handle = io.open(filename, 'w', encoding='utf-8')
    c = 0
    w = 0
    max_entries = 200
    for tag, count in sorted(variable.items(), key=lambda x:x[1], reverse=True):
        if c > max_entries:
            break
        if count > w:
            w = count
        if count >= threshold:
            handle.write(unicode(tag) + u"," + unicode(count) + u"\n")
            c += 1
    handle.close()
    debug_print("Found " + str(len(variable)) + " " + var_name + " (wrote " + str(c) + ", winner had " + str(w) + ").")

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
    s = get_all_interarrival_stdevs()
    if s is not None:
        for category, raw_data in s.iteritems():
            filename = "data/custom/interarrivals_" + category + ".txt"
            output_string = u""
            output_data = {}
            for name, value in raw_data.iteritems():
                output_data[name] = str(value)
            for name, count in sorted(output_data.items(), key=lambda x:x[1], reverse=True):
                output_string += unicode(name) + u" | " + unicode(count) + u"\n"
            debug_print("Writing custom file: " + filename)
            handle = io.open(filename, 'w', encoding='utf-8')
            handle.write(output_string)
            handle.close()

def dump_userinfo():
    debug_print(sys._getframe().f_code.co_name)
    userinfo_data = get_all_userinfo()
    if userinfo_data is None:
        return
    userinfo_order = ["suspiciousness_reasons", "suspiciousness_score", "account_created_at", "num_tweets", "tweets_per_day", "tweets_per_hour", "favourites_count", "listed_count", "friends_count", "followers_count", "follower_ratio", "source", "default_profile", "default_profile_image", "protected", "verified", "links_out", "links_in", "two_way", "interarrival_stdev", "interarrival_av", "reply_stdev", "reply_av", "retweet_stdev", "retweet_av", "tweets_seen", "replies_seen", "reply_percent", "retweets_seen", "retweet_percent", "mentions_seen", "mentioned", "fake_news_seen", "fake_news_percent", "used_hashtags", "positive_words", "negative_hashtags", "positive_words", "negative_hashtags", "user_id_str"]
    for category, raw_data in userinfo_data.iteritems():
        filename = "data/custom/userinfo_" + category + ".csv"
        debug_print("Writing userinfo: " + filename)
        handle = io.open(filename, 'w', encoding='utf-8')
        handle.write(u"screen_name, ")
        handle.write(u", ".join(map(unicode, userinfo_order)) + u"\n")
        for name, data in raw_data.iteritems():
            handle.write(unicode(name))
            for key in userinfo_order:
                handle.write(u", ")
                if key in data:
                    data_type = type(data[key])
                    if data_type is int or data_type is float:
                        handle.write(unicode(data[key]))
                    else:
                        handle.write(unicode(data[key]))
            handle.write(u"\n")
        handle.close

###################
# Data dump routine
###################
def dump_data():
    debug_print(sys._getframe().f_code.co_name)
    debug_print("Starting dump...")

    debug_print("Calculating average_high for all counters")

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
                        dump_dicts(label, raw_data, 1, dirname)

    debug_print("Dumping users")
    dump_type = ""
    for category in get_categories_from_storage(dump_type + "users"):
        raw_data = get_category_data(dump_type + "users", category)
        dump_dicts(category, raw_data, 1, "")

    debug_print("Dumping metadata")
    for category in get_categories_from_storage(dump_type + "metadata"):
        raw_data = get_category_data(dump_type + "metadata", category)
        dump_dicts(category, raw_data, 1, "")

    debug_print("Dumping heatmaps")
    heatmaps = get_all_heatmaps()
    for name, value in heatmaps.iteritems():
        dump_heatmap(name)

    debug_print("Dumping interarrivals.")
    if conf["config"]["dump_interarrivals"] == True:
        dump_interarrivals()

    debug_print("Dumping heatmap comparison.")
    dump_heatmap_comparison()

    debug_print("Dumping userinfo.")
    dump_userinfo()

    debug_print("Dumping associations.")
    dump_associations()

    debug_print("Dumping counters.")
    dump_counters()
    dump_languages_graph()
    dump_targets_graph()

    debug_print("Completed dump...")
    return

def dump_graphs():
    debug_print(sys._getframe().f_code.co_name)
    debug_print("Dumping trends and graphs.")
    data_types = ["per_hour_data", "per_day_data"]
    for d in data_types:
        dump_periodic_data_trends(d)
        dump_periodic_data_graphs(d)
        dump_overall_data_graphs(d)
        dump_overall_object_graphs(d)


###############################
# Tweet processing main routine
###############################
def process_tweet(status):
    debug_print(sys._getframe().f_code.co_name)
    info = {}
    info["processing_start_time"] = int(time.time())
    increment_counter("tweets_processed")
    increment_counter("tweets_processed_this_interval")

    if "created_at" not in status:
        return
    if status["created_at"] is None:
        return

    info["tweet_time_readable"] = status["created_at"]
    tweet_time_object = time_string_to_object(info["tweet_time_readable"])
    info["tweet_time_unix"] = time_object_to_unix(tweet_time_object)
    info["datestring"] = tweet_time_object.strftime("%Y%m%d%H")
    week = tweet_time_object.strftime("%Y%W")
    week_number = int(tweet_time_object.strftime("%W"))
    weekday = tweet_time_object.weekday()
    hour = tweet_time_object.hour

    info["lang"] = status["lang"]
    info["tweet_id"] = status["id_str"]
    info["text"] = status["text"]
    info["name"] = status["screen_name"]
    if info["lang"] is None or info["name"] is None or info["text"] is None or info["tweet_id"] is None:
        return

    increment_heatmap("all_tweets", week, weekday, hour)
    increment_heatmap("tweets_" + info["lang"], week, weekday, hour)
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
        add_data("metadata", "source", info["source"])
        add_graphing_data("sources", info["name"], info["source"])
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
            increment_heatmap("retweets", week, weekday, hour)
            increment_per_hour("retweeters", info["datestring"], info["name"])
            add_graphing_data("retweets", info["name"], info["retweeted_name"])
            add_timeline_data(info["tweet_time_readable"], info["name"], "retweeted", info["retweeted_name"], info["tweet_id"])
            add_interarrival("retweets", info["name"], info["tweet_time_unix"])
            info["retweet_stdev"], info["retweet_av"] = calculate_interarrival_statistics(get_interarrival("retweets", info["name"]))
            if info["retweet_stdev"] > 0.0:
                add_interarrival_stdev("retweets", info["name"], info["retweet_stdev"])

# Quote tweets
    info["quote_tweeted_name"] = ""
    if "quote_tweeted_screen_name" in status:
        info["quote_tweeted_name"] = status["quote_tweeted_screen_name"]
        if info["quote_tweeted_name"] is not None:
            add_graphing_data("quote_tweets", info["name"], info["quote_tweeted_name"])
            add_timeline_data(info["tweet_time_readable"], info["name"], "quote_tweeted", info["quote_tweeted_name"], info["tweet_id"])

# Tweet words, description words
    info["positive_words"] = 0
    info["negative_words"] = 0
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
                        increment_heatmap("target_" + ta, week, weekday, hour)
                        increment_per_hour("targets", info["datestring"], ta)
                if add_data("metadata", label, t) is True:
                    increment_counter("tweet_words_seen")
        pos_words = get_positive_words(tokens)
        info["positive_words"] = len(pos_words)
        if len(pos_words) > 0:
            for w in pos_words:
                add_data("metadata", "positive_words", w)
                increment_per_hour("positive_words", info["datestring"], w)
                increment_heatmap("positive_words", week, weekday, hour)
                increment_counter("positive_words")
        neg_words = get_negative_words(tokens)
        info["negative_words"] = len(neg_words)
        if len(neg_words) > 0:
            for w in neg_words:
                add_data("metadata", "negative_words", w)
                increment_per_hour("negative_words", info["datestring"], w)
                increment_heatmap("negative_words", week, weekday, hour)
                increment_counter("negative_words")
        info["negative_words"] += get_userinfo_value("all_users", info["name"], "negative_words")
        info["positive_words"] += get_userinfo_value("all_users", info["name"], "positive_words")
        if "description" in status:
            tokens = strip_stopwords(tokenize(status["description"]), info["lang"])
            if len(tokens) > 0:
                for t in tokens:
                    if add_data("metadata", "description_words", t) is True:
                        increment_counter("description_words_seen")

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
                        if h in t:
                            label = "monitored_" + h + "_hashtag"
                            increment_heatmap(label, week, weekday, hour)
                            add_data("users", label + "_tweeters", info["name"])
                            increment_per_hour(label + "_tweeters", info["datestring"], info["name"])
                            increment_per_hour(label + "_tweets", info["datestring"], info["text"])
                            increment_counter(label + "_tweets")
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
            for u in info["urls"]:
                if u is not None:
                    add_data("metadata", "all_urls", u)
                    increment_per_hour("all_urls", info["datestring"], u)
                    add_timeline_data(info["tweet_time_readable"], info["name"], "tweeted url", u, info["tweet_id"])
                    if "twitter.com" not in u:
                        increment_heatmap("urls_not_twitter", week, weekday, hour)
                        add_data("metadata", "urls_not_twitter", u)
                        increment_per_hour("urls_not_twitter", info["datestring"], u)
                    for k in conf["settings"]["url_keywords"]:
                        if k in u:
                            label = "url_keyword_" + k
                            add_data("metadata", label, u)
                            increment_per_hour(label, info["datestring"], u)
                            increment_per_hour(label + "_tweeter", info["datestring"], info["name"])
                            increment_per_hour(label + "_tweets", info["datestring"], info["text"])
                            increment_counter(label + "_tweets")
                            add_data("users", label + "_tweeter", info["name"])
                            add_timeline_data(info["tweet_time_readable"], info["name"], "tweeted keyword url about", k, info["tweet_id"])
                            increment_heatmap("keyword_urls", week, weekday, hour)
                    for f in conf["corpus"]["fake_news_sources"]:
                        if f in u:
                            increment_heatmap("fake_news", week, weekday, hour)
                            increment_counter("fake_news_tweets")
                            increment_per_hour("fake_news_urls", info["datestring"], u)
                            increment_per_hour("fake_news_tweeters", info["datestring"], info["name"])
                            add_data("users", "fake_news_tweeters", info["name"])
                            add_data("metadata", "fake_news_urls", u)
                            add_timeline_data(info["tweet_time_readable"], info["name"], "tweeted fake news link", u, info["tweet_id"])
                            for h in info["hashtags"]:
                                increment_per_hour("fake_news_hashtags", info["datestring"], h)
                                add_data("metadata", "fake_news_hashtags", h)

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
                increment_heatmap(label, week, weekday, hour)
                increment_counter(label + "_tweets")
                add_data("metadata", label + "_tweeters", info["name"])
                increment_per_hour(label + "_tweeters", info["datestring"], info["name"])
                increment_per_hour(label + "_tweets", info["datestring"], info["text"])
                add_timeline_data(info["tweet_time_readable"], info["name"], "used monitored keyword:", k, info["tweet_id"])
                add_data("metadata", "keyword_tweets", info["text"])
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
    min_percentage = 75
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
    if info["tweets_per_day"] > min_tweets_per_day:
        info["suspiciousness_score"] += info["tweets_per_day"] - min_tweets_per_day
    if info["account_age_days"] > 0:
        if info["account_age_days"] < min_account_age_days:
            if info["tweets_per_day"] > min_tweets_per_day:
                info["suspiciousness_score"] += (info["tweets_per_day"] - min_tweets_per_day) * (min_account_age_days - info["account_age_days"])
    if info["followers_count"] < min_followers:
        if info["tweets_per_day"] > min_tweets_per_day:
            info["suspiciousness_score"] += info["tweets_per_day"] - min_tweets_per_day
    if info["interarrival_stdev"] > min_stdev:
        info["suspiciousness_score"] += (info["interarrival_stdev"] * stdev_multiplier)
        if info["tweets_seen"] > min_tweets:
            record_user = True
            info["suspiciousness_reasons"] += "[frequent tweets]"
    if info["reply_stdev"] > min_stdev:
        info["suspiciousness_score"] += (info["reply_stdev"] * stdev_multiplier)
        if info["tweets_seen"] > min_tweets:
            record_user = True
            info["suspiciousness_reasons"] += "[frequent replies]"
    if info["retweet_stdev"] > min_stdev:
        info["suspiciousness_score"] += (info["retweet_stdev"] * stdev_multiplier)
    if info["reply_percent"] > min_percentage:
        info["suspiciousness_score"] += info["reply_percent"]
        if info["tweets_seen"] > min_tweets:
            record_user = True
            info["suspiciousness_reasons"] += "[high percentage of replies]"
    if info["retweet_percent"] > min_percentage:
        info["suspiciousness_score"] += info["retweet_percent"]
    if info["fake_news_percent"] > min_percentage:
        info["suspiciousness_score"] += info["fake_news_percent"]
        if info["tweets_seen"] > min_tweets:
            record_user = True
            info["suspiciousness_reasons"] += "[frequent fake news tweets]"
    if info["followers_count"] > min_followers:
        if info["follower_ratio"] > min_follow_ratio:
            info["suspiciousness_score"] += (info["follower_ratio"] * follow_ratio_multiplier)
            if info["tweets_seen"] > min_tweets:
                record_user = True
                info["suspiciousness_reasons"] += "[follow ratio]"
    if info["account_age_days"] > min_account_age_days:
        if info["followers_count"] == 0:
            info["suspiciousness_score"] += 100

# XXX come up with a rule that removes consitutionb0t, etc.
    info["suspiciousness_score"] = int(info["suspiciousness_score"])
    debug_print("Suspiciousness: " + str(info["suspiciousness_score"]))

    if "source" in info:
        if is_source_legit(info["source"]) is False:
            record_user = True
            info["suspiciousness_reasons"] += "[non-legit Twitter client]"

    if info["suspiciousness_score"] > suspiciousness_threshold:
        record_user = True
        info["suspiciousness_reasons"] += "[suspiciousness threshold]"

    if info["tweets_seen"] > crazy_threshold:
        record_user = True
        info["suspiciousness_reasons"] += "[crazy threshold]"

    if info["verified"] == "Yes":
        record_user = False

    if info["suspiciousness_score"] < 1:
        record_user = False

    if info["two_way"] > 0:
        info["suspiciousness_reasons"] += "[non-zero two_way]"

    debug_print("Preparing userinfo for " + info["name"])

    if conf["config"]["log_all_userinfo"] == True:
        add_userinfo("all_users", info["name"], info)

    if record_user == True:
        debug_print("Recording suspiciousness for " + info["name"])
        add_userinfo("suspicious", info["name"], info)
        increment_counter("suspicious_users")
    else:
        if exists_userinfo("suspicious", info["name"]):
            debug_print("Deleting suspiciousness for " + info["name"])
            del_userinfo("suspicious", info["name"])
            decrement_counter("suspicious_users")

    info["processing_end_time"] = int(time.time())
    processing_duration = info["processing_end_time"] - info["processing_start_time"]
    debug_print("Processing " + info["name"] + " took " + str(processing_duration))
    return

##############
# Process args
##############
def process_args(args):
    debug_print(sys._getframe().f_code.co_name)
    global restart, debug, test
    for arg in sys.argv:
        print "Got arg: " + arg
        if "restart" in arg:
            restart = True
        if "debug" in arg:
            debug = True
        if "test" in arg:
            test = True

#########################################
# Main routine, called when script starts
#########################################
if __name__ == '__main__':
    init_tweet_processor()
    targets = read_config_unicode("config/targets.txt")

# Read in serialized data
# extract the number of tweets processed
# extract start date and end date

    items_seen = 0
    with open("data/raw/raw.json") as f:
        for line in f:
            item = json.loads(line)
            items_seen += 1
            process_tweet(item)
            if items_seen % 10 == 0:
                sys.stdout.write("#")
                sys.stdout.flush()
            if items_seen % 10000 == 0:
                print
# Show time elapsed
# Show percentage completion based on total tweets
                print "Processed " + str(items_seen) + " items so far."
                print
        print "Processed " + str(items_seen) + " items in total."
        print "Serializing..."
        serialize_data()



