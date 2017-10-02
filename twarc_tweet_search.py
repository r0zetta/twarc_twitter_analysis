# -*- coding: utf-8 -*-
from twarc import Twarc
from authentication_keys import get_account_credentials
from datetime import datetime, date, time, timedelta
import shutil
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

##################
# Global variables
##################
debug = False
test = False
exit_correctly = False
searches = []
targets = []
data = {}
conf = {}
acct_name = ""
script_start_time_str = ""
output_dir = ""
serialize_dir = ""
dump_dir = ""

def init_params():
    debug_print(sys._getframe().f_code.co_name)
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
    debug_print(sys._getframe().f_code.co_name)
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

def init_tweet_processor():
    debug_print(sys._getframe().f_code.co_name)
    global output_dir, dump_dir, serialize_dir
    date_string = str(int(time.time()))
    output_dir = "captures/searches/" + date_string + "/"
    dump_dir = "captures/searches/" + date_string + "/raw/"
    serialize_dir = "captures/searches/" + date_string + "/serialized/"
    directories = [output_dir, dump_dir, serialize_dir]
    for dir in directories:
        if not os.path.exists(dir):
            os.makedirs(dir)
    shutil.copy2('config/searcher_targets.txt', output_dir)
    deserialize_data()
    init_params()
    init_config()
    reload_settings()
    set_counter("dump_interval", conf["params"]["default_dump_interval"])
    set_counter("previous_dump_time", int(time.time()))
    set_counter("previous_graph_dump_time", int(time.time()))
    set_counter("script_start_time", int(time.time()))
    set_counter("previous_config_reload", int(time.time()))

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
    global data
    if variable in data:
        if category in data[variable]:
            return data[variable][category]

def get_all_storage(variable):
    debug_print(sys._getframe().f_code.co_name)
    global data
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

###############
# Serialization
###############
def serialize_variable(variable, filename):
    debug_print(sys._getframe().f_code.co_name)
    global serialize_dir
    filename = serialize_dir + "/" + filename + ".json"
    handle = open(filename, 'w')
    json.dump(variable, handle, indent=4)
    handle.close()
    return

def unserialize_variable(varname):
    debug_print(sys._getframe().f_code.co_name)
    global serialize_dir
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
    global serialize_dir
    tmp_dir = serialize_dir[:-1] + ".tmp"
    old_dir = serialize_dir[:-1] + ".old"
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


##################
# Helper functions
##################
def debug_print(string):
    if debug == True:
        print string

def twarc_time_to_readable(time_string):
# e.g. Thu Aug 31 12:45:31 +0000 2017
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
    global dump_file_handle
    dump_file_handle.close()
    print "Serializing data..."
    serialize_data()

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

def dump_line_graph(chart_title, y_axis_label, x_axis_labels, plot_data):
    debug_print(sys._getframe().f_code.co_name)
    filename = output_dir + chart_title + ".svg"
    chart = pygal.Line(show_y_guides=True, show_dots=False, x_labels_major_count=5, show_minor_x_labels=False, show_minor_y_labels=False, x_label_rotation=20)
    chart.title = chart_title
    chart.x_labels = x_axis_labels
    chart.add(y_axis_label, plot_data)
    chart.render_to_file(filename)

def dump_counters():
    debug_print(sys._getframe().f_code.co_name)
    filename = output_dir + "_collector_counters.txt"
    handle = io.open(filename, "w", encoding='utf-8')
    counter_dump = get_all_counters()
    if counter_dump is not None:
        for n, c in sorted(counter_dump.iteritems(), key=lambda x:x[0], reverse=False):
            handle.write(unicode(n) + u" = " + unicode(c) + u"\n")
    handle.close

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
        dirname = output_dir
        filename = "_lang_breakdown.svg"
        title = "Language breakdown"
        dump_pie_chart(dirname, filename, title, chart_data)

def dump_chronology_graphs():
    debug_print(sys._getframe().f_code.co_name)
    lists = ["retweet_heatmap", "reply_heatmap", "all_tweet_heatmap"]
    for l in lists:
        dump_chronology_graph(l)

def dump_chronology_graph(label):
    debug_print(sys._getframe().f_code.co_name)
    output_data = get_category_storage(label, "per_minute_data")
    if output_data is None:
        return
    x_axis_labels = []
    plot_data = []
    chart_title = label
    y_axis_label = "tweets/minute"
    for name, count in sorted(output_data.items(), key=lambda x:x[0], reverse=False):
        x_axis_labels.append(name)
        plot_data.append(count)
    dump_line_graph(chart_title, y_axis_label, x_axis_labels, plot_data)

def record_per_minute(category, status):
    debug_print(sys._getframe().f_code.co_name)
    created_at = ""
    if "created_at" in status:
        created_at = status["created_at"][:-3]
    if created_at != "":
        increment_storage(category, "per_minute_data", created_at)

def record_chronology(category, status):
    debug_print(sys._getframe().f_code.co_name)
    global data
    created_at = ""
    screen_name = ""
    if "created_at" in status:
        created_at = status["created_at"]
    if "screen_name" in status:
        screen_name = status["screen_name"]
    if category not in data:
        data[category] = {}
    data[category][created_at] = screen_name

def dump_chronology_lists():
    debug_print(sys._getframe().f_code.co_name)
    lists = ["retweeters", "repliers", "original_tweets"]
    for l in lists:
        dump_chronology_data(l)

def dump_chronology_data(label):
    debug_print(sys._getframe().f_code.co_name)
    global data
    filename = output_dir + label + ".txt"
    handle = io.open(filename, "w", encoding='utf-8')
    if label in data:
        if data[label] is not None:
            for date, name in sorted(data[label].items(), key=lambda x:x[0], reverse=False):
                handle.write(date + "\t" + name + "\n")
            handle.close

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

############################
# Tweet data capture routine
############################
def capture_status_items(status):
    debug_print(sys._getframe().f_code.co_name)
    captured_status = {}
# Tweet data from tweet object
    if "lang" in status:
        captured_status["lang"] = status["lang"]
    else:
        return
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
    if "source" in status:
        source_url = status["source"]
        m = re.search("^\<.+\>(.+)\<\/a\>$", source_url)
        if m is not None:
            captured_status["source"] = m.group(1)
    if "in_reply_to_screen_name" in status:
        captured_status["in_reply_to_screen_name"] = status["in_reply_to_screen_name"]
        captured_status["reply"] = True
# retweet data
    if "retweeted_status" in status:
        orig_tweet = status["retweeted_status"]
        if "id_str" in orig_tweet:
            captured_status["retweeted_id"] = orig_tweet["id_str"]
        if "user" in orig_tweet:
            if orig_tweet["user"] is not None:
                if "screen_name" in orig_tweet["user"]:
                    if orig_tweet["user"]["screen_name"] is not None:
                        captured_status["retweeted_screen_name"] = orig_tweet["user"]["screen_name"]
                        captured_status["retweet"] = True
    if "retweeted_screen_name" not in captured_status:
        rt_name = re.search("^RT @(\w+)\W", captured_status["text"])
        if rt_name is not None:
            captured_status["retweeted_screen_name"] = rt_name.group(1)
            captured_status["retweet"] = True

# quote tweet data
    if "quoted_status" in status:
        orig_tweet = status["quoted_status"]
        if "user" in orig_tweet:
            if orig_tweet["user"] is not None:
                if "screen_name" in orig_tweet["user"]:
                    if orig_tweet["user"]["screen_name"] is not None:
                        captured_status["quote_tweeted_screen_name"] = orig_tweet["user"]["screen_name"]
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
    return captured_status

############################
# Process captured status
############################
def record_tweet(text, name, tweet_id):
    debug_print(sys._getframe().f_code.co_name)
    global data
    url = "https://twitter.com/" + name + "/status/" + str(tweet_id)
    if "tweets" not in data:
        data["tweets"] = {}
    if text not in data["tweets"]:
        data["tweets"][text] = {}
        data["tweets"][text]["url"] = url
    if "users_found" not in data:
        data["users_found"] = []
    if name not in data["users_found"]:
        data["users_found"].append(name)

def dump_tweets():
    debug_print(sys._getframe().f_code.co_name)
    output = ""
    if "tweets" in data:
        for text, stuff in data["tweets"].iteritems():
            output += text + "\n"
            output += data["tweets"][text]["url"] + "\n"
            output += "\n"
    if len(output) > 0:
        filename = output_dir + "_tweets.txt"
        handle = io.open(filename, "w", encoding='utf-8')
        handle.write(unicode(output))
        handle.close()

def dump_names():
    debug_print(sys._getframe().f_code.co_name)
    output = ""
    if "users_found" in data:
        for n in data["users_found"]:
            output += n + "\n"
    if len(output) > 0:
        filename = output_dir + "_names.txt"
        handle = io.open(filename, "w", encoding='utf-8')
        handle.write(unicode(output))
        handle.close()


def process_status(status):
    debug_print(sys._getframe().f_code.co_name)
    retweet = False
    reply = False
    if "retweet" in status:
        if status["retweet"] == True:
            retweet = True
            record_tweet(status["text"], status["retweeted_screen_name"], status["retweeted_id"])
            record_per_minute("retweet_heatmap", status)
            record_chronology("retweeters", status)
            increment_counter("retweets")
    else:
        record_tweet(status["text"], status["screen_name"], status["id_str"])
        record_per_minute("original_tweet_heatmap", status)
        record_chronology("original_tweets", status)
        increment_counter("original_tweets")
    if "reply" in status:
        if status["reply"] == True:
            record_per_minute("reply_heatmap", status)
            record_chronology("repliers", status)
            increment_counter("replies")
    record_per_minute("all_tweet_heatmap", status)
    record_chronology("all_tweets", status)
    increment_counter("all_tweets")

############################
# Manage all periodic events
############################
def periodic_events():
    dump_event()


########################
# Periodically dump data
########################
def dump_all():
    dump_counters()
    dump_chronology_graphs()
    dump_chronology_lists()
    dump_languages_graph()
    dump_tweets()
    dump_names()

def dump_event():
    debug_print(sys._getframe().f_code.co_name)
    global dump_file_handle
    global data
    if int(time.time()) > get_counter("previous_dump_time") + get_counter("dump_interval"):
        start_time = int(time.time())
        gathering_time = start_time - get_counter("previous_dump_time") - get_counter("dump_interval")
        dump_all()
        serialize_data()
        end_time = int(time.time())
        processing_time = gathering_time
        print
        print "Gathering took: " + str(gathering_time) + " seconds."
        print "Processed " + str(get_counter("tweets_processed_this_interval")) + " tweets this " + str(get_counter("dump_interval")) + " second interval."
        print "Tweets encountered: " + str(get_counter("tweets_encountered")) + ", captured: " + str(get_counter("tweets_captured")) + ", processed: " + str(get_counter("tweets_processed"))
        print "Tweets per second: " + str(float(float(get_counter("tweets_processed_this_interval"))/float(get_counter("dump_interval"))))
        set_counter("tweets_processed_this_interval", 0)
        set_counter("previous_dump_time", int(time.time()))
        set_counter("processing_time", processing_time)
        set_counter("dump_interval", conf["params"]["default_dump_interval"] + processing_time)
        increment_counter("successful_loops")
        print "Executed " + str(get_counter("successful_loops")) + " successful loops."
        total_running_time = end_time - get_counter("script_start_time")
        print "Running as " + acct_name + " since " + script_start_time_str + " (" + str(total_running_time) + " seconds)"
        current_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
        print "Current time is: " + current_time_str
        print
        return

##############
# Process args
##############
def process_args(args):
    debug_print(sys._getframe().f_code.co_name)
    global debug, test
    for arg in sys.argv:
        print "Got arg: " + arg
        if "debug" in arg:
            debug = True
        if "test" in arg:
            test = True

#########################################
# Main routine, called when script starts
#########################################
if __name__ == '__main__':
    if len(sys.argv) > 1:
        process_args(sys.argv)

    if debug == True:
        exit_correctly = True

    init_tweet_processor()
    dump_filename = dump_dir + "raw.json"
    full_dump_filename = dump_dir + "full.json"
    dump_file_handle = open(dump_filename, "a")
    full_dump_file_handle = open(full_dump_filename, "a")

    targets = read_config_unicode("config/searcher_targets.txt")
    if len(targets) > 0:
        searches = targets
    else:
        print "Please add search targets in config/searcher_targets.txt"
        sys.exit(0)
    if len(searches) != 1:
        print "Please supply exactly one search."
        sys.exit(0)
    print "Search targets:"
    print searches
    script_start_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    acct_name, consumer_key, consumer_secret, access_token, access_token_secret = get_account_credentials()
    t = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)
    print "Signing in as: " + acct_name
    for status in t.search(searches):
        captured_status = {}
        increment_counter("tweets_encountered")
        #json.dump(status, full_dump_file_handle)
        full_dump_file_handle.write("\n")
        if "lang" in status:
            lang = status["lang"]
            increment_counter("tweets_" + lang)
            captured_status = capture_status_items(status)
            process_status(captured_status)
            if captured_status is not None:
                increment_counter("tweets_captured")
                increment_counter("tweets_processed")
                increment_counter("tweets_processed_this_interval")
                json.dump(captured_status, dump_file_handle)
                dump_file_handle.write("\n")
                periodic_events()
            sys.stdout.write("#")
            sys.stdout.flush()
    dump_all()
    serialize_data()
    print
    print "Done."
    print
