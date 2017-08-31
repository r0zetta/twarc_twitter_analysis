# -*- coding: utf-8 -*-
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

##################
# Global variables
##################
restart = False
threaded = True
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
    directories = ["serialized", "data", "data/raw"]
    for dir in directories:
        if not os.path.exists(dir):
            os.makedirs(dir)
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
    if threaded == True:
        if get_active_threads > 1:
            print "Waiting for queue to empty..."
            tweet_queue.join()
    dump_file_handle.close()
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

def dump_counters():
    debug_print(sys._getframe().f_code.co_name)
    handle = io.open("data/_collector_counters.txt", "w", encoding='utf-8')
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
        dirname = "data/"
        filename = "_lang_breakdown.svg"
        title = "Language breakdown"
        dump_pie_chart(dirname, filename, title, chart_data)

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
# retweet data
    if "retweeted_status" in status:
        orig_tweet = status["retweeted_status"]
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
# Periodically reload config
############################
def reload_config_event():
    debug_print(sys._getframe().f_code.co_name)
    if int(time.time()) > get_counter("previous_config_reload") + conf["params"]["config_reload_interval"]:
        reload_settings()
        set_counter("previous_config_reload", int(time.time()))

############################
# Manage all periodic events
############################
def periodic_events():
    dump_event()
    reload_config_event()

########################
# Periodically dump data
########################
def dump_event():
    debug_print(sys._getframe().f_code.co_name)
    global dump_file_handle
    global data
    if int(time.time()) > get_counter("previous_dump_time") + get_counter("dump_interval"):
        start_time = int(time.time())
        gathering_time = start_time - get_counter("previous_dump_time") - get_counter("dump_interval")
        dump_counters()
        dump_languages_graph()
        serialize_data()
        dump_file_handle.flush()
        os.fsync(dump_file_handle)
        end_time = int(time.time())
        processing_time = gathering_time
        print
        if threaded == True:
            queue_length = tweet_queue.qsize()
            print str(queue_length) + " items in the queue."
            active_threads = get_active_threads()
            print str(active_threads) + " threads active."
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

#########################
# Tweet processing thread
#########################
def tweet_processing_thread():
    debug_print(sys._getframe().f_code.co_name)
    global dump_file_handle
    while True:
        item = tweet_queue.get()
        increment_counter("tweets_processed")
        increment_counter("tweets_processed_this_interval")
        json.dump(item, dump_file_handle)
        dump_file_handle.write("\n")
        tweet_queue.task_done()
        periodic_events()
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
    if "lang" in status:
        lang = status["lang"]
        increment_counter("tweets_" + lang)
        if len(conf["settings"]["monitored_langs"]) > 0:
            if lang not in conf["settings"]["monitored_langs"]:
                debug_print("Skipping tweet of lang: " + lang)
                sys.stdout.write("-")
                sys.stdout.flush()
                return
    captured_status = capture_status_items(status)
    if captured_status is not None:
        increment_counter("tweets_captured")
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
    global restart, threaded, debug, test
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
