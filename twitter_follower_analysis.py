from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy import Cursor
from datetime import datetime, date, time, timedelta
from authentication_keys import get_account_credentials, get_account_sequential
import numpy as np
import pygal
from collections import Counter
import os.path
import time
import json
import sys
import re
import io

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

def time_object_to_unix(time_object):
    return int(time_object.strftime("%s"))

def get_utc_unix_time():
    dts = datetime.utcnow()
    epochtime = time.mktime(dts.timetuple())
    return epochtime

def unix_time_to_readable(time_string):
    return datetime.fromtimestamp(int(time_string)).strftime('%Y-%m-%d %H:%M:%S')

def twarc_time_to_unix(time_string):
    return time_object_to_unix(twarc_time_to_object(time_string))

def seconds_to_days(seconds):
    return float(float(seconds)/86400.00)

def seconds_since_twarc_time(time_string):
    input_time_unix = int(twarc_time_to_unix(time_string))
    current_time_unix = int(get_utc_unix_time())
    return current_time_unix - input_time_unix

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

def save_json(variable, filename):
    with open(filename, "w") as f:
        json.dump(variable, f, indent=4)

def load_json(filename):
    ret = None
    if os.path.exists(filename):
        try:
            with open(filename, "r") as f:
                ret = json.load(f)
            print("Loaded data from " + filename + ".")
        except:
            print("Couldn't load " + filename + ".")
    else:
        print(filename + " didn't exist.")
    return ret

def dump_bar_chart(filename, title, x_labels, chart_data):
    if len(x_labels) < 5:
        return
    chart = pygal.Bar(show_y_guides=True, x_labels_major_count=5, show_minor_x_labels=False, show_minor_y_labels=False, x_label_rotation=20)
    chart.title = title
    chart.x_labels = x_labels
    for item in chart_data:
        print("Adding values: " + str(item[0]) + " = " + str(item[1]) + " to chart.")
        chart.add(item[0], item[1])
    chart.render_to_file(filename)

def get_follower_ids(target):
    acct_name, consumer_key, consumer_secret, access_token, access_token_secret = get_account_credentials()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    auth_api = API(auth)
    print "Signing in as: "+auth_api.me().name
    queried = 0
    threshold = 20
    max = 100
    print("Target: " + target)
    print("Getting follower ids")
    follower_ids = auth_api.followers_ids(target)
    filename = target + "_follower_ids.json"
    save_json(follower_ids, filename)
    return follower_ids

def create_batches(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in xrange(0, len(l), n))

def get_details_for_batch(batch):
    acct_name, consumer_key, consumer_secret, access_token, access_token_secret = get_account_sequential()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    auth_api = API(auth)
    users_list = auth_api.lookup_users(user_ids=batch)
    users_json = (map(lambda t: t._json, users_list))
    return users_json

def get_data(target):
    filename = target + ".json"
    follower_ids = get_follower_ids(target)
    batches = create_batches(follower_ids, 100)
    all_data = []
    batch_count = 1
    for b in batches:
        print("Batch: " + batch_count)
        batch_count += 1
    all_data += get_details_for_batch(b)
    save_json(all_data, filename)
    return all_data

def get_account_ages(all_data, num_ranges):
    account_ages = {}
    max_age = 0
    blocks = 1000
    if num_ranges != 0:
        blocks = num_ranges
    all_ages = []
    for d in all_data:
        create_date = d["created_at"]
        account_age = seconds_since_twarc_time(create_date)
        all_ages.append(account_age)
        if account_age > max_age:
            max_age = account_age
    account_ages = Counter(all_ages)
    age_range = max_age/blocks
    labels = []
    for x in range(blocks):
        start_range = seconds_to_days(x * age_range)
        end_range = seconds_to_days(x * age_range + age_range)
        item = "%.2f" % start_range + " - " + "%.2f" % end_range
        labels.append(item)
    groups = np.arange(0, max_age, age_range)
    grouped_ages = np.digitize(all_ages, groups)
    group_counts = Counter(grouped_ages)
    sorted_group_counts = []
    for g, c in sorted(group_counts.items()):
        if g <= len(labels):
            sorted_group_counts.append([labels[g], int(c)])
    sorted_ages = []
    for age, count in sorted(account_ages.items()):
        sorted_ages.append([int(age), int(count)])
    #dump_bar_chart("age_ranges.svg", "Haavisto followers account ages (days)", labels[:10], sorted_group_counts[:10])
    return sorted_ages, sorted_group_counts

def pretty_print_age_groups(grouped_ages):
    print
    print
    print
    total = 0
    for item in grouped_ages[:20]:
        print("\t" + str(item[1]) + " followers had and account that was " + item[0] + " days old.")
        total += item[1]
    print
    print("\tTotal: " + str(total))
    print
    print

if __name__ == '__main__':
    target = "Haavisto"
    num_ranges = 1000
    if (len(sys.argv) > 1):
        target = str(sys.argv[1])
    all_data = []
    filename = target + ".json"
    if os.path.exists(filename):
        try:
            with open(filename, "r") as f:
                all_data = json.load(f)
            print("Loaded data from " + filename + ".")
        except:
            print("Couldn't load " + filename + ". Fetching data.")
            all_data = get_data(target)
    else:
        print(filename + " didn't exist. Fetching data.")
        all_data = get_data(target)
    account_ages, grouped_ages = get_account_ages(all_data, num_ranges)
    filename = target + "_follower_account_ages.json"
    save_json(account_ages, filename)
    filename = target + "_follower_account_ages_grouped.json"
    save_json(grouped_ages, filename)
    pretty_print_age_groups(grouped_ages)






