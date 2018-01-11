from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy import Cursor
from datetime import datetime, date, time, timedelta
from authentication_keys import get_account_credentials, get_account_sequential
from langdetect import detect
import numpy as np
import pygal
from collections import Counter
import os.path
import time
import json
import sys
import re
import io

def sort_to_list(dict_data):
    ret = []
    for k, v in sorted(dict_data.items(), key=lambda x:x[1], reverse=True):
        ret.append([k, v])
    return ret

def is_bot_name(name):
    interesting = True
    if re.search("^([A-Z]?[a-z]{1,})?[\_]?([A-Z]?[a-z]{1,})?[\_]?[0-9]{,9}$", name):
        interesting = False
    if re.search("^[\_]{,3}[A-Z]{2,}[\_]{,3}$", name):
        interesting = False
    if re.search("^[A-Z]{2}[a-z]{2,}$", name):
        interesting = False
    if re.search("^([A-Z][a-z]{1,}){3}[0-9]?$", name):
        interesting = False
    if re.search("^[A-Z]{1,}[a-z]{1,}[A-Z]{1,}$", name):
        interesting = False
    if re.search("^[A-Z]{1,}[a-z]{1,}$", name):
        interesting = False
    if re.search("^([A-Z]?[a-z]{1,}[\_]{1,}){1,}[A-Z]?[a-z]{1,}$", name):
        interesting = False
    if re.search("^[A-Z]{1,}[a-z]{1,}[\_][A-Z][\_][A-Z]{1,}[a-z]{1,}$", name):
        interesting = False
    if re.search("^[a-z]{1,}[A-Z][a-z]{1,}[A-Z][a-z]{1,}$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{1,}[A-Z][a-z]{1,}[A-Z]{1,}$", name):
        interesting = False
    if re.search("^([A-Z][\_]){1,}[A-Z][a-z]{1,}$", name):
        interesting = False
    if re.search("^[\_][A-Z][a-z]{1,}[\_][A-Z][a-z]{1,}[\_]?$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{1,}[\_][A-Z][\_][A-Z]$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{2,}[0-9][A-Z][a-z]{2,}$", name):
        interesting = False
    if re.search("^[A-Z]{1,}[0-9]?$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{1,}[\_][A-Z]$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{1,}[A-Z]{2}[a-z]{1,}$", name):
        interesting = False
    if re.search("^[\_]{1,}[a-z]{2,}[\_]{1,}$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{2,}[\_][A-Z][a-z]{2,}[\_][A-Z]$", name):
        interesting = False
    if re.search("^[A-Z]?[a-z]{2,}[0-9]{2}[\_]?[A-Z]?[a-z]{2,}$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{2,}[A-Z]{1,}[0-9]{,2}$", name):
        interesting = False
    if re.search("^[\_][A-Z][a-z]{2,}[A-Z][a-z]{2,}[\_]$", name):
        interesting = False
    if re.search("^([A-Z][a-z]{1,}){2,}$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{2,}[\_][A-Z]{2}$", name):
        interesting = False
    if re.search("^[a-z]{3,}[0-9][a-z]{3,}$", name):
        interesting = False
    if re.search("^[a-z]{4,}[A-Z]{1,}$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{3,}[A-Z][0-9]{,9}$", name):
        interesting = False
    if re.search("^[A-Z]{2,}[\_][A-Z][a-z]{3,}$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{3,}[A-Z]{1,3}[a-z]{3,}$", name):
        interesting = False
    if re.search("^[A-Z]{3,}[a-z]{3,}[0-9]?$", name):
        interesting = False
    if re.search("^[A-Z]?[a-z]{3,}[\_]+$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{3,}[\_][a-z]{3,}[\_][A-Za-z]{1,}$", name):
        interesting = False
    if re.search("^[A-Z]{2,}[a-z]{3,}[A-Z][a-z]{3,}$", name):
        interesting = False
    if re.search("^[A-Z][a-z]{2,}[A-Z][a-z]{3,}[\_]?[A-Z]{1,}$", name):
        interesting = False
    if re.search("^[A-Z]{4,}[0-9]{2,9}$", name):
        interesting = False
    return interesting

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
    filename = os.path.join(save_dir, target + "_follower_ids.json")
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
    filename = os.path.join(save_dir, target + ".json")
    follower_ids = get_follower_ids(target)
    batches = create_batches(follower_ids, 100)
    all_data = []
    batch_count = 1
    for b in batches:
        print("Batch: " + str(batch_count))
        batch_count += 1
        all_data += get_details_for_batch(b)
    save_json(all_data, filename)
    return all_data

def analyze_account_names(all_data, analyze_lang):
    descs = []
    desc_langs = []
    names = []
    lc_names = {}
    name_langs = []
    bot_names = []
    screen_names = []
    screen_name_langs = []
    desc_data = {}
    name_data = {}
    screen_name_data = {}
    desc_count = 0
    no_desc_count = 0
    iter_count = 0
    print("Analyzing names, descriptions, etc.")
    for d in all_data:
        if iter_count % 500 == 0:
            print(str(iter_count) + "/" + str(len(all_data)))
        iter_count += 1

        name = d["name"]
        names.append(name)
        if analyze_lang == True:
            name_lang = ""
            try:
                name_lang = detect(name)
            except:
                name_lang = "Unknown"
            name_langs.append(name_lang)
            if name_lang not in name_data:
                name_data[name_lang] = []
                name_data[name_lang].append(name)
            else:
                name_data[name_lang].append(name)

        screen_name = d["screen_name"]
        screen_names.append(screen_name)
        if is_bot_name(screen_name):
            bot_names.append(screen_name)
        if analyze_lang == True:
            screen_name_lang = ""
            try:
                screen_name_lang = detect(screen_name)
            except:
                screen_name_lang = "Unknown"
            screen_name_langs.append(screen_name_lang)
            if screen_name_lang not in screen_name_data:
                screen_name_data[screen_name_lang] = []
                screen_name_data[screen_name_lang].append(screen_name)
            else:
                screen_name_data[screen_name_lang].append(screen_name)

        lc_name = name.lower()
        if lc_name not in lc_names:
            lc_names[lc_name] = []
        lc_names[lc_name].append(screen_name)

        if "description" in d:
            desc = d["description"]
            if len(desc) > 0:
                desc_count += 1
                descs.append(desc)
                if analyze_lang == True:
                    desc_lang = ""
                    try:
                        desc_lang = detect(desc)
                    except:
                        desc_lang = "Unknown"
                    desc_langs.append(desc_lang)
                    if desc_lang not in desc_data:
                        desc_data[desc_lang] = []
                        desc_data[desc_lang].append(desc)
                    else:
                        desc_data[desc_lang].append(desc)
            else:
                no_desc_count += 1
        else:
            no_desc_count += 1
    name_lang_breakdown = sort_to_list(Counter(name_langs))
    screen_name_lang_breakdown = sort_to_list(Counter(screen_name_langs))
    desc_lang_breakdown = sort_to_list(Counter(desc_langs))

    lc_names_filtered = {}
    for name, screen_names in sorted(lc_names.items()):
        if len(screen_names) > 1:
            lc_names_filtered[name] = screen_names
    filename = os.path.join(save_dir, target + "_lowercase_names.json")
    save_json(lc_names, filename)
    filename = os.path.join(save_dir, target + "_lowercase_names_filtered.json")
    save_json(lc_names_filtered, filename)
    filename = os.path.join(save_dir, target + "_botlike_names.json")
    save_json(bot_names, filename)
    if analyze_lang == True:
        filename = os.path.join(save_dir, target + "_names_by_language.json")
        save_json(name_data, filename)
        filename = os.path.join(save_dir, target + "_screen_names_by_language.json")
        save_json(screen_name_data, filename)
        filename = os.path.join(save_dir, target + "_descriptions_by_language.json")
        save_json(desc_data, filename)
        filename = os.path.join(save_dir, target + "_name_language_breakdown.json")
        save_json(name_lang_breakdown, filename)
        filename = os.path.join(save_dir, target + "_screen_name_language_breakdown.json")
        save_json(screen_name_lang_breakdown, filename)
        filename = os.path.join(save_dir, target + "_description_language_breakdown.json")
        save_json(desc_lang_breakdown, filename)
        pretty_print_counter("", "Twitter names were identified as language", "", name_lang_breakdown)
        pretty_print_counter("", "Twitter screen names were identified as language", "", screen_name_lang_breakdown)
        pretty_print_counter("", "Twitter descriptions were identified as language", "", desc_lang_breakdown)

    print("Had description: " + str(desc_count))
    print("Had no description: " + str(no_desc_count))
    return name_lang_breakdown, screen_name_lang_breakdown, desc_lang_breakdown, desc_count, no_desc_count

def make_ranges(data_set, data_map, num_ranges=1000):
    range_counter = Counter(data_set)
    range_max = max(data_set)
    range_step = range_max/num_ranges
    range_counts = []
    sorted_items = []
    labels = []
    for x in range(num_ranges):
        start_range = x * range_step
        end_range = x * range_step + range_step
        label = str(start_range) + " - " + str(end_range)
        labels.append(label)
        temp = []
        for x, y in data_map.iteritems():
            if x > start_range and x < end_range:
                for z in y:
                    temp.append(z)
        sorted_items.append([label, temp])
        range_counts.append([label, len(temp)])
    return labels, sorted_items, range_counts

def get_ranges_for_var(all_data, var_name, num_ranges=1000):
    val_list = []
    val_name_map = {}
    for d in all_data:
        sn = d["screen_name"]
        val = d[var_name]
        val_list.append(val)
        if val not in val_name_map:
            val_name_map[val] = []
        val_name_map[val].append(sn)
    labels, sorted_items, range_counts = make_ranges(val_list, val_name_map, num_ranges)
    filename = os.path.join(save_dir, target + "_" + var_name + "_abs_name_map.json")
    save_json(val_name_map, filename)
    filename = os.path.join(save_dir, target + "_" + var_name + "_name_map.json")
    save_json(sorted_items, filename)
    filename = os.path.join(save_dir, target + "_" + var_name + "_ranges.json")
    save_json(range_counts, filename)

def analyze_activity(all_data, blocks=1000):
    tpd_list = []
    tpd_name_map = {}
    for d in all_data:
        sn = d["screen_name"]
        tweets = d["statuses_count"]
        create_date = d["created_at"]
        account_age = seconds_since_twarc_time(create_date)
        account_age_days = float(account_age / 86400.00)
        tpd = float(tweets/account_age_days)
        tpd_list.append(tpd)
        if tpd not in tpd_name_map:
            tpd_name_map[tpd] = []
        tpd_name_map[tpd].append(sn)
    labels, sorted_items, range_counts = make_ranges(tpd_list, tpd_name_map, blocks)
    filename = os.path.join(save_dir, target + "_activity_name_map.json")
    save_json(sorted_items, filename)
    filename = os.path.join(save_dir, target + "_activity_ranges.json")
    save_json(range_counts, filename)
    pretty_print_counter("", "tweeted", "times per day", range_counts)

def get_account_ages(all_data, blocks=1000):
    account_ages = {}
    age_name_map = {}
    all_ages = []
    for d in all_data:
        sn = d["screen_name"]
        create_date = d["created_at"]
        account_age = seconds_since_twarc_time(create_date)
        if account_age not in age_name_map:
            age_name_map[account_age] = []
        age_name_map[account_age].append(sn)
        all_ages.append(account_age)
    account_ages = Counter(all_ages)
    max_age = max(all_ages)
    age_range = max_age/blocks
    labels = []
    sorted_range_name_map = []
    for x in range(blocks):
        start_seconds = x * age_range
        end_seconds = x * age_range + age_range
        start_range = seconds_to_days(start_seconds)
        end_range = seconds_to_days(end_seconds)
        item = "%.2f" % start_range + " - " + "%.2f" % end_range
        ns = []
        for age, names in age_name_map.iteritems():
            if age > start_seconds and age < end_seconds:
                for n in names:
                    ns.append(n)
        sorted_range_name_map.append([item, ns])
        labels.append(item)
    groups = np.arange(0, max_age, age_range)
    grouped_ages = np.digitize(all_ages, groups)
    group_counts = Counter(grouped_ages)
    sorted_group_counts = []
    for g, c in sorted(group_counts.items()):
        if g <= len(labels):
            sorted_group_counts.append([labels[g], int(c)])
    sorted_age_name_map = []
    for age, names in sorted(age_name_map.items()):
        sorted_age_name_map.append([age, names])
    sorted_ages = []
    for age, count in sorted(account_ages.items()):
        sorted_ages.append([int(age), int(count)])
    filename = os.path.join(save_dir, target + "_age_name_map.json")
    save_json(sorted_age_name_map, filename)
    filename = os.path.join(save_dir, target + "_range_name_map.json")
    save_json(sorted_range_name_map, filename)
    filename = os.path.join(save_dir, target + "_account_ages.json")
    save_json(sorted_ages, filename)
    filename = os.path.join(save_dir, target + "_account_ages_grouped.json")
    save_json(sorted_group_counts, filename)
    pretty_print_age_groups(sorted_group_counts)
    return sorted_ages, sorted_group_counts

def is_egg(d):
    ret = False
    dpi = d["default_profile_image"]
    dp = d["default_profile"]
    if dpi == True and dp == True:
        ret = True
    return ret

def find_eggs(all_data):
    eggs = []
    non_eggs = []
    for d in all_data:
        sn = d["screen_name"]
        if is_egg(d):
            eggs.append(sn)
        else:
            non_eggs.append(sn)
    filename = os.path.join(save_dir, target + "_egg_followers.json")
    save_json(eggs, filename)
    filename = os.path.join(save_dir, target + "_non_egg_followers.json")
    save_json(non_eggs, filename)
    print("Found " + str(len(eggs)) + " eggs and " + str(len(non_eggs)) + " non-eggs.")
    return eggs, non_eggs

def analyze_locations(all_data):
    locs = []
    loc_name_map = {}
    no_loc = []
    for d in all_data:
        sn = d["screen_name"]
        loc = d["location"]
        if len(loc) > 0:
            if loc not in loc_name_map:
                loc_name_map[loc] = []
            loc_name_map[loc].append(sn)
            locs.append(loc)
        else:
            no_loc.append(sn)
    sorted_name_map = []
    for loc, names in sorted(loc_name_map.items()):
        sorted_name_map.append([loc, names])
    loc_counts = dict(Counter(locs))
    filename = os.path.join(save_dir, target + "_locations_counts.json")
    save_json(loc_counts, filename)
    filename = os.path.join(save_dir, target + "_locations_name_map.json")
    save_json(sorted_name_map, filename)
    filename = os.path.join(save_dir, target + "_no_location.json")
    save_json(no_loc, filename)
    print(str(len(locs)) + " users had location set.")
    print(str(len(no_loc)) + " users didn't have location set.")

def analyze_geo_data(all_data):
    enabled = []
    disabled = []
    name_map = {}
    for d in all_data:
        sn = d["screen_name"]
        geo = d["geo_enabled"]
        if geo == True:
            enabled.append(sn)
            if "status" in d:
                if "coordinates" in d["status"]:
                    if d["status"]["coordinates"] is not None:
                        print d["status"]["coordinates"]
            if "retweeted_status" in d:
                if "coordinates" in d["retweeted_status"]:
                    if d["retweeted_status"]["coordinates"] is not None:
                        print d["retweeted_status"]["coordinates"]
        else:
            disabled.append(sn)
    print(str(len(enabled)) + " had geo enabled")
    print(str(len(disabled)) + " had geo disabled")

def is_bot(d):
    ret = False
    score = 0
    egg = is_egg(d)
    sn = d["screen_name"]
    bot_name = is_bot_name(sn)
    tweets = d["statuses_count"]
    following = d["friends_count"]
    followers = d["followers_count"]
    if egg == True:
        score += 50
    if bot_name == True:
        score += 100
    if following == 21:
        score += 100
    if following < 22:
        score += 50
    if followers == 0:
        score += 100 
    if tweets == 0:
        score += 100
    if score > 300:
        ret = True
    return ret

def find_bots(all_data):
    bots = []
    for d in all_data:
        sn = d["screen_name"]
        if is_bot(d):
            bots.append(sn)
    print
    print("Found " + str(len(bots)) + " bots.")
    if len(bots) > 0:
        filename = os.path.join(save_dir, target + "_bot_followers.json")
        save_json(bots, filename)
    return bots

def pretty_print_age_groups(grouped_ages):
    print
    print
    print
    total = 0
    for item in grouped_ages[:20]:
        print("\t" + str(item[1]) + " followers had an account that was " + item[0] + " days old.")
        total += item[1]
    print
    print("\tTotal: " + str(total))
    print
    print

def pretty_print_counter(start, middle, end, counter_list):
    print
    print
    print
    total = 0
    for item in counter_list[:20]:
        print("\t" + start + " " + str(item[1]) + " " + middle + " " + str(item[0]) + " " + end + ".")
        total += int(item[1])
    print
    print("\tTotal: " + str(total))
    print
    print


if __name__ == '__main__':
    num_ranges = 1000
    target = "Haavisto"
    if (len(sys.argv) > 1):
        target = str(sys.argv[1])
    save_dir = "captures/followers/follower_analysis_" + target
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    all_data = []
    filename = os.path.join(save_dir, target + ".json")
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

    analyze_lang = False
    filename = os.path.join(save_dir, target + "_names_by_language.json")
    if os.path.exists(filename):
        analyze_lang = False

    get_account_ages(all_data, num_ranges)
    find_bots(all_data)
    find_eggs(all_data)
    analyze_account_names(all_data, analyze_lang)
    analyze_activity(all_data)
    analyze_locations(all_data)
    analyze_geo_data(all_data)
    get_ranges_for_var(all_data, "statuses_count", num_ranges)
    get_ranges_for_var(all_data, "favourites_count", num_ranges)
    get_ranges_for_var(all_data, "friends_count", num_ranges)
    get_ranges_for_var(all_data, "followers_count", num_ranges)
    get_ranges_for_var(all_data, "listed_count", num_ranges)


