# -*- coding: utf-8 -*-
from twarc import Twarc
from tweepy import OAuthHandler
from tweepy import API
from authentication_keys import get_account_credentials
from datetime import datetime, date, time, timedelta
from itertools import combinations
import Queue
import threading
import time
import json
import io
import os
import sys
import re

def save_json(variable, filename):
    with io.open(filename, "w", encoding="utf-8") as f:
        f.write(unicode(json.dumps(variable, indent=4, ensure_ascii=False)))

def load_json(filename):
    ret = None
    if os.path.exists(filename):
        try:
            with io.open(filename, "r", encoding="utf-8") as f:
                ret = json.load(f)
        except:
            pass
    return ret

def read_account_names(config_file):
    ret = []
    with open(config_file, "r") as f:
        for line in f:
            if not line.isspace():
                line = line.strip()
                line = line.lower()
                ret.append(line)
    return ret

def get_account_data_for_names(names):
    print("Got " + str(len(names)) + " names.")
    acct_name, consumer_key, consumer_secret, access_token, access_token_secret = get_account_credentials()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    auth_api = API(auth)

    batch_len = 100
    batches = (names[i:i+batch_len] for i in range(0, len(names), batch_len))
    ret = []
    for batch_count, batch in enumerate(batches):
        users_list = auth_api.lookup_users(screen_names=batch)
        users_json = (map(lambda t: t._json, users_list))
        ret += users_json
    return ret

def get_ids_from_names(names):
    ret = []
    all_json = get_account_data_for_names(names)
    for d in all_json:
        if "id_str" in d:
            id_str = d["id_str"]
            ret.append(id_str)
    return ret

def get_interactions(status):
    interactions = []
    screen_name = ""
    if "user" in status:
        user_data = status["user"]
        if "screen_name" in user_data:
            screen_name = user_data["screen_name"]
    if screen_name is None:
        return
    if "entities" in status:
        entities = status["entities"]
        if "user_mentions" in entities:
            for item in entities["user_mentions"]:
                if item is not None:
                    mention = item['screen_name']
                    if mention is not None:
                        if mention not in interactions:
                            interactions.append(mention)
    if "quoted_status" in status:
        orig_tweet = status["quoted_status"]
        if "user" in orig_tweet:
            if orig_tweet["user"] is not None:
                user = orig_tweet["user"]
                if "screen_name" in user:
                    if user["screen_name"] is not None:
                        if user["screen_name"] not in interactions:
                            interactions.append(user["screen_name"])
    if "retweeted_status" in status:
        orig_tweet = status["retweeted_status"]
        if "user" in orig_tweet:
            if orig_tweet["user"] is not None:
                user = orig_tweet["user"]
                if "screen_name" in user:
                    if user["screen_name"] is not None:
                        if user["screen_name"] not in interactions:
                            interactions.append(user["screen_name"])
    if "in_reply_to_screen_name" in status:
        if status["in_reply_to_screen_name"] is not None:
            if status["in_reply_to_screen_name"] not in interactions:
                interactions.append(status["in_reply_to_screen_name"])
    return interactions

def process_hashtags(status):
    hashtags = []
    if "entities" in status:
        entities = status["entities"]
        if "hashtags" in entities:
            for item in entities["hashtags"]:
                if item is not None:
                    if "text" in item:
                        tag = item['text']
                        if tag is not None:
                            if tag not in hashtags:
                                hashtags.append(tag.lower())
    return hashtags

def record_frequency_dist(category, item):
    global data
    if category not in data:
        data[category] = {}
    if item not in data[category]:
        data[category][item] = 1
    else:
        data[category][item] += 1

def record_word_interactions(category, words):
    global data
    interactions = []
    if len(words) > 1:
        for comb in combinations(words, 2):
            interactions.append(comb)

    if category not in data:
        data[category] = {}
    if len(interactions) > 0:
        for inter in interactions:
            item1, item2 = inter
            if item1 not in data[category]:
                data[category][item1] = {}
            if item2 not in data[category][item1]:
                data[category][item1][item2] = 1
            else:
                data[category][item1][item2] += 1

def add_interactions(category, source, targets):
    global data
    if targets is not None and len(targets) > 0:
        if category not in data:
            data[category] = {}
        if source not in data[category]:
            data[category][source] = {}
        for item in targets:
            if item not in data[category][source]:
                data[category][source][item] = 1
            else:
                data[category][source][item] += 1

def process_text(text):
# Processing step 1
# Remove unwanted characters, URLs, screen names, etc.
    valid = u"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ#@-'/…… "
    url_match = u"(https?:\/\/[0-9a-zA-Z\-\_]+\.[\-\_0-9a-zA-Z]+\.?[0-9a-zA-Z\-\_]*\/?.*)"
    name_match = u"\@[\_0-9a-zA-Z]+\:?"
    ret = []
    text = re.sub(url_match, u"", text)
    text = re.sub(name_match, u"", text)
    text = re.sub(u"\&amp\;?", u"", text)
    text = re.sub(u"[\:\.]{1,}$", u"", text)
    text = re.sub(u"^RT\:?", u"", text)
    text = re.sub(u"\w*[\…]", u"", text)
    text = re.sub(u"\/", u" ", text)
    text = u''.join(x for x in text if x in valid)
    text = text.strip()
    if len(text.split()) < 5:
        return
# Processing step 2
# Tokenize sentence into words
    words = re.split(r'(\s+)', text)
    if len(words) < 1:
        return
    tokens = []
    for w in words:
        if w is not None:
            w = w.strip()
            w = w.lower()
            if w.isspace() or w == "\n" or w == "\r":
                w = None
            if w is not None and "http" in w:
                w = None
            if w is not None and len(w) < 1:
                w = None
            if w is not None and u"…" in w:
                w = None
            if w is not None:
                tokens.append(w)
    if len(tokens) < 0:
        return
# Processing step 3
# Remove stopwords and other undesirable tokens
    cleaned = []
    for token in tokens:
        if len(token) > 0:
            if all_stopwords is not None:
                for s in all_stopwords:
                    if token == s:
                        token = None
            if token is not None:
                if re.search(".+…$", token):
                    token = None
            if token is not None:
                if token == "#":
                    token = None
            if token is not None:
                cleaned.append(token)
    if len(cleaned) < 1:
        return
    return cleaned

def dump_stuff():
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    interactions = ["word_interactions", "user_user_interactions", "hashtag_hashtag_interactions"]
    for s in interactions:
        if s in data:
            filename = os.path.join(save_dir, s + ".json")
            save_json(data[s], filename)
            filename = os.path.join(save_dir, s + ".csv")
            with io.open(filename, "w", encoding="utf-8") as handle:
                handle.write(u"Source,Target,Weight\n")
                for source, targets in sorted(data[s].items()):
                    for target, count in sorted(targets.items()):
                        if source != target and source is not None and target is not None:
                            handle.write(source + u"," + target + u"," + unicode(count) + u"\n")
    freq_dists = ["tweet_frequencies", "tweeter_frequencies", "word_frequencies", "interacted_frequencies", "hashtag_frequencies", "influencer_frequencies"]
    for f in freq_dists:
        if f in data:
            filename = os.path.join(save_dir, f + ".txt")
            with io.open(filename, "w", encoding="utf-8") as handle:
                for item, count in sorted(data[f].items(), key=lambda x:x[1], reverse=True):
                    entry = unicode(count) + u"\t" + unicode(item) + u"\n"
                    handle.write(entry)
    filename = os.path.join(save_dir, "data.json")
    save_json(data, filename)
    print("Done")

def process_tweet(status):
    global previous_dump
    if "user" not in status or "text" not in status:
        return
    sn = status["user"]["screen_name"]
    record_frequency_dist("tweeter_frequencies", sn)
    text = status["text"]

    tokens = process_text(text)
    tokens_printable = ""
    if tokens is not None and len(tokens) > 0:
        tokens_printable = " ".join(tokens)
        record_frequency_dist("tweet_frequencies", tokens_printable)
        for t in tokens:
            record_frequency_dist("word_frequencies", t)
        if len(tokens) > 1:
            record_word_interactions("word_interactions", tokens)

    interactions = get_interactions(status)
    int_printable = ""
    if interactions is not None and len(interactions) > 0:
        int_printable = ",".join(interactions)
        for n in interactions:
            record_frequency_dist("influencer_frequencies", n)
            record_frequency_dist("interacted_frequencies", sn)
        add_interactions("user_user_interactions", sn, interactions)

    hashtags = process_hashtags(status)
    hashtags_printable = ""
    if hashtags is not None and len(hashtags) > 0:
        hashtags_printable = ",".join(hashtags)
        for h in hashtags:
            record_frequency_dist("hashtag_frequencies", h)
        if len(hashtags) > 1:
            record_word_interactions("hashtag_hashtag_interactions", hashtags)

    sys.stdout.write("#")
    sys.stdout.flush()

    #print(sn + " [" + int_printable + "] [" + hashtags_printable + "] " + tokens_printable)
    if int(time.time()) - previous_dump > 10:
        print("Dumping")
        dump_stuff()
        previous_dump = int(time.time())

def tweet_processing_thread():
    while True:
        item = tweet_queue.get()
        process_tweet(item)
        tweet_queue.task_done()

def get_tweet_stream(query, twarc):
    print("Query: " + query)
    for status in twarc.filter(follow=query):
        tweet_queue.put(status)

if __name__ == '__main__':
    save_dir = "account_follower"
    data = {}
    filename = os.path.join(save_dir, "data.json")
    old_data = load_json(filename)
    if old_data is not None:
        data = old_data
    previous_dump = int(time.time())
    extra_stopwords = []
    stopword_file = load_json("corpus/stopwords-iso.json")
    all_stopwords = stopword_file["en"]
    all_stopwords += extra_stopwords

    config_file = "config/to_follow.txt"
    to_follow = read_account_names(config_file)
    print("Converting names to IDs")
    id_list = get_ids_from_names(to_follow)
    print("Names count: " + str(len(to_follow)) + " ID count: " + str(len(id_list)))
    query = ",".join(id_list)

    acct_name, consumer_key, consumer_secret, access_token, access_token_secret = get_account_credentials()
    twarc = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)
    print("Acct: " + acct_name)

    tweet_queue = Queue.Queue()
    thread = threading.Thread(target=tweet_processing_thread)
    thread.daemon = True
    thread.start()

    while True:
        try:
            get_tweet_stream(query, twarc)
        except KeyboardInterrupt:
            print "Keyboard interrupt..."
            sys.exit(0)
        except:
            print("Error. Restarting...")
            time.sleep(5)
            pass
