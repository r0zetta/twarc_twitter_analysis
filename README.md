# twarc_twitter_analysis
Twitter analysis tools based on twarc and tweepy
- all tools utilize authentication keys stored in the keys/ directory. Check the placeholder file here for an example of how to set it up
- you can put multiple keys in that directory if you plan on using multiple scripts in parallel
- to generate authentication keys, go to https://apps.twitter.com/

authentication_keys.py
- authentication interface used by all scripts

twarc_stream_analysis.py
- used to listen to a Twitter stream (based on search targets) and log useful information
- uses the config files stored in the config/ directory
  - bad_users.txt - Twitter users interacting with the list of accounts in this file will gain suspiciousness score, and certain actions will be logged
  - good_users.txt - list of known-good Twitter screen_names, used primarily for white-listing accounts
  - description_keywords.txt - if you add a word to this list, accounts that have that word in their description will gain suspiciousness score
  - keywords.txt - words listed here will trigger Tweets containing those words to be further processed
  - languages.txt - a list of two-letter language identifiers. Only tweets of those languages will be captured and processed. Leave this empty to capture everything.
  - monitored_hashtags.txt - if these hashtags appear in tweets, more information is collected about the tweet
  - settings.txt - settings for what to collect, process, log, etc.
  - targets.txt - search terms to run the stream against. If empty, the stream will listen to the 1% feed.
  - tweet_identifiers.txt - if these terms appear in a tweet, the publisher of that tweet will gain suspiciousness score
  - url_keywords.txt - if these words appear in a URL itself, more information will be logged
- also uses data from the corpus/ directory

twarc_tweet_search.py
- gathers tweets based on a search defined in config/searcher_targets.txt
- search can be a term, set of terms, URL, hashtag, or a full sentence

twitter_follower_analysis.py
- gathers data about the followers of queried account (given as the first command-line arg)

twitter_user_analysis.py
- does a full analysis of the queried account (given as the first command-line arg)
