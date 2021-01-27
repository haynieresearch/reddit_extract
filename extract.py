#!/usr/local/bin/python3.9
#**********************************************************
#* CATEGORY	SOFTWARE
#* GROUP	DATA EXTRACTION
#* AUTHOR	LANCE HAYNIE <LANCE@HAYNIEMAIL.COM>
#* FILE		EXTRACT.PY
#**********************************************************
#ETL Stock Market Data
#Copyright 2020 Haynie IPHC, LLC
#Developed by Haynie Research & Development, LLC
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.#
#You may obtain a copy of the License at
#http://www.apache.org/licenses/LICENSE-2.0
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
import praw
import pandas as pd
import datetime as dt
import sys
import os
import yaml

settings_file = "settings.yaml"
if not os.path.exists(settings_file):
    print("settings.yaml not found!")
    sys.exit()

with open(settings_file, "r") as f:
    settings_data = yaml.load(f, Loader=yaml.FullLoader)

client  = settings_data['reddit']['client_id']
secret  = settings_data['reddit']['client_secret']
agent   = settings_data['reddit']['user_agent']
user    = settings_data['reddit']['user']
pwd     = settings_data['reddit']['pass']

if len(sys.argv) == 1:
    args = sys.argv
    print("No option provided, use --help for options.")
    exit(0)
elif len(sys.argv) == 2:
    args = sys.argv
    arg1 = sys.argv[1]
elif len(sys.argv) == 3:
    args = sys.argv
    arg1 = sys.argv[1]
    arg2 = sys.argv[2]
else:
    print("No option provided, use --help for options.")
    exit(0)

if arg1.lower() == "--check_config":
    print("Just making sure everything works!")
    exit(0)

connection = praw.Reddit(client_id=f"{client}", \
                         client_secret=f"{secret}", \
                         user_agent=f"{agent}", \
                         username=f"{user}", \
                         password=f"{pwd}")

sub = connection.subreddit(f"{arg1}")
top = sub.top()

dict = { "title":[], \
         "score":[], \
         "id":[], "url":[], \
         "comms_num": [], \
         "created": [], \
         "body":[]}

for submission in top:
    submission.title.encode('ascii', 'ignore')
    submission.selftext.encode('ascii', 'ignore')
    dict["title"].append(submission.title)
    dict["score"].append(submission.score)
    dict["id"].append(submission.id)
    dict["url"].append(submission.url)
    dict["comms_num"].append(submission.num_comments)
    dict["created"].append(submission.created)
    dict["body"].append(submission.selftext)

data = pd.DataFrame(dict)

def get_date(created):
    return dt.datetime.fromtimestamp(created)

_timestamp = data["created"].apply(get_date)

data = data.assign(timestamp = _timestamp)
data.to_csv(f"{arg2}{arg1}.csv", index=False)
exit(0)
