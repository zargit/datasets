import pandas as pd 
import numpy as np 
import mysql.connector
import time
import matplotlib.pyplot as plt
import datetime
import logging
import sys

pd.set_option('display.width', 2000)
pd.set_option('display.max_columns', 50)
starttime = datetime.datetime.now()
logging.basicConfig(level=logging.INFO, filename=sys.argv[0].split('.')[0]+'.log')

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="password",
  database="stackoverflow2010"
)

logging.info("Starting to retreive data ... Time passed %s", (datetime.datetime.now()-starttime))
users = pd.read_sql_query("select id, accountid, reputation from susers", mydb);
# votes = pd.read_sql_query("select postid, votetypeid from votes", mydb);
posts = pd.read_sql_query("select id, answercount, commentcount, unix_timestamp(creationdate) as creationdate, owneruserid, parentid, posttypeid, score, viewcount, postlength from posts", mydb)
posts['creationdate'] = posts['creationdate'].apply(lambda t: datetime.datetime.fromtimestamp(t))
logging.info("Data retrieved ... Time passed %s", (datetime.datetime.now()-starttime))

data = None

"""
Attach reputaion with posts
1) Merge posts with users.
"""
posts = pd.merge(posts, users[['accountid', 'reputation']], how='left', left_on='owneruserid', right_on='accountid')

"""
Start filtering posts.
1) Get all question with at least one answer.
2) Get all answers.
"""
questions = posts[(posts['posttypeid'] == 1) & (posts['answercount'] > 0)]
logging.info("Total questions found %s", questions.shape)
answers = posts[posts['posttypeid'] == 2]
logging.info("Total answers found %s", answers.shape)
# upvotes = votes[votes['votetypeid'].isin([2,3])]
# logging.info("Total upvotes found %s", upvotes.shape)
# downvotes = votes[votes['votetypeid'] == 3]
# logging.info("Total downvotes found %s", downvotes.shape)

# perpost_upvotes = upvotes.groupby('postid', as_index=False).count()
# perpost_upvotes.columns = ['id', 'votecount']
# answers = pd.merge(answers, perpost_upvotes, how='left', on='id')

"""
Merge questions ans answers to perform comparisons.
1) Remove all answers after 30 days of question time.
"""
qa = pd.merge(questions, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])
qa['delay'] = (qa['creationdate_a']-qa['creationdate_q']).astype('timedelta64[s]') # Getting the delay time
qa['timerank'] = qa.groupby('id_q')['delay'].rank(method='dense')

qa = qa[qa['answercount_q'] >= 2]
print(qa.shape)
# remove questions with first anwser in less than six minutes
qa = qa[((qa['timerank']==1) & (qa['delay'] >= 360)) | (qa['timerank']>=2)]
print(qa.shape)
print(qa.head())
q1 = qa[qa['timerank'] == 1]
print(q1.shape)
q2 = qa[qa['timerank'] == 2]
print(q2.shape)
qq = pd.merge(q1[['id_q', 'id_a', 'timerank', 'reputation_a', 'answercount_q']], q2[['id_q', 'id_a', 'timerank', 'reputation_a']], how='inner', on='id_q', suffixes=['_1', '_2'])
print(qq.head())
data = qq[['id_q', 'id_a_1', 'id_a_2', 'answercount_q', 'reputation_a_1', 'reputation_a_2' ]]
data.columns = ['qid', 'aid_1', 'aid_2', 'answercount', 'ans1_reputation', 'ans2_reputation']
data.replace(np.nan, 0, inplace=True)
data['rep1-rep2'] = data['ans1_reputation']-data['ans2_reputation']
print(data.head())
# data['rep1-rep2'] = data['reputation_a_1']-data['reputation_a_2']
# print(data.iloc[:5,:])

# data = qa[['id_q', 'reputation_a', 'votecount']]

# data = data.sort_values('id_q')

# # """
# # Final Merging
# # """
# data = pd.merge(max_score_row, max_reputation_row, how='left', on='id')
# data = pd.merge(data, stats, how='left', on='id')
# data = data.sort_values('id')

# print(data.iloc[:5,:])

# logging.info("All query complete ... Time passed %s", (datetime.datetime.now()-starttime))

data.to_csv('fig4_1.csv', index=False)