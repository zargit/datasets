"""
Questioner features: 4 features total: 
1. questioner reputation
2. # of questioner’s questions 
3. # of questioner’s answers
4. percentage of accepted answers on their previous questions
"""
import pandas as pd 
import numpy as np 
import mysql.connector
import time
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
  database="stackoverflow2017"
)

logging.info("Starting to retreive data ... Time passed %s", (datetime.datetime.now()-starttime))
users = pd.read_sql_query("select id, accountid, reputation from susers", mydb);
posts = pd.read_sql_query("select id, answercount, commentcount, unix_timestamp(creationdate) as creationdate, owneruserid, parentid, acceptedanswerid, posttypeid, score, viewcount, postlength from posts where creationdate between \'2009-12-01\' and \'2009-12-31\'", mydb)
posts['creationdate'] = posts['creationdate'].apply(lambda t: datetime.datetime.fromtimestamp(t))
logging.info("Data retrieved ... Time passed %s", (datetime.datetime.now()-starttime))

# Add reputation to posts
posts = pd.merge(posts, users[['accountid', 'reputation']], how='left', left_on='owneruserid', right_on='accountid')

data = None

"""
Start filtering posts.
1) Get all question with at least one answer.
2) Get all answers.
"""
questions = posts[(posts['posttypeid'] == 1) & (posts['answercount'] > 0)]
questions['acceptedanswerid'].replace(0, np.nan, inplace=True)
logging.info("Total questions found %s", questions.shape)
answers = posts[posts['posttypeid'] == 2]
logging.info("Total answers found %s", answers.shape)


"""
Merge questions ans users to get noq
1) Merge question ans users
"""
qa = pd.merge(questions, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])
qa['delay'] = (qa['creationdate_a']-qa['creationdate_q']).astype('timedelta64[s]')

t = [1,3,24,72]
qas = []

for i in t:
	x = qa[qa['delay'] <= i*3600][['id_q', 'owneruserid_q', 'reputation_q', 'acceptedanswerid_q']].drop_duplicates('id_q')
	x.columns = ['id', 'owneruserid', 'reputation', 'acceptedanswerid']
	qas.append(x)

nos = []

for x in qas:
	noq = pd.merge(users[['accountid']], x[['owneruserid', 'acceptedanswerid']], how='inner', left_on='accountid', right_on='owneruserid').groupby('accountid', as_index=False).agg({'owneruserid':'count', 'acceptedanswerid':'count'})
	noq.columns = ['accountid', 'noq', 'accepted_count']
	noq['accepted_ratio'] = noq['accepted_count']/noq['noq']
	noq = noq.drop(columns=['accepted_count'])
	noa = pd.merge(users[['accountid']], answers[['owneruserid']], how='inner', left_on='accountid', right_on='owneruserid').groupby('accountid', as_index=False).count()
	noa.columns = ['accountid', 'noa']
	nos.append((noq, noa))

for i,v in enumerate(t):
	data = pd.merge(nos[i][0], nos[i][1], how='left', on='accountid')
	data['noa'].fillna(0, inplace=True)
	data = pd.merge(qas[i], data, how='left', left_on='owneruserid', right_on='accountid')
	data = data.drop(columns=['accountid', 'owneruserid', 'acceptedanswerid'])
	data.to_csv('Sa_%dhour.csv'%(v), index=False)

# """
# Final Merging
# """
# data = pd.merge(noq, noa, how='left', on='accountid')
# data['noa'].fillna(0, inplace=True)
# qa_1hour = pd.merge(qa_1hour, data, how='left', left_on='owneruserid', right_on='accountid')
# qa_1hour = qa_1hour.drop(columns=['accountid', 'owneruserid'])
# qa_1hour.to_csv('Sa_1hour.csv', index=False)

# qa_3hour = pd.merge(qa_3hour, data, how='left', left_on='owneruserid', right_on='accountid')
# qa_3hour = qa_3hour.drop(columns=['accountid', 'owneruserid'])
# qa_3hour.to_csv('Sa_3hour.csv', index=False)

# qa_24hour = pd.merge(qa_24hour, data, how='left', left_on='owneruserid', right_on='accountid')
# qa_24hour = qa_24hour.drop(columns=['accountid', 'owneruserid'])
# qa_24hour.to_csv('Sa_24hour.csv', index=False)

# qa_72hour = pd.merge(qa_72hour, data, how='left', left_on='owneruserid', right_on='accountid')
# qa_72hour = qa_72hour.drop(columns=['accountid', 'owneruserid'])
# qa_72hour.to_csv('Sa_72hour.csv', index=False)