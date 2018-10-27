import pandas as pd 
import numpy as np 
import mysql.connector
import time
import datetime
import logging
import sys
import matplotlib.pyplot as plt
import scipy as sp
import scipy.interpolate

def log_interp1d(xx, yy, kind='linear'):
    logx = np.log10(xx)
    logy = np.log10(yy)
    lin_interp = sp.interpolate.interp1d(logx, logy, kind=kind)
    log_interp = lambda zz: np.power(10.0, lin_interp(np.log10(zz)))
    return log_interp

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
votes = pd.read_sql_query("select postid, votetypeid from votes", mydb);
# posts = pd.read_sql_query("select id, answercount, commentcount, unix_timestamp(creationdate) as creationdate, owneruserid, parentid, posttypeid, score, viewcount, postlength from posts limit 100", mydb)
posts = pd.read_sql_query("select id, answercount, commentcount, unix_timestamp(creationdate) as creationdate, owneruserid, parentid, acceptedanswerid, posttypeid, score, viewcount, postlength from posts where creationdate between \'2009-12-01\' and \'2009-12-31\'", mydb)
# posts['creationdate'] = posts['creationdate'].apply(lambda t: datetime.datetime.fromtimestamp(t))
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

accepted = votes[votes['votetypeid'] == 1]

accepted = accepted.groupby('postid', as_index=False).count()
accepted.columns = ['id', 'accepted']
accepted['accepted'] = accepted['accepted'].fillna(0)

answers = pd.merge(answers, accepted, how='left', on='id')

"""
Merge questions ans answers to perform comparisons.
1) Remove all answers after 30 days of question time.
"""
qa = pd.merge(questions, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])
qa['delay'] = pd.to_timedelta(qa['creationdate_a']-qa['creationdate_q']) # Getting the delay time
# qa['delay'] = qa['creationdate_a']-qa['creationdate_q'] # Getting the delay time
qa['timerank'] = qa.groupby('id_q')['delay'].rank(method='dense')

qa = qa[qa['timerank']==1].drop_duplicates('id_q')
qa['acceptedanswerid_q'].replace(0, np.nan, inplace=True)

data = qa[['delay', 'id_q', 'acceptedanswerid_q']]

data = data[data['delay'] >= datetime.timedelta(0)]
# data = data.set_index('delay')

data = data.sort_values('delay')

data = data.resample('24H', on='delay').agg({'id_q':'count', 'acceptedanswerid_q':'count'})

print(data.head())
print(data.columns)
print(data.index)

data['fract'] = data['acceptedanswerid_q']/data['id_q']

dd = data[['fract']]

plt.plot(dd)

# dd.plot(loglog=True, legend=False)

print(dd.head())

plt.show()





# print(qa.head())

# print(qa['delay'].max())



# logging.info("All query complete ... Time passed %s", (datetime.datetime.now()-starttime))

# data.to_csv('Sd.csv', index=False)

# logging.info("Data saved to file, done... Time passed %s", (datetime.datetime.now()-starttime))