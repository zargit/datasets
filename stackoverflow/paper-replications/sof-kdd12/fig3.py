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
import matplotlib.pyplot as plt

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
users = pd.read_sql_query("select accountid, reputation from users", mydb);
posts = pd.read_sql_query("select id, owneruserid, parentid, posttypeid, answercount from posts where creationdate between \'2016-07-31\' and \'2017-12-31\'", mydb)
# posts['creationdate'] = posts['creationdate'].apply(lambda t: datetime.datetime.fromtimestamp(t))
logging.info("Data retrieved ... Time passed %s", (datetime.datetime.now()-starttime))

# Add reputation to posts
posts = pd.merge(posts, users[['accountid', 'reputation']], how='left', left_on='owneruserid', right_on='accountid')

posts = posts[(posts['reputation'] < posts['reputation'].quantile(0.996))]

data = None

"""
Start filtering posts.
1) Get all question with at least one answer.
2) Get all answers.
"""
questions = posts[(posts['posttypeid'] == 1) & (posts['answercount'] > 0)]
logging.info("Total questions found %s", questions.shape)
answers = posts[posts['posttypeid'] == 2]
logging.info("Total answers found %s", answers.shape)


"""
Merge questions ans users to get noq
1) Merge question ans users
"""
qa = pd.merge(questions, answers, how='left', left_on='id', right_on='parentid', suffixes=['_q', '_a'])
print(qa.iloc[:20,:])
data = qa[['id_q', 'reputation_q', 'reputation_a']].groupby('id_q', as_index=False).agg({'reputation_q':'mean', 'reputation_a':['min','median','max']})
data.columns = ['id', 'qrep', 'arep_min', 'arep_median', 'arep_max']

data = data.sort_values('qrep')
data['qrep'] = data['qrep'].apply(np.log10)


x = []
ymedian = []
ymin = []
ymax = []
ticks = np.array([0., 0.8, 1.01818182, 1.23636364, 1.45454545, 1.67272727, 1.89090909, 2.10909091, 2.32727273, 2.54545455, 2.76363636, 2.98181818, 3.2, 4.])
# ticks = np.linspace(0, 4, 14)
for i in np.nditer(ticks):
	d = data[data['qrep'] <= i]
	data = data[data['qrep'] > i]
	x.append(d['qrep'].mean())
	ymedian.append(d['arep_median'].mean())
	ymin.append(d['arep_min'].mean())
	ymax.append(d['arep_max'].mean())

plt.xticks(range(0,5))
plt.xlabel("Questioner reputation (log base 10)", fontsize=18)
plt.ylabel("Answerer reputation", fontsize=18)
plt.plot(x, ymax, marker='o', markerfacecolor='red', markeredgecolor='black', markersize='12', color='red', linewidth=4, label="Max")
plt.plot(x, ymedian, marker='s', markerfacecolor='black', markeredgecolor='black', markersize='12', color='black', linewidth=4, label="Median")
plt.plot(x, ymin, marker='d', markerfacecolor='blue', markeredgecolor='black', markersize='12', color='blue', linewidth=4, label="Min")
plt.legend()
plt.show()