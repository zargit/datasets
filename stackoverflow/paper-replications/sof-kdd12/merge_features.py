import pandas as pd 
import numpy as np 
import time
import datetime
import logging
import sys
from functools import reduce

starttime = datetime.datetime.now()
logging.basicConfig(level=logging.INFO, filename=sys.argv[0].split('.')[0]+'.log')

logging.info("Starting to retreive data ... Time passed %s", (datetime.datetime.now()-starttime))
t = [1,3,24,72]

for i in t:
	Sa = pd.read_csv('Sa_%dhour.csv'%(i))
	Sb = pd.read_csv('Sb_%dHour.csv'%(i))
	Sc = pd.read_csv('Sc_%dhour.csv'%(i))
	Sd = pd.read_csv('Sd_%dhour.csv'%(i))
	logging.info("Done retrieving data ... Time passed %s", (datetime.datetime.now()-starttime))

	logging.info("Starting to merge data ... Time passed %s", (datetime.datetime.now()-starttime))
	task2 = pd.merge(Sa[['id', 'reputation', 'noq', 'noa', 'accepted_ratio']], Sb[['id', 'favoritecount', 'pageview', 'pos_votes', 'neg_votes', 'answercount', 'max_reputation_answerer', 'max_score', 'max_score_answerer_reputation']], how='left', on='id')
	task2 = pd.merge(task2, Sc[['id', 'mean_reputation', 'median_reputation', 'max_reputation_contribution_to_sum', 'sum_reputation', 'max_reputation_answer_length', 'max_reputation_answer_comment_count', 'max_score_answer_length', 'max_score_comment_count']], how='left', on='id')
	task2 = pd.merge(task2, Sd[['id', 'mean_answertimegap_seconds', 'median_answertimegap_seconds', 'min_answertimegap_seconds', 'max_score_timerank', 'max_score_answer_wall_clock_arrival', 'max_reputation_answer_timerank', 'max_reputation_answer_wall_clock_arrival']], how='left', on='id')
	task2 = task2.sort_values('id')
	logging.info("Merging complete ... Time passed %s", (datetime.datetime.now()-starttime))

	logging.info("Writing to file ... Time passed %s", (datetime.datetime.now()-starttime))
	task2.to_csv("task1_%d.csv"%(i), index=False)
	logging.info("Writing complete ... Time passed %s", (datetime.datetime.now()-starttime)) 