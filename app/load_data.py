import csv
import sys
from dbdriver import DB

query = "INSERT INTO pageviews(prev_id, curr_id, num_requests, prev_name, curr_name) VALUES (?, ?, ?, ?, ?)"
vals = []
with open(sys.argv[1], 'rb') as csvfile:
	freader = csv.reader(csvfile, delimiter='\t')
	for row in freader:
		vals.append(tuple(row))
DB('db/pageview_stats.db').execute_many(query, vals)