import csv
import sys
from dbdriver import SQLiteDB

query = "INSERT INTO pageviews(prev_id, curr_id, num_requests, prev_name, curr_name) VALUES (?, ?, ?, ?, ?)"
vals = []
with open(sys.argv[1], 'rb') as csvfile:
	freader = csv.reader(csvfile, delimiter='\t')
	for row in freader:
		vals.append(tuple(row))
SQLiteDB().execute_many(query, vals)