import csv
import sys
from dbdriver import DB

query = "INSERT INTO clickstream_data(prev_id, curr_id, num_requests, prev_name, curr_name, month) VALUES (?, ?, ?, ?, ?, ?)"
vals = []
ct = 0

month = sys.argv[2]

def read_file(path):
    with open(path, 'rU') as csvfile:
        freader = csv.reader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        for row in freader:
            yield row

for idx, row in enumerate(read_file(sys.argv[1])):
    if idx % 1000 == 0:
    	DB().execute_many(query, vals)
        del vals[:]
        print 'Finished 1000 rows'
        break
    vals.append(tuple(row) + (month,))
    ct = ct + 1
print "Count: ", ct