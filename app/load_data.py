import csv
import sys
from dbdriver import DB

query = "INSERT INTO clickstream_data(prev_id, curr_id, num_requests, prev_name, curr_name, month) VALUES (%s, %s, %s, %s, %s, %s)"
vals = []
ct = 0

month = sys.argv[2]

def read_file(path):
    with open(path, 'rU') as csvfile:
        freader = csv.reader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        for row in freader:
            yield row

for idx, row in enumerate(read_file(sys.argv[1])):
    if (idx > 0 and idx % 10000 == 0):
    	DB().execute_many(query, vals)
        del vals[:]
        print 'Finished 10000 rows'
        break
    if row[0] == '':
    	row[0] = None
    if row[1] == '':
    	row[1] = None
    vals.append(tuple(row) + (month,))
    ct = ct + 1
print "Count: ", ct