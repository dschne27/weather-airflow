import os
from datetime import datetime
date = datetime.today().date()
fname = "report-{}.txt".format(date)
path = os.path.join(os.getcwd(), "dags" ,"documents" , fname)
print(path)

f = open(path, 'w')
f.write('hello' + "\n")
f.close()