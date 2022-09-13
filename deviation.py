from ast import With
import pandas as pd
import sys

pid = sys.argv[1].strip()

data = pd.read_csv(f"/usr/local/hdsetup/hadoop_store/hdfs/datanode/current/project/{pid}.csv/demo.csv")
data2 = pd.read_csv("./disease_test.csv")

dis = data["disease_name"][0]
ds_score = 0

for i in range(len(data2)):
    x = data2.iloc[i]
    if x[0] == dis:
        ds_score += x[-1] * x[-2]

dg_score = data["Total Drugs Cost"][0]

deviation = (ds_score - dg_score) / ds_score * 100

dev = str(round(deviation, 2))

with open('deviation', 'w') as f:
    f.write(dev)
