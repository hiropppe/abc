import gzip
import os
import sys

vcb1 = sys.argv[1]
vcb2 = sys.argv[2]
t3_1 = sys.argv[3]
t3_2 = sys.argv[4]
e2f = sys.argv[5]
f2e = sys.argv[6]

svocabulary = {}
tvocabulary = {}
svcb = open(vcb1, "r")
tvcb = open(vcb2, "r")
for line in svcb:
    item = line.strip().split(" ")
    svocabulary[item[0]] = item[1]

for line in tvcb:
    item = line.strip().split(" ")
    tvocabulary[item[0]] = item[1]

t3s = open(t3_1, "r")
t3t = open(t3_2, "r")
dice2f = gzip.open(e2f, "wt")
dicf2e = gzip.open(f2e, "wt")

for line in t3t:
    item = line.strip().split(" ")
    value = float(item[2])
    if value > 0.1:
        if item[0] in svocabulary and item[1] in tvocabulary:
            dice2f.write("{0} {1} {2}\n".format(
                svocabulary[item[0]], tvocabulary[item[1]], item[2]))

for line in t3s:
    item = line.strip().split(" ")
    value = float(item[2])
    if value > 0.1:
        if item[1] in svocabulary and item[0] in tvocabulary:
            dicf2e.write("{0} {1} {2}\n".format(
                tvocabulary[item[0]], svocabulary[item[1]], item[2]))
svcb.close()
tvcb.close()
t3s.close()
t3t.close()
dice2f.close()
dicf2e.close()
os.sync()
