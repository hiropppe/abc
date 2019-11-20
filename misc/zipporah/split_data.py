import numpy as np
import sys

data = [l.strip() for l in open(sys.argv[1])]

np.random.seed(123)
indices = np.random.permutation(len(data))

bad = float(sys.argv[2])
dev = float(sys.argv[3])

bad_size = int(len(data) * bad)
dev_size = int(len(data) * dev)

bad_indices = indices[:bad_size]
dev_indices = indices[bad_size: bad_size + dev_size]
good_indices = indices[bad_size + dev_size:]

with open(sys.argv[4], "w") as good:
    for i in good_indices:
        print(data[i], file=good)

with open(sys.argv[5], "w") as bad:
    for i in bad_indices:
        print(data[i], file=bad)

with open(sys.argv[6], "w") as dev:
    for i in dev_indices:
        print(data[i], file=dev)
