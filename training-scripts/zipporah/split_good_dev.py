import lzma
import numpy as np
import sys
import subprocess

from tqdm import tqdm


prefix = sys.argv[1]
slang = sys.argv[2]
tlang = sys.argv[3]

sinp = f"{prefix}.{slang}-{tlang}.{slang}.xz"
tinp = f"{prefix}.{slang}-{tlang}.{tlang}.xz"

s_size = int(subprocess.check_output(f'xzcat {sinp} | wc -l', shell=True).split()[0])
t_size = int(subprocess.check_output(f'xzcat {tinp} | wc -l', shell=True).split()[0])

assert s_size == t_size

data_size = s_size

np.random.seed(123)
indices = np.random.permutation(data_size)

if len(sys.argv) > 5:
    good = float(sys.argv[4])
    dev = float(sys.argv[5])
else:
    good = None
    dev = float(sys.argv[4])

if dev < 1.0:
    dev_size = int(data_size * dev)
else:
    dev_size = int(dev)

if good:
    if good < 1.0:
        good_size = int(data_size * dev)
    else:
        good_size = int(good)
    dev_indices = indices[:dev_size]
    good_indices = indices[dev_size:dev_size+good_size]
else:
    dev_indices = indices[:dev_size]
    good_indices = indices[dev_size:]

s_good_out = f"{prefix}.good.{slang}-{tlang}.{slang}.xz"
s_dev_out = f"{prefix}.dev.{slang}-{tlang}.{slang}.xz"
t_good_out = f"{prefix}.good.{slang}-{tlang}.{tlang}.xz"
t_dev_out = f"{prefix}.dev.{slang}-{tlang}.{tlang}.xz"

with lzma.open(s_good_out, "wt") as go, lzma.open(s_dev_out, "wt") as do:
    with lzma.open(sinp) as si:
        for i, line in tqdm(enumerate(si)):
            line = line.decode("utf8").strip()
            if i in good_indices:
                print(line, file=go)
            if i in dev_indices:
                print(line, file=do)

with lzma.open(t_good_out, "wt") as go, lzma.open(t_dev_out, "wt") as do:
    with lzma.open(tinp) as ti:
        for i, line in tqdm(enumerate(ti)):
            line = line.decode("utf8").strip()
            if i in good_indices:
                print(line, file=go)
            if i in dev_indices:
                print(line, file=do)
