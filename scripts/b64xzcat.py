#!/usr/bin/env python3
import base64
import lzma
import sys

if len(sys.argv) > 1:
    try:
        b = [int(i) for i in sys.argv[1].split(',')]
        reader = sys.stdin
    except:
        reader = lzma.open(sys.argv[1])
        if len(sys.argv) > 2:
            b = [int(i) for i in sys.argv[2].split(',')]
        else:
            b = [0]
else:
    reader = sys.stdin
    b = [0]

for line in reader:
    if not isinstance(line, str):
        line = line.decode("utf8")
    cols = line.strip().split("\t")
    for i, col in enumerate(cols):
        if i in b:
            cols[i] = base64.b64decode(col).decode("utf8").replace("\n", "\\n")
    print("\t".join(cols))
