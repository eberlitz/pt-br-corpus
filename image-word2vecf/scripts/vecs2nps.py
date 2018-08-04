#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import sys

fh = open(sys.argv[1], 'r')
foutname = sys.argv[2]
first = fh.__next__()
size = first.strip().split()

wvecs = np.zeros((int(size[0]), int(size[1])), float)

vocab = []
for i, line in enumerate(fh):
    line = line.strip().split()
    vocab.append(line[0])
    wvecs[i, ] = np.array(list(map(float, line[1:])))

np.save(foutname+".npy", wvecs)
with open(foutname+".vocab", "w") as outf:
    outf.write(" ".join(vocab))
