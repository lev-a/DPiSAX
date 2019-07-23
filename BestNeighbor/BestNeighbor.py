
import sys
import math

if len( sys.argv ) < 2:
    print 'USAGE: python ' + sys.argv[0] + ' <filename>'
    print
    print '      <filename> contains the data sample to examine, one series per line.'
    print '      must be in comma-separated text format:'
    print '        - the first column is the series identifier,'
    print '        - the rest are data values, one per column'
    print
    exit()

energyThreshold = 0.8
filename = sys.argv[1]

def norm(a):
    mean = sum(a)/len(a)
    stdev = math.sqrt( sum( [ (x-mean)*(x-mean) for x in a ] ) / len(a) )
    return [ (x-mean)/stdev for x in a ]

def prefix(a, val, thresh):
    total = sum(val(x) for x in a)
    acc = 0.
    for i in range(len(a)):
        acc = acc + val(a[i])
        if acc >= total*thresh:
            return a[:i+1]

f = open(filename)
data = [l.rstrip().split(',') for l in f.readlines()]
f.close()

data = [norm([float(x) for x in d[1:]]) for d in data]

if len(data) == 0:
    print 'ERROR: input file contains no data!'
    print
    exit()

lens = [len(d) for d in data]
if min(lens) < max(lens):
    print 'ERROR: all series must be of the same size!'
    print
    exit()

tslen = lens[0]

print 'Processing ' + str(len(data)) + ' series of size ' + str(tslen) + ' ...'

import numpy as np

power = [np.square(np.abs(np.fft.fft(d))) for d in data]
power_avg = np.average(power, axis=0)[1:tslen/2]

power_ind = [(i+1, power_avg[i]) for i in range(len(power_avg))]
power_ind.sort(reverse=True, key=lambda x: x[1])

hec = [x[0] for x in prefix(power_ind, lambda x: x[1], energyThreshold)]
l = min(hec)
g = max(hec)

def minseg(g):
    if g < 4:
        return 2
    return int(math.ceil((7.*g-20)/4))

print
print 'Least high energy frequency component:              L = ' + str(l)
print 'Greatest high energy frequency component:           G = ' + str(g)
print
print 'iSAX word length (number of segments):'
print '  - for minimal reasonable pruning:         minseg(G) = ' + str(minseg(g))
print '  - for very good pruning:                        2*G = ' + str(2*g)
print

if g > 30:
    print 'This dataset has substantial power at high frequency components.'
    print 'DPiSAX indexing and/or exact search may be inefficient.'
    print 'Using ParSketch instead is recommended for better approximate search.'
else:
    print 'For efficient DPiSAX indexing and exact search, use word length from the above range.'
    if minseg(g) < 8:
        print 'Recommended minimal word length is 8.'

print

