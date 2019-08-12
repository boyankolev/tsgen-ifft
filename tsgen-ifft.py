import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-fn", "--freqnum", help="number of significant frequencies", type=int, required=True)
parser.add_argument("-fs", "--freqstart", help="first frequency index", default=0)
parser.add_argument("-tl", "--tslen", help="length of time series", type=int, required=True)
parser.add_argument("-tn", "--tsnum", help="number of time series", type=int, required=True)
parser.add_argument("-o", "--outfile", help="output file", required=True)
args = parser.parse_args()

k = args.freqnum
startk = args.freqstart
tslen = args.tslen
tsnum = args.tsnum
outfile = args.outfile


import random
import numpy as np

def genTS(k, startk, tslen, s):
  random.seed(s)
  
  freecoefs = [random.random() for i in range(tslen/2 - 1 - k)]
  w = [1 for i in range(k)]
  bw = (tslen*tslen/2 - sum(freecoefs)) / sum(w)
  
  coefs = np.sqrt(np.concatenate( (freecoefs[:startk], bw*np.array(w), freecoefs[startk:]) ))
  coefs_all = np.concatenate(([0.], coefs, [(random.random()-0.5) * 2], coefs[::-1]))
  
  angles = np.array([(random.random()-0.5) * 2*np.pi for i in range(tslen/2 - 1)])
  angles_all = np.concatenate(([0.], angles, [0.], -angles[::-1]))
  
  tsft = [r * np.exp(1j * fi) for r, fi in zip(coefs_all, angles_all)]
  ts = np.fft.ifft(tsft).real + np.array([random.gauss(0, 1)/4 for i in range(tslen)])
  return ts


from pyspark import SparkConf, SparkContext
from pyspark.mllib.random import RandomRDDs

conf = SparkConf()
sc = SparkContext(conf = conf)

RandomRDDs.uniformRDD(sc, tsnum).zipWithIndex() \
  .map( lambda (r, i): (i, genTS(k, startk, tslen, int(r*1000) + (i+1)*1000)) ) \
  .map( lambda (i, ts): str(i) + ',' + ','.join(["%.5f" % x for x in ts]) ) \
  .saveAsTextFile(outfile)

