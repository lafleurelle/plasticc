import os
from collections import OrderedDict
import numpy as np
import matplotlib.pyplot as plt
from astropy.table import Table
import multiprocessing
from cesium.time_series import TimeSeries
import cesium.featurize as featurize
from tqdm import tnrange, tqdm_notebook
import sklearn 
import pandas as pd
import warnings
warnings.simplefilter('ignore')
import csv
import gc

def worker(tsobj):
    global features_to_use
    thisfeats = featurize.featurize_single_ts(tsobj,\
    features_to_use=features_to_use,
    raise_exceptions=False)
    return thisfeats

pbmap = OrderedDict([(0,'u'), (1,'g'), (2,'r'), (3,'i'), (4, 'z'), (5, 'Y')])
pbcols = OrderedDict([(0,'blueviolet'), (1,'green'), (2,'red'),\
                      (3,'orange'), (4, 'black'), (5, 'brown')])
pbnames = list(pbmap.values())

features_to_use = ["amplitude",
                   "percent_beyond_1_std",
                   "maximum",
                   "max_slope",
                   "median",
                   "median_absolute_deviation",
                   "percent_close_to_median",
                   "minimum",
                   "skew",
                   "std",
                   "weighted_average"]
                   
lcfilename_test = '../input/PLAsTiCC-2018/test_set.csv'

tsdict_test = OrderedDict()

with open(lcfilename_test, "r") as csvfile:
    datareader = csv.reader(csvfile)
    timeserie = []
    object_num = 0
    features_list = []
    for row in datareader:
        if timeserie==[]:
            object_num = row[0]
            timeserie.append(row)
        else:
            if row[0]==object_num:
               timeserie.append(row)
            else:
                if object_num!='object_id':
                    test_table=Table(rows=timeserie, names=['object_id', 'mjd', 'passband', 'flux', 'flux_err', 'detected'],\
                                    dtype=['i8', 'f8', 'i4', 'f8', 'f8', 'b'])
                    pbind = [(test_table['passband'] == pb) for pb in pbmap]
                    thisid=test_table['object_id'][0]
                    t = [test_table['mjd'][mask].data for mask in pbind ]
                    m = [test_table['flux'][mask].data for mask in pbind ]
                    e = [test_table['flux_err'][mask].data for mask in pbind ]
                    tsdict_test[thisid] = TimeSeries(t=t, m=m, e=e,\
                                                     name=thisid)
                    if len(tsdict_test.values())>200000:
                        print (200000)
                        with multiprocessing.Pool() as pool:  
                            results = pool.imap(worker, list(tsdict_test.values()))
                            for res in results:
                                features_list.append(res)
                        tsdict_test.clear()
                        del results
                        gc.collect()
                timeserie=[]
                timeserie.append(row)
                object_num = row[0]
if len(tsdict_test.values())>0:
    with multiprocessing.Pool() as pool:  
        results = pool.imap(worker, list(tsdict_test.values()))
        for res in results:
            features_list.append(res)
    tsdict_test.clear()
