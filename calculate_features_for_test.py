from collections import OrderedDict
import matplotlib.pyplot as plt
from astropy.table import Table
import multiprocessing
from cesium.time_series import TimeSeries
import cesium.featurize as featurize
import warnings
warnings.simplefilter('ignore')
import csv
import gc
import numpy as np
import pandas as pd

def worker(tsobj):
    global features_to_use
    thisfeats = featurize.featurize_single_ts(tsobj,\
    features_to_use=features_to_use,
    raise_exceptions=False)
    return thisfeats

pbmap = OrderedDict([(0,'u'), (1,'g'), (2,'r'), (3,'i'), (4, 'z'), (5, 'Y')])

def pooler(tsdict, func):
    with multiprocessing.Pool() as pool:  
        results = pool.imap(func, list(tsdict.values()))
        for res in results:
            features_list.append(res)
        del results, res, 
    return features_list

def init_list_of_objects(size):
    list_of_objects = list()
    for i in range(0,size):
        list_of_objects.append([])
    return list_of_objects

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
    object_num = 0
    features_list = []
    rows_count=0
    t=init_list_of_objects(6)
    m=init_list_of_objects(6)
    e=init_list_of_objects(6)
    objects_list=[]
    total_feats=pd.DataFrame()
    for row in datareader:
        if rows_count==0:
            names=row
            rows_count+=1
        elif rows_count==1:
            object_num = row[0]
            for pb in range(6):
                if int(row[2])==pb:
                    t[pb].append(float(row[1]))
                    m[pb].append(float(row[3]))
                    e[pb].append(float(row[4]))
            rows_count+=1
        else:
            if row[0]==object_num:
                rows_count+=1
                for pb in range(6):
                    if int(row[2])==pb:
                        t[pb].append(float(row[1]))
                        m[pb].append(float(row[3]))
                        e[pb].append(float(row[4]))
            else:
                rows_count+=1
                t1=[np.array(i) for i in t]
                m1=[np.array(i) for i in m]
                e1=[np.array(i) for i in e]
                thisid=int(object_num)
                objects_list.append(thisid)
                tsdict_test[thisid] = TimeSeries(t=t1, m=m1, e=e1,name=thisid)
                
                if len(tsdict_test.values())%100000==0:

                    features_list=pooler(tsdict_test, worker)
                    featuretable = featurize.assemble_featureset(features_list=features_list,\
                                                  time_series=tsdict_test.values())
                    total_feats=pd.concat([total_feats, featuretable], axis=0)
                    tsdict_test=OrderedDict()
                    features_list=[]
                    gc.collect()

                del t[:], m[:], e[:], t, m, e
                
                t=init_list_of_objects(6)
                m=init_list_of_objects(6)
                e=init_list_of_objects(6)
                object_num = row[0]
                
                for pb in range(6):
                    if int(row[2])==pb:
                        t[pb].append(float(row[1]))
                        m[pb].append(float(row[3]))
                        e[pb].append(float(row[4]))
if len(tsdict_test.values())>0:
    features_list=pooler(tsdict_test, worker)
    featuretable = featurize.assemble_featureset(features_list=features_list,\
                                                  time_series=tsdict_test.values())
    total_feats=pd.concat([total_feats, featuretable], axis=0)

