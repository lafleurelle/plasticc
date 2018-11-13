import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from  sklearn.model_selection import train_test_split
from catboost import CatBoostClassifier 

train_metadata=pd.read_csv('.\\data\\metafeatures_train.csv')
X=train_metadata.drop(['object_id', 'hostgal_specz', 'target'], axis=1)
y=train_metadata['target']
object_id=train_metadata.object_id

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

cb=CatBoostClassifier(random_state=42, loss_function='MultiClass')
cb.fit(X_train, y_train)

pred_test=cb.predict_proba(X_test)
pred_test=pd.DataFrame(pred_test,\
                  columns=['6', '15', '16', '42', '52', '53', '62', '64', '65', '67', '88', '90', '92', '95'], index=y_test.index)
          
cols=['6', '15', '16', '42', '52', '53', '62', '64', '65', '67', '88', '90', '92', '95']
pred_test['pred_prob']=pred_test[cols].max(axis=1)
pred_test['pred_value']=pred_test[cols].idxmax(axis=1).astype(int)

pred_test=pd.concat([pred_test,y_test], axis=1)



precision_table=[]
recall_table=[]
p_list=[]
f_meas_list=[]
for p in range(0, 101, 1):
    class_determined= pred_test['pred_prob']> p*0.01
    #print(sum(class_determined))
    correctly_determined=pred_test[class_determined].pred_value==pred_test[class_determined].target
    recall=1.0*sum(correctly_determined)/len(pred_test)
    if (sum(class_determined)==0):
        precision=0
        f_meas=0
    else:
        precision=1.0*sum(correctly_determined)/sum(class_determined)
        f_meas=2*precision*recall/(precision+recall)
    
    recall_table.append(recall)
    precision_table.append(precision)
    f_meas_list.append(f_meas)
    p_list.append(p*0.01)
    
    
for i in range(len(f_meas_list)):
    if f_meas_list[i]==max(f_meas_list):
        print ('recall', 'precision', 'p')
        print (recall_table[i], precision_table[i], p_list[i])
        prob_undetermined=p_list[i]
        ratio_99=1-1.0*sum(pred_test['pred_prob']> p_list[i])/len(pred_test)
        print ('ratio of non-clssified objects: ', 1-1.0*sum(pred_test['pred_prob']> p_list[i])/len(pred_test))

test_metadata=pd.read_csv('.\\data\\test_set_metadata.csv')
feats=pd.read_csv('.\\data\\cesium_feats.csv')
test_metadata=pd.concat([test_metadata, feats.drop('object_id', axis=1)], axis=1)
X_test_set=test_metadata.drop(['object_id', 'hostgal_specz'], axis=1)
tr=cb.predict_proba(X_test_set)
zz=feats['object_id']
zz=pd.concat([zz,pd.DataFrame(tr, columns=['class_6', 'class_15', 'class_16', 'class_42', 'class_52', 'class_53', 'class_62', 'class_64', 'class_65', 'class_67', 'class_88', 'class_90', 'class_92', 'class_95'], index=X_test_set.index)], axis=1)
