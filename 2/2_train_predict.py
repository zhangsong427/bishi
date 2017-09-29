__author__ = 'zhangsong'
from labels import get_labels
import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
from xgboost import plot_tree
from xgboost import plot_importance
import matplotlib.pyplot as plt
import operator


def xgboost():
    #训练数据集和预测数据集
    train_start_date = '2016-01-01'
    train_end_date = '2016-01-31'

    predict_start_date = '2016-06-01'
    predict_end_date = '2016-06-30'

    path_train =  './data/train/train_set_%s_%s.csv'%(train_start_date, train_end_date)
    path_predict = './data/predict/train_set_%s_%s.csv'%(predict_start_date, predict_end_date)

    train_set = pd.read_csv(path_train)
    label = train_set['label'].copy()
    del train_set['user_id']
    del train_set['product_id']
    del train_set['label']

    predict_set = pd.read_csv(path_predict)
    users_product_predict = predict_set[['user_id', 'product_id']].copy()
    del predict_set['user_id']
    del predict_set['product_id']


    #模型训练
    X_train, X_test, y_train, y_test = train_test_split(train_set.values, label.values, test_size=0.2, random_state=0)
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)
    param = {'learning_rate': 0.1, 'n_estimators': 1000, 'max_depth': 3,
             'min_child_weight': 5, 'gamma': 0, 'subsample': 1.0, 'colsample_bytree': 0.8,
             'scale_pos_weight': 1, 'eta': 0.05, 'silent': 1, 'objective': 'binary:logistic','nthread': 4}

    num_round = 150
    plst = list(param.items())
    plst += [('eval_metric', 'logloss')]
    evallist = [(dtest, 'eval'), (dtrain, 'train')]
    bst = xgb.train(plst, dtrain, num_round, evallist)

    #特征权重分析
    importance = bst.get_fscore()
    importance = sorted(importance.items(), key=operator.itemgetter(1))

    df = pd.DataFrame(importance, columns=['feature', 'fscore'])
    df['fscore'] = df['fscore'] / df['fscore'].sum()

    data = train_set.head(10)
    data.to_csv('./data/index_im.csv', index=False, index_label=False)
    df.to_csv('./data/feature_im.csv', index=False, index_label=False)


    #模型预测
    predict_data = xgb.DMatrix(predict_set.values)
    y = bst.predict(predict_data)

    pred = users_train_sub.copy()
    pred['label'] = y
    pred = pred.sort_values(['product_id', 'label'], ascending=False)

    #取前20名并保存结果
    pred = pred.groupby('product_id').head(20).reset_index()
    pred.to_csv('./data/Data_Seller_Product_User.csv', index=False, index_label=False)


if __name__ == '__main__':
    xgboost()
