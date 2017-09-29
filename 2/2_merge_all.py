# -*- coding: UTF-8 -*-
import pandas as pd

def merge_train_set(train_start_date, train_end_date):

    #读入商品数据，确定筛选范围
    product_required = pd.read_csv( './data/Data_Seller_Product.csv')

    #交互特征
    user_product_feat = pd.read_csv('./data/user_product/user_product_feat_%s_%s.csv' % (train_start_date, train_end_date))

    #基本特征
    user_feat = pd.read_csv('./data/user/user_feat_%s_%s.csv' % (train_start_date, train_end_date))
    product_feat = pd.read_csv('./data/product/product_feat_%s_%s.csv' % (train_start_date, train_end_date))

    #标签 labels
    labels = pd.read_csv('./cache/labels/labels_%s_%s.csv' % (train_start_date, train_end_date))

    #初始
    actions = user_product_feat

    #筛选满足要求的商品
    actions = pd.merge(actions, product_required, how='inner', on='product_id')

    #基本特征合表
    actions = pd.merge(actions, user_feat, how = 'inner', on = 'user_id')
    actions = pd.merge(actions, product_feat, how = 'inner', on = 'sku_id')

    #添加标签
    actions = pd.merge(actions, labels, how='left', on = ['user_id', 'product_id'])

    #空置处理
    actions = actions.fillna(0)

    #保存
    actions.to_csv('./data/train/train_set_%s_%s.csv' % (train_start_date, train_end_date), index=False, index_label=False)


def merge_predict_set(predict_start_date, predict_end_date):
    # 读入商品数据，确定筛选范围
    product_required = pd.read_csv('./data/Data_Seller_Product.csv')

    # 交互特征
    user_product_feat = pd.read_csv(
        './data/user_product/user_product_feat_%s_%s.csv' % (predict_start_date, predict_end_date))

    # 基本特征
    user_feat = pd.read_csv('./data/user/user_feat_%s_%s.csv' % (predict_start_date, predict_end_date))
    product_feat = pd.read_csv('./data/product/product_feat_%s_%s.csv' % (predict_start_date, predict_end_date))

    # 标签 labels
    labels = pd.read_csv('./cache/labels/labels_%s_%s.csv' % (predict_start_date, predict_end_date))

    # 初始
    actions = user_product_feat

    # 筛选满足要求的商品
    actions = pd.merge(actions, product_required, how='inner', on='product_id')

    # 基本特征合表
    actions = pd.merge(actions, user_feat, how='inner', on='user_id')
    actions = pd.merge(actions, product_feat, how='inner', on='sku_id')

    # 空置处理
    actions = actions.fillna(0)

    # 保存
    actions.to_csv('./data/predict/predict_set_%s_%s.csv' % (predict_start_date, predict_end_date), index=False,
                   index_label=False)



if __name__ == '__main__':

    end_date1 = '2016-01-31'

    start_date2 = '2016-02-01'
    end_date2 = '2016-02-29'

    start_date3 = '2016-03-01'
    end_date3 = '2016-03-31'

    start_date4 = '2016-04-01'
    end_date4 = '2016-04-30'

    start_date5 = '2016-05-01'
    end_date5 = '2016-05-31'

    start_date6 = '2016-06-01'
    end_date6 = '2016-06-30'

    train_start_date = [start_date1, start_date2, start_date3, start_date4, start_date5]
    train_end_date = [end_date1, end_date2, end_date3, end_date4, end_date5]

    predict_start_date = [start_date6]
    predict_end_date = [end_date6]

    for i in range(0,5):
        merge_train_set(train_start_date[i], train_end_date[i])

    merge_predict_set(predict_start_date[0], predict_end_date[0])






