from datetime import datetime
from datetime import timedelta
from get_actions import get_actions
import pandas as pd
import pickle
import os


#计算日期差
def lasttime(the_time):
    end_date = end_date_line
    return int((datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(the_time,'%Y-%m-%d')).total_days())

#获取商品各种互动行为总数
def get_product_num(start_date, end_date):
    dump_path = './data/product/product_num_%s_%s.pkl' % (start_date, end_date)
    if os.path.exists(dump_path):
        actions = pickle.load(open(dump_path, 'rb'),)
    else:
        actions = get_actions(start_date, end_date)
        actions = actions[['product_id', 's_num', 'c_num', 'r_num', 'b_num']]

        #各项求和
        actions = actions.groupby(['product_id'], as_index=False).sum()
        pickle.dump(actions, open(dump_path, 'wb'))
    return actions

#获取用户最近一次互动行为的时间与截止时间的间隔，最大间隔默认为30天
def get_product_lasttime(start_date, end_date):
    feature = ['product_id','product_s_lasttime','product_c_lasttime','product_r_lasttime','product_b_lasttime']
    dump_path = './data/product/product_lasttime_%s_%s.pkl' % (start_date, end_date)
    global end_date_line
    end_date_line = end_date

    if os.path.exists(dump_path):
        actions = pickle.load(open(dump_path, 'rb'),)
    else:
        actions = get_actions(start_date, end_date)
        actions = actions[['product_id', 's_num', 'c_num','r_num','b_num','Date']]

        actions = actions.groupby(['product_id', 's_num', 'c_num','r_num','b_num'], as_index=False).last()
        actions['product_s_lasttime'] = actions[actions.s_num != 0].Date.map(lasttime)
        actions['product_c_lasttime'] = actions[actions.c_num != 0].Date.map(lasttime)
        actions['product_r_lasttime'] = actions[actions.r_num != 0].Date.map(lasttime)
        actions['product_b_lasttime'] = actions[actions.b_num != 0].Date.map(lasttime)

        actions = actions.groupby(['product_id'], as_index=False).sum()
        actions = actions[feature]
        actions = actions.fillna(30)
        actions.to_csv('./data/product/product_lasttime_%s_%s.csv' % (start_date, end_date), index=False,index_label=False)
        pickle.dump(actions, open(dump_path, 'wb'))
    return actions


def get_product_feat(start_date, end_date):
    #读取product特征的两个分表
    product_action_num = get_product_num(start_date, end_date)
    product_lasttime = get_product_lasttime(start_date, end_date)

    #合成product特征总表
    actions = pd.merge(product_action_num, product_lasttime, how='left', on=['product_id'])

    # 空值填充
    actions = actions.fillna({'product_s_lasttime':360, 'product_c_lasttime':360,
                              'product_r_lasttime':360, 'product_b_lasttime':360})
    actions.to_csv('./data/product/product_feat_%s_%s.csv' % (start_date, end_date), index=False,
                   index_label=False)


if __name__ == '__main__':
    start_date1 = '2016-01-01'
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

    start_date = [start_date1, start_date2, start_date3, start_date4, start_date5, start_date6]
    end_date = [end_date1, end_date2, end_date3, end_date4, end_date5, end_date6]

    for i in range(0,6):
        get_product_feat(start_date[i], end_date[i])
