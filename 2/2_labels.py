from datetime import datetime
from datetime import timedelta
from get_actions import get_actions
import pandas as pd
import pickle
import os

#获取标签数据
def get_labels(start_date, end_date):
    dump_path = './data/labels/labels_%s_%s.pkl' % (start_date, end_date)
    if os.path.exists(dump_path):
        actions = pickle.load(open(dump_path, 'rb'))
    else:
        #注意标签的时间是end_date的后面10天
        start_date1 = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days = 1)
        end_date1 = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days = 10)
        start_date1 = start_date1.strftime('%Y-%m-%d')
        end_date1 = end_date1.strftime('%Y-%m-%d')
        actions = get_actions(start_date1, end_date1)
        actions = actions[actions['b_num'] != 0]
        actions = actions.drop_duplicates(['user_id'])
        actions = actions.groupby(['user_id', 'product_id'], as_index=False).sum()
        actions['label'] = 1
        actions = actions[['user_id', 'product_id', 'label']]
        actions.to_csv('./data/labels/labels_%s_%s.csv' % (start_date, end_date), index=False,index_label=False)
        pickle.dump(actions, open(dump_path, 'wb'))
    return actions

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

    start_date = [start_date1, start_date2, start_date3, start_date4, start_date5]
    end_date = [end_date1, end_date2, end_date3, end_date4, end_date5]

    for i in range(0,5):
        get_labels(start_date[i], end_date[i])
