from datetime import datetime
from datetime import timedelta
import pandas as pd
import pickle
import os

action_1_path = "./Data_Action_201601/*.csv"
action_2_path = "./Data_Action_201602/*.csv"
action_3_path = "./Data_Action_201603/*.csv"
action_4_path = "./Data_Action_201604/*.csv"
action_5_path = "./Data_Action_201605/*.csv"
action_6_path = "./Data_Action_201606/*.csv"

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

def get_actions(start_date, end_date):
    dump_path = './data/action/all_action_%s_%s.pkl' % (start_date, end_date)
    if os.path.exists(dump_path):
        actions = pickle.load(open(dump_path, 'rb'))
    else:
        if start_date == start_date1:
            actions =pd.read_csv(action_1_path)
        elif start_date == start_date2:
            actions = pd.read_csv(action_2_path)
        elif start_date == start_date3:
            actions = pd.read_csv(action_3_path)
        elif start_date == start_date4:
            actions = pd.read_csv(action_4_path)
        elif start_date == start_date5:
            actions = pd.read_csv(action_5_path)
        elif start_date == start_date6:
            actions = pd.read_csv(action_6_path)
        else:
            return -1
        actions = actions[actions.time >= start_date]
        actions = actions[actions.time <=end_date]
        pickle.dump(actions, open(dump_path, 'wb'))
    return actions

if __name__ == '__main__':

    start_date = [start_date1, start_date2, start_date3, start_date4, start_date5, start_date6]
    end_date = [end_date1, end_date2, end_date3, end_date4, end_date5, end_date6]

    for i in range(0,5):
        get_actions(start_date[i], end_date[i])

