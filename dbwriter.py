import datetime

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pymysql
from keras.models import load_model
import time
import tensorflow as tf

df_mean = np.array([21.565917, 31.581203, 629.595461])
df_std = np.array([3.023922, 8.201175, 156.725974])
co2_model = load_model('co2_model.h5')


def dbOpen():
    # 데이터 베이스 연결
    occupancy_db = pymysql.connect(
        user='datacollection',
        passwd='collection2021!@',
        host='121.156.90.144',
        db='db_datacollection',
        charset='utf8'
    )
    cursor = occupancy_db.cursor(pymysql.cursors.DictCursor)
    return occupancy_db, cursor


def dbClose(db, cursor):
    cursor.close()
    db.close()


def dbRead(cursor):
    query_str = 'SELECT sdata_credate, sdata_temp, sdata_humi, sdata_co2 FROM vw_data_composite '
    condition_str = 'WHERE sdata_modbus_id = 1 AND sdata_credate >= "2021-10-19";'
    cursor.execute(query_str+condition_str)
    data = cursor.fetchall()
    return data


def dbwrite(data, db, cursor):
    df = pd.DataFrame(data)
    df = df.set_index('sdata_credate')
    df = df.resample('1T').first()
    interval = 50
    for index in range(0, int(len(df)/10)):
        df_mini = df[index:index+interval]
        last_time = df.index[-1]
        print('processing ', last_time)
        array = np.array(df_mini)
        array = (array - df_mean) / df_std
        array = np.reshape(array, ((1,) + array.shape))
        print('predicting')
        predictions = co2_model.predict(array)[0, :, 2]
        print('predicted')

        for count, prediction in enumerate(predictions):
            if last_time.weekday() < 4:
                if last_time.hour >= 8 or last_time.hour <= 19:
                    people = int((prediction + np.random()*50 - 400)/800*20)
                else:
                    people = 0

            credate = last_time + datetime.timedelta(minutes=count+1)
            print(people, credate)
            #cursor.execute(f"INSERT INTO dc_short_term VALUES ({people}, {credate})")
            #cursor.execute("SELECT * FROM stocks")

        #db.commit()
        time.sleep(1)


def main():
    print('opening db')
    db, cursor = dbOpen()
    print('reading db')
    data = dbRead(cursor)
    print('writing db')
    dbwrite(data, db, cursor)


if __name__ == '__main__':
    main()