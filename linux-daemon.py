import time
from daemon import runner
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
    condition_str = 'WHERE sdata_modbus_id = 1 AND sdata_credate >= now() - INTERVAL 1 HOUR;'
    cursor.execute(query_str+condition_str)
    data = cursor.fetchall()
    return data


def dbWrite(data, db, cursor):
    df = pd.DataFrame(data)
    df = df.set_index('sdata_credate')
    df = df.resample('1T').first()

    df_mini = df[-50:]
    last_time = df_mini.index[-1]

    # 예측값을 얻기 위해 학습데이터 기준으로 정규화
    array = np.array(df_mini)
    array = (array - df_mean)/df_std
    array = np.reshape(array, ((1,) + array.shape))
    predictions = co2_model.predict(array)[0]

    # 예측값을 직관적으로 보정하기 위해 정규화를 역으로 연산
    predictions = predictions * df_std + df_mean
    predictions = predictions[:, 2]
    for count, prediction in enumerate(predictions):
        # 주말, 근무외시간은 0명으로 계수
        if last_time.weekday() < 4:
            if 8 <= last_time.hour <= 19:
                people = int((prediction + np.random.rand()*50 - 400)/800*20)
            else:
                people = 0
        else:
            people = 0

        #db에 예측값 기록
        credate = last_time + datetime.timedelta(minutes=count+1)
        if cursor.execute("SELECT EXISTS (SELECT * from dc_short_term WHERE regdate = %s limit 1) AS SUCCESS;", (credate)) == 1:
            continue
            #cursor.execute("UPDATE dc_short_term SET data = %s WHERE regdate = %s", (people, credate))
        cursor.execute("INSERT INTO dc_short_term (regdate, data) VALUES (%s, %s)", (credate, people))

    db.commit()
    #5분마다 예측값 기록
    time.sleep(300)

#unix 데몬으로 상주시킴
class App():
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path = '/tmp/foo.pid'
        self.pidfile_timeout = 5
        self.db, self.cursor = dbOpen()

    def run(self):
        while True:
            data = dbRead(self.cursor)
            dbWrite(data, self.db, self.cursor)

app = App()
daemon_runner = runner.DaemonRunner(app)
daemon_runner.do_action()