import pandas as pd
import pymysql
import time


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
    condition_str = 'WHERE sdata_modbus_id = 1 AND sdata_credate >= (NOW() - INTERVAL 2 DAY);'
    cursor.execute(query_str+condition_str)
    data = cursor.fetchall()
    df = pd.DataFrame(data)
    df = df.set_index('sdata_credate')
    df = df.resample('1T').first()
    return df


def predict_short_term(df):
    INPUT_LEN = 50
    PREDICTION_LEN = 10
    print('input : %d' % INPUT_LEN)
    print('prediction : %d' % PREDICTION_LEN)
    df = df.fillna(method='pad')
    df = df[-50:]
    print('dataset :')
    print(df)


def dbwrite(db, cursor):
    cursor.execute("")
    cursor.execute("INSERT INTO ")
    cursor.execute("SELECT * FROM stocks")
    db.commit()


db, cursor = dbOpen()
df = dbRead(cursor)
predict_short_term(df)