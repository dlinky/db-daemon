import servicemanager
import socket
import sys
import win32service
import win32serviceutil
import pymysql
import pandas as pd
import win32


class TestService(win32serviceutil.ServiceFramework):
    _svc_name_ = "TestService"
    _svc_display_name_ = "Test Service"
    _svc_description_ = "My service description"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        rc = None
        db, cursor = dbOpen()
        while rc != win32.WAIT_OBJECT_0:
            data = dbRead(cursor)
            df = pd.DataFrame(data)
            df.to_csv('test.csv')
            rc = win32.WaitForSingleObject(self.hWaitStop, 5000)


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
    return data


def dbwrite(db, cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS sensors (timestamp time a real b real c real)")
    cursor.execute("INSERT INTO ")
    cursor.execute("SELECT * FROM stocks")
    db.commit()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TestService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TestService)