import servicemanager
import socket
import sys
import win32event
import win32service
import win32serviceutil
import sqlite3
import os


class TestService(win32serviceutil.ServiceFramework):
    _svc_name_ = "TestService"
    _svc_display_name_ = "Test Service"
    _svc_description_ = "My service description"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        rc = None
        connect, cursor = dbOpen()
        while rc != win32event.WAIT_OBJECT_0:
            dbwrite(connect, cursor)
            rc = win32event.WaitForSingleObject(self.hWaitStop, 5000)


def dbOpen():
    db_path = os.getcwd() + '/mydb.db'
    connect = sqlite3.connect(db_path)
    cursor = connect.cursor()
    return connect, cursor


def dbClose(connect, cursor):
    cursor.close()
    connect.close()


def dbwrite(connect, cursor):
    cursor.execute("CREATE TABLE IF NOT EXISTS sensors (timestamp time a real b real c real)")
    cursor.execute("INSERT INTO ")
    cursor.execute("SELECT * FROM stocks")
    connect.commit()


if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(TestService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(TestService)