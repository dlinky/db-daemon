import sqlite3
import os
import daemon
from daemon import pidfile

db_path = os.getcwd()+'/mydb.db'
conn = sqlite3.connect(db_path)
c = conn.cursor()
c.execute("CREATE TABLE IF NOT EXISTS stocks (date text, trans text, symbol test, qty real, price real)")
c.execute("INSERT INTO stocks VALUES ('20200701', 'TEST', 'AIFFEL', 1, 10000)")
c.execute("SELECT * FROM stocks")
conn.commit()
c.close()
conn.close()