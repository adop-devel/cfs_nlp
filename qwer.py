# Updating the database with new adult url.
import mysql.connector

#methods
def querryF(select, create, insert):
    cur.execute(select)
    print('selected')
    data = cur.fetchall()
    lcur.execute(create)
    lcur.executemany(insert, data)
    print('Inserted Data into DB')

# Connects to the local mysql here and automatically commits anything.
lconn = mysql.connector.connect(host='', user='',passwd='') #Enter in your host name, username, and password to your mysql acc.
lconn.autocommit = True
lcur = lconn.cursor(buffered=True)
print('connected')
lcur.execute('USE cfs;')


conn = mysql.connector.connect(host='', user='',passwd='') #Enter in host name, username, and password to the database with the necessary websites to collect
# Connects with the online database
cur = conn.cursor()
adultQ = "SELECT adt_idx, adt_url, adt_url_md5, adt_reg_dt, adt_yn FROM i_adult_url WHERE MOD(adt_idx, 10) = 5"
# The MOD section is to segment the data into 10 different sections so that the script can allocate each one separately. Go from 0 to 9 to change which section to select and insert.
adultC = "CREATE TABLE IF NOT EXISTS adttable(id INT(11) NOT NULL, url VARCHAR(1024) NOT NULL, md5 CHAR(32), dt DATETIME, yn CHAR(1), parsed TINYINT(1) DEFAULT 0, PRIMARY KEY (id));"
adultI = "INSERT IGNORE INTO adttable(id, url, md5, dt, yn) VALUES (%s, %s, %s, %s, %s);"

cur.execute('USE insight;')
querryF(adultQ, adultC, adultI)
cur.close()

print('Goodbye')
