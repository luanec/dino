import csv
import MySQLdb

mydb = MySQLdb.connect(host='localhost',
    user='root',
    passwd='password',
    db='test')
cursor = mydb.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS Temp(
    ts VARCHAR(255),
    api_name VARCHAR(255),
    mtd VARCHAR(255),
    cod VARCHAR(255))""")
     
cursor.execute("""CREATE TABLE IF NOT EXISTS Result (
    timeframe_start VARCHAR(255),
    api_name VARCHAR(255),
    http_method VARCHAR(255),
    count_http_code_5xx VARCHAR(255),
    is_anomaly INT)""" )

with open('raw_data.csv') as csvfile:
    csv_data = csv.reader(csvfile)
    next (csv_data)
    
    for row in csv_data:
#        if row[4] >= '500':
            cursor.execute('INSERT INTO Temp(ts,api_name, mtd, cod)' ' VALUES("%s","%s","%s","%s")' % (row[0],row[1]+' '+row[2],row[3],row[4]) )
            z=row
 
a=0
while a<3:
    j=0
    while j<10:
        i=15
        while i <= 60:
            sql = "insert into Result (timeframe_start,api_name, http_method, count_http_code_5xx, is_anomaly) SELECT '%s %s%s:%s:00', api_name,mtd, count( case when cod > '499' and cod < '600' then 1 else null end), '0' FROM (select * from test.temp where ts < '%s %s%s:%s:00' and ts >= '%s %s%s:%s0:00' ) table1 group by api_name,mtd"  %(z [0][0:10],a,j,i-15,z[0][0:10],a,j,i,z[0][0:10],a,j,i-15)
            cursor.execute(sql)
                
            i+=15
        j+=1
    a+=1


cursor.execute("""DROP TABLE Temp""")
mydb.commit()
cursor.close()
