import MySQLdb

mydb = MySQLdb.connect(host='localhost',
    user='root',
    passwd='password',
    db='test')
cursor = mydb.cursor()
cursor.execute("""CREATE TABLE IF NOT EXISTS test(
    max VARCHAR(255),
    min VARCHAR(255))""")

sql = "INSERT INTO test(max,min) select avg(count_http_code_5xx) + 3* stddev(count_http_code_5xx), avg(count_http_code_5xx) - 3* stddev(count_http_code_5xx) from result" 
cursor.execute(sql)
sql = "update result set is_anomaly = 'Anomaly' where count_http_code_5xx > (select max from test) or count_http_code_5xx < (select min from test)"
cursor.execute(sql)
cursor.execute("""DROP TABLE test""")
mydb.commit()
cursor.close()



