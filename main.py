import mysql.connector
import json
import requests


###  DB connection
# mydb = mysql.connector.connect(
#     host = "localhost", 
#     user = "kartaca",
#     password = "kartaca.123",
#     database = "kartaca-db"
# )
mydb = mysql.connector.connect(
    host = "localhost", 
    user = "kartaca",
    password = "kartaca",
    database = "kartaca-db"
)
###  Creating -clearing if exists- Tables
mycursor = mydb.cursor()
try:
    mycursor.execute("CREATE TABLE country (code VARCHAR(4), name VARCHAR(255))")
    mycursor.execute("CREATE TABLE currency (code VARCHAR(4), curr VARCHAR(4))")
    mycursor.execute("CREATE TABLE datamerge (name VARCHAR(255), curr VARCHAR(4))")

except mysql.connector.errors.ProgrammingError:
    mycursor.execute("Truncate table country")
    mycursor.execute("Truncate table currency")
    mycursor.execute("Truncate table datamerge")
else:
    print("Unknown issue.")

mydb.close()
#Table insert functions.
def insertData(code,curr):
    conn = mysql.connector.connect(host = "localhost",user = "kartaca",password = "kartaca.123",database = "kartaca-db")
    mycursor = conn.cursor()
    sql = f"INSERT INTO currency(code, curr) VALUES(%s,%s)"
    values = (code, curr)
    mycursor.execute(sql,values)
    try:
        conn.commit()
    except mysql.connector.Error as err:
        print("error: ", err)
    finally:
        conn.close()

def insertData2(code,curr):
    conn = mysql.connector.connect(host = "localhost",user = "kartaca",password = "kartaca.123",database = "kartaca-db")
    mycursor = conn.cursor()
    sql = f"INSERT INTO country(code, name) VALUES(%s,%s)"
    values = (code, curr)
    mycursor.execute(sql,values)
    try:
        conn.commit()
    except mysql.connector.Error as err:
        print("error: ", err)
    finally:
        conn.close()


#///////////////////////////////////////////////////////////
url = "http://country.io/currency.json"
dataCurrency = json.loads(requests.request("GET",url).text)

for code in dataCurrency:
     insertData(code, dataCurrency[code])

###  Getting datas from links

url2 = "http://country.io/names.json"
dataNames = json.loads(requests.request("GET",url2).text)

for code in dataNames:
    insertData2(code, dataNames[code])

###  merge script
# mydb = mysql.connector.connect(
#     host = "localhost", 
#     user = "kartaca",
#     password = "kartaca.123",
#     database = "kartaca-db"
# )
mydb = mysql.connector.connect(
    host = "localhost", 
    user = "kartaca",
    password = "kartaca",
    database = "kartaca-db"
)

mycursor = mydb.cursor()

mycursor.execute("INSERT INTO datamerge (SELECT name, curr FROM country,currency WHERE country.code = currency.code)")

mydb.commit()
mydb.close()