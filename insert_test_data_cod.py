import sys
import psycopg2
import easygui as eg

from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

files_dir=eg.diropenbox('hjjhvg')
if files_dir==None:
    sys.exit()


# connect to server
myconnect=psycopg2.connect(host='localhost',user='user',password='user',port=5432,database='postgres')
myconnect.set_isolation_level (ISOLATION_LEVEL_AUTOCOMMIT)
mycursor=myconnect.cursor()


# check if database YURB exist
is_exist_db=False
mycursor.execute('SELECT * FROM pg_database')
records = mycursor.fetchall()
for rec in records:
    if rec[1].lower()=="yurb":
        is_exist_db=True
        break


# DROP database YURB
if is_exist_db:
    try:
        mycursor.execute('DROP DATABASE yurb')
    except psycopg2.Error as err:
        print(err)
        print("Не удалось пересоздать YURB database. Закройте все открытые сеансы с YURB и повторите.")
        mycursor.close()
        myconnect.close()
        sys.exit()

# Create data base YURB
mycursor.execute('CREATE DATABASE yurb')



mycursor.close()
myconnect.close()

# connect to database YURB
myconnect=psycopg2.connect(host='localhost',user='user',password='user',port=5432,database='yurb')
myconnect.set_isolation_level (ISOLATION_LEVEL_AUTOCOMMIT)
mycursor=myconnect.cursor()

#create table "users"
crt_user_table="CREATE TABLE users (userid serial, age int)"
mycursor.execute(crt_user_table)
myconnect.commit()

# INSERT test data into table "users"
copyintousers=r"COPY users FROM '"+files_dir+"\\users.txt' WITH (FORMAT csv)"
mycursor.execute(copyintousers)
myconnect.commit()


#create table "items"
crt_item_table="CREATE TABLE items (itemid serial, price numeric(8,2))"
mycursor.execute(crt_item_table)
myconnect.commit()

# INSERT test data into table "items"
copyintoitems="COPY items FROM '"+files_dir+"\\items.txt' WITH (FORMAT csv)"
mycursor.execute(copyintoitems)
myconnect.commit()



#create table "purchases"
crt_purch_table="CREATE TABLE purchases (purchaseid serial, userid int,itemid int,date date)"
mycursor.execute(crt_purch_table)
myconnect.commit()

# INSERT test data into table "purchases"
copyintopurchases="COPY purchases FROM '"+files_dir+"\\purchases.txt' WITH (FORMAT csv)"
mycursor.execute(copyintopurchases)
myconnect.commit()

        
   
# disconnect from server    
mycursor.close()
myconnect.close()

print('ОК')

