import sys
import psycopg2
import pandas

from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# connect to database YURB
myconnect=psycopg2.connect(host='localhost',user='user',password='user',port=5432,database='yurb')
myconnect.set_isolation_level (ISOLATION_LEVEL_AUTOCOMMIT)
mycursor=myconnect.cursor()

# Задание А
#У вас SQL база с таблицами:
#1) Users(userId, age)
#2) Purchases (purchaseId, userId, itemId, date)
#3) Items (itemId, price).

#Напишите SQL запросы для расчета следующих метрик:




#А) какую сумму в среднем в месяц тратит:
#- пользователи в возрастном диапазоне от 18 до 25 лет включительно
#- пользователи в возрастном диапазоне от 26 до 35 лет включительно


# RUN SQL "A"
sql_A="""select t.usr,round(avg(t.monsum),2) as avg_monthly_sum from (select 
CASE 
	WHEN u.age BETWEEN 18 AND 25 THEN 'users_18_25'
	WHEN u.age BETWEEN 26 AND 35 THEN 'users_26_35'
END as usr,
to_char(p.date,'MM YYYY') as mon,
SUM(i.price) as monsum 
from users u,purchases p,items i
where p.userid=u.userid and p.itemid=i.itemid and u.age between 18 and 35 
group by mon,usr) t group by t.usr ;
"""
mycursor.execute(sql_A)
myconnect.commit()
result = mycursor.fetchall()

print('****************')
print('Задание А')
print('----SQL query ------------------')
print(sql_A)
print('----------------------')
print('---- result ------------------')
colnames = [desc[0] for desc in mycursor.description]
print('Titles:',colnames)
rows = pandas.Series(result)

print(rows)
print('---- END Задание А ------------------')
print('----  ------------------')



#Б) в каком месяце года выручка от пользователей в возрастном диапазоне 35+ самая большая

# RUN SQL "B"
sql_B="""select 'users_35plus' as usr,t.mon as bestmonth from
(select to_char(p.date,'MM YYYY') as mon,sum(i.price) as monsum
from users u,purchases p,items i
where p.userid=u.userid and p.itemid=i.itemid and u.age>=35 group by mon) t
where t.monsum in
(select max(tt.monsum) as maxmonsum from
 (select to_char(p1.date,'MM YYYY') as mon,sum(i1.price) as monsum
from users u1,purchases p1,items i1
where p1.userid=u1.userid and p1.itemid=i1.itemid and u1.age>=35 group by mon) tt);
"""
mycursor.execute(sql_B)
myconnect.commit()
result = mycursor.fetchall()

print('****************')
print('Задание Б')
print('----SQL query ------------------')
print(sql_B)
print('----------------------')
print('---- result ------------------')
colnames = [desc[0] for desc in mycursor.description]
print('Titles:',colnames)
rows = pandas.Series(result)

print(rows)
print('---- END Задание Б ------------------')
print('----  ------------------')


#В) какой товар обеспечивает дает наибольший вклад в выручку за последний год

# RUN SQL "V"
sql_V="""select 'Best item' as title,t.itemid from
(select i.itemid as itemid,sum(i.price) as itemtotalyear from purchases p,items i
where p.itemid=i.itemid and p.date between (current_date - '1Y':: interval) and current_date  group by i.itemid) t 
where t.itemtotalyear in (select max(tt.itemtotalyear) from
(select i1.itemid as itemid,sum(i1.price) as itemtotalyear from purchases p1,items i1
where p1.itemid=i1.itemid and p1.date between (current_date - '1Y':: interval) and current_date  group by i1.itemid) tt);
"""
mycursor.execute(sql_V)
myconnect.commit()
result = mycursor.fetchall()

print('****************')
print('Задание В')
print('----SQL query ------------------')
print(sql_V)
print('----------------------')
print('---- result ------------------')
colnames = [desc[0] for desc in mycursor.description]
print('Titles:',colnames)
rows = pandas.Series(result)

print(rows)
print('---- END Задание V ------------------')
print('----  ------------------')


#Г) топ-3 товаров по выручке и их доля в общей выручке за любой год

# RUN SQL "G"
sql_G="""select 'top3_2021' as title,t.itemid, round(t.itemtotalyear/tt.totalyear*100,2) as percentage_share from
(select sum(i1.price) as totalyear from purchases p1,items i1
where p1.itemid=i1.itemid and date_part('year', p1.date)='2021') tt,
(select i.itemid as itemid,sum(i.price) as itemtotalyear from purchases p,items i
where p.itemid=i.itemid and date_part('year', p.date)='2021'  group by i.itemid) t 
order by percentage_share desc limit 3;
"""
mycursor.execute(sql_G)
myconnect.commit()
result = mycursor.fetchall()

print('****************')
print('Задание Г')
print('----SQL query ------------------')
print(sql_G)
print('----------------------')
print('---- result ------------------')
colnames = [desc[0] for desc in mycursor.description]
print('Titles:',colnames)
rows = pandas.Series(result)

print(rows)
print('---- END Задание G ------------------')
print('----  ------------------')


   
# disconnect from server    
mycursor.close()
myconnect.close()

print('ОК')

