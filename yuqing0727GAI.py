#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import math
import pymysql
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import mysql.connector
from datetime import date
import numpy as np
import datetime
from gspread_dataframe import set_with_dataframe
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import gspread_dataframe as gd
import psycopg2
import os


def Clear_Pool():
    Query ='''
    SELECT
    sl.warehouse_number,
    t.item_number AS `SKU`,
    concat(item.`name`, '<br>', item.ename) AS `SKU Name`,
    item.storage_type AS `Storage Type`,
    sum(trans.quantity + trans.allocated_qty) AS `Quantity`,
    item.shelf_life AS `Shelf life`,
    ie.reminder_shelf_life_limit AS 'reminder_limit',
    ie.outbound_shelf_life_limit AS 'outbound_limit',
    DATE(CONVERT_TZ(FROM_UNIXTIME( t.expire_dtm ), '+00:00', wh.time_zone)) 'Expiration Date',
    -- DATE(CONVERT_TZ(FROM_UNIXTIME(t.expire_dtm - ie.outbound_shelf_life_limit * 24 * 3600), '+00:00', wh.time_zone)) as 'Outbound Limit Date',
    Datediff(Date(FROM_UNIXTIME(t.expire_dtm - ie.outbound_shelf_life_limit * 24 * 3600)),CURDATE()) `Remained Days to Ship`

    FROM
    wh_storage_location_item t 
    -- not sure if this is the right table to use cuz it has not storage type,
    INNER JOIN wh_storage_location sl ON t.storage_location_id = sl.rec_id 
    AND sl.location_type IN ( 3, 4 )
    INNER JOIN wh_item item ON t.item_number = item.item_number
    AND item.check_shelf_life = 1 
    AND item.storage_type IN (1, 2, 3)
    INNER JOIN wh_item_extend ie ON ie.item_number = item.item_number
    INNER JOIN wh_warehouse wh ON t.warehouse_number = wh.warehouse_number and wh.is_active = 1
    INNER JOIN wh_inventory_transaction trans ON t.item_number = trans.item_number 
    AND t.location_no = trans.location_no 
    AND t.warehouse_number = trans.warehouse_number
    WHERE
    -- sl.warehouse_number = %(inv_id)s
    t.expire_dtm IS NOT NULL
    -- turn on exp control
    AND t.expire_dtm != 0
    -- exclude expired item
    and t.expire_dtm - ie.reminder_shelf_life_limit * 24 * 3600 <= UNIX_TIMESTAMP()
    -- expire date - current date <= reminder_shelf_life_limit 意为该商品已进入reminder_shelf_life_limit
    and t.expire_dtm - ie.outbound_shelf_life_limit * 24 * 3600 > UNIX_TIMESTAMP()
    -- expire date - current date > outbound_shelf_life_limit 意为该商品还可以发货
    -- 该两条rule抓取了所有在reminder日期之内，但是在outbound limit日期之前的商品
    and item.category_id > 299
    -- 排除掉可能漏掉的果蔬品类
    group by t.item_number,sl.warehouse_number,t.expire_dtm
    -- 增加了按照过期时间batch分类的filter
    order by 
    sl.warehouse_number asc,
    CONVERT_TZ(FROM_UNIXTIME(t.expire_dtm - ie.outbound_shelf_life_limit * 24 * 3600), '+00:00', wh.time_zone)
    '''
    server = sshtunnel.SSHTunnelForwarder(('fortress.db.sayweee.net', 22), 
                                      ssh_password='LuCqi1qHV3bk8WZz', 
                                      ssh_username='jiong',
     remote_bind_address=('wms-m1-r1.db.sayweee.net', 3306))

    server.start()
    conn = pymysql.connect(host='127.0.0.1', 
                           user='jiong', 
                           passwd='LuCqi1qHV3bk8WZz', 
                           db='wms',
                           port=server.local_bind_port)
    data = pd.read_sql_query(Query, conn)
    
    server.close()
    return data

def price():
    Query = '''
    SELECT distinct
    f.product_id 'SKU',
    pp.sales_org_id as 'sales_org_id.1',
    IFNULL(pp.base_price, pp.price) 'Base Price',
    pp.original_price 'Org Cost',
    p.storage_type as storage_type,
    pp.price_code,
    p.ethnicity,
    p.catalogue_num,
    f.qty_sold_local as Qty_Sold_thisweekLocal,
    f.qty_sold_local_last as Qty_Sold_lastweekLocal,
    round((f.qty_sold_local + f.qty_sold_local_last)/14,0) as daily_sold_qty
    
    FROM gb_forecast f 
    INNER JOIN gb_product p ON f.product_id = p.id and f.selling_local = 'Y'
    INNER JOIN gb_inventory i on i.id = f.inventory_id
    LEFT JOIN gb_product_price pp ON f.product_id = pp.product_id and i.sales_org_id = pp.sales_org_id
    WHERE
    f.product_id in %(close_list)s AND f.start_day = %(sr_week)s
    and (p.ethnicity in ('chinese', 'mainstream','japanese','korean'))
    and left(p.catalogue_num, 2) not in ('01', '02')
    order by f.product_id, pp.sales_org_id
    
    ;'''
    server = sshtunnel.SSHTunnelForwarder(('fortress.db.sayweee.net', 22), 
                                      ssh_password='LuCqi1qHV3bk8WZz', 
                                      ssh_username='jiong',
     remote_bind_address=('weee-m1-r3.db.sayweee.net', 3306))

    server.start()

    # print(server.local_bind_port)  # show assigned local port
    # work with `SECRET SERVICE` through `server.local_bind_port`.
    conn = pymysql.connect(host='127.0.0.1', 
                           user='jiong', 
                           passwd='LuCqi1qHV3bk8WZz', 
                           db='weee_comm',
                           port=server.local_bind_port)
    data = pd.read_sql_query(Query, conn, params = Input)

    server.close()
    print('price done')
    return data 


def inv():
    Query = '''
    SELECT distinct
    inv.product_id 'SKU',
    inv.warehouse_number 'inventory_id',
    inv.available_qty 'sellable inventory',
    hist.avg_price 'Avg Cost'
    
    FROM weee_inv.inv_inventory inv
    LEFT JOIN gb_inventory_avg_price_history hist on 
    hist.product_id = inv.product_id and hist.inventory_id = inv.warehouse_number
    and hist.day = (select max(day) from gb_inventory_avg_price_history)
    where
    inv.product_id in %(close_list)s
    order by inv.product_id, warehouse_number
    ;'''
    server = sshtunnel.SSHTunnelForwarder(('fortress.db.sayweee.net', 22), 
                                      ssh_password='LuCqi1qHV3bk8WZz', 
                                      ssh_username='jiong',
     remote_bind_address=('weee-m1-r3.db.sayweee.net', 3306))

    server.start()

    # print(server.local_bind_port)  # show assigned local port
    # work with `SECRET SERVICE` through `server.local_bind_port`.
    conn = pymysql.connect(host='127.0.0.1', 
                           user='jiong', 
                           passwd='LuCqi1qHV3bk8WZz', 
                           db='weee_comm',
                           port=server.local_bind_port)
    data = pd.read_sql_query(Query, conn, params = Input)

    server.close()
    print('inv done')
    return data 

def sold():
    Query = '''
    select
    o.product_id as "SKU",
    o.sales_org_id as "sales_org_id.1",
    o.purchased_days_in_two,
    o1.purchased_days_in_four,
    o.past_two_days_sold_quantity,
    o1.past_four_days_sold_quantity
    FROM (
    select 
    product_id,
    sales_org_id,
    count(distinct order_day) as purchased_days_in_two,
    sum(quantity) as past_two_days_sold_quantity
    -- sold quantity is only yesterday's sale
    from metrics.order_product
    -- metric.order_product是有用户订单的数据表格，即是该商品至少被用户购买过一次
    where order_day > dateadd(day,-3,current_date)
    and order_day < current_date
    and payment_mode = 'F'
    and product_id in close_list
    group by 1, 2) o
    left join
    (select 
    product_id,
    sales_org_id,
    count(distinct order_day) as purchased_days_in_four,
    sum(quantity) as past_four_days_sold_quantity
    -- week sold quantity is only 3 days sold total, confusing concept here!
    from metrics.order_product
    where order_day > dateadd(day,-5,current_date)
    and order_day < current_date
    and payment_mode = 'F'
    and product_id in close_list
    group by 1, 2) o1 on o.product_id = o1.product_id and o.sales_org_id = o1.sales_org_id'''
    
    Query = Query.replace('close_list',str(Input['close_list']).replace('[','(').replace(']',')'))
    conn = psycopg2.connect(host="redshift.sayweee.net",
                            port = 5439,
                            database="dev",
                            user="jiong",
                            password='LuCqi1qHV3bk8WZz')
    cursor = conn.cursor()
    records = pd.read_sql_query(Query,conn)
    conn.close()
    cursor.close()
    print('sold done')
    return records

def dealp():
    Query = '''
    select 
    pp.product_id 'SKU',
    pp.sales_org_id 'sales_org_id.1',
    ps.discount_percentage
    
    from gb_product_price_special ps 
    LEFT join gb_product_price pp on pp.id = ps.product_price_id
    where ps.start_time > UNIX_TIMESTAMP(DATE_ADD(current_date(),interval -1 day))
    and pp.product_id in %(close_list)s
    group by product_id,pp.sales_org_id;'''
    server = sshtunnel.SSHTunnelForwarder(('fortress.db.sayweee.net', 22), 
                                          ssh_password='LuCqi1qHV3bk8WZz', 
                                          ssh_username='jiong',
         remote_bind_address=('weee-m1-r3.db.sayweee.net', 3306))

    server.start()

    # print(server.local_bind_port)  # show assigned local port
    # work with `SECRET SERVICE` through `server.local_bind_port`.
    conn = pymysql.connect(host='127.0.0.1', 
                           user='jiong', 
                           passwd='LuCqi1qHV3bk8WZz', 
                           db='weee_comm',
                           port=server.local_bind_port)
    data = pd.read_sql_query(Query, conn, params = Input)

    server.close()
    print('dealp done')
    return data 

def booking():
    Query = '''
    select product_id as "SKU",
    sales_org_id as "sales_org_id.1",
    sum(quantity) as booking
    FROM metrics.order_product
    where delivery_day >= dateadd(day,-1,current_date)
    and payment_mode = 'F'
    and product_id in close_list
    group by 1, 2'''
    
    Query = Query.replace('close_list',str(Input['close_list']).replace('[','(').replace(']',')'))
    conn = psycopg2.connect(host="redshift.sayweee.net",
                            port = 5439,
                            database="dev",
                            user="jiong",
                            password='LuCqi1qHV3bk8WZz')
    cursor = conn.cursor()
    records = pd.read_sql_query(Query,conn)
    conn.close()
    cursor.close()
    print('booking done')
    return records


# In[2]:


weekday = date.today().weekday()
offset = (weekday - 1) % 7
Tuesday = date.today() - datetime.timedelta(days=offset)

df_clear = Clear_Pool()
df_clear['SKU'] = [int(i) for i in df_clear['SKU'].to_list()]


df_clear['warehouse_number'] = [int(i) for i in df_clear['warehouse_number'].to_list()]
df_map=pd.read_csv('df_map1.csv')
df_map=df_map.iloc[0:31, 0:5]
skus = df_clear['SKU'].to_list()
skus = [x for x in skus if math.isnan(x) == False]
Input = {'sr_week': Tuesday, 'close_list': skus}
df_clearance = df_map.merge(df_clear,on = ['warehouse_number'], how='left',)

df_price = price()
df_inv = inv()
df_sold = sold()
df_deal_percentage = dealp()
df_booking = booking()


# In[3]:


df_sold['SKU'] = df_sold['sku']
df_sold = df_sold.drop(['sku'], axis = 1)

df_booking['SKU'] = df_booking['sku']
df_booking = df_booking.drop(['sku'], axis = 1)

#add storage_type for items
df_price['storage_type'] = [str(i) for i in df_price['storage_type']]
## end of code changed

df_inv['inventory_id'] = [int(i) for i in df_inv['inventory_id']]

df_clear_price = df_clearance.merge(df_price, on =['sales_org_id.1','SKU'], how = 'left')
#df_clear_price.to_csv('df_clear_price.csv')
df_clear_new2 = df_clear_price.merge(df_inv, on=['inventory_id','SKU'], how = 'left')
#df_clear_new2.to_csv('df_clear_new2.csv')

df_clear_new3 = df_clear_new2.merge(df_sold, how = 'left', on=['sales_org_id.1','SKU'])
#df_clear_new3.to_csv('df_clear_new3.csv')

df_clear_new4 = df_clear_new3.merge(df_deal_percentage, how = 'left', on=['sales_org_id.1','SKU'])
#df_clear_new4.to_csv('df_clear_new4.csv')

df_clear_new = df_clear_new4.merge(df_booking, how = 'left', on=['sales_org_id.1','SKU'])

df_clear_new = df_clear_new[(df_clear_new['sellable inventory']>0)].reset_index(drop=True)

df_clear_new['booking'] =  [0 if pd.isnull(i) else i for i in df_clear_new['booking']]

#df_clear_new['qty_to_clean'] = [min(df_clear_new[('Quantity')][i],df_clear_new[('sellable inventory')][i]) for i in range(len(df_clear_new))]
df_clear_new['qty_to_clean'] = df_clear_new['Quantity'] - df_clear_new['booking']
df_clear_new['Avg Cost'] = [max(df_clear_new['Org Cost'][i],df_clear_new['Avg Cost'][i]) for i in range(len(df_clear_new))]

df_clear_new['past_two_days_sold_quantity'] = [0 if pd.isnull(i) else i for i in df_clear_new['past_two_days_sold_quantity']]
df_clear_new['past_four_days_sold_quantity'] = [0 if pd.isnull(i) else i for i in df_clear_new['past_four_days_sold_quantity']]

df_clear_new['purchased_days_in_two'] = [i for i in df_clear_new['purchased_days_in_two']]
df_clear_new['purchased_days_in_four'] = [i for i in df_clear_new['purchased_days_in_four']]

df_clear_new = df_clear_new[(df_clear_new['Base Price']>0)].reset_index(drop=True)

df_clear_new['Gross Margin'] = round(1-df_clear_new['Avg Cost']/df_clear_new['Base Price'],2)*100

df_clear_new['suggested_discount'] = 0

df_clear_new = df_clear_new[(df_clear_new['qty_to_clean']>0)].reset_index(drop=True)

df_clear_new['catalogue_num'] = [int(i) for i in df_clear_new['catalogue_num']]

df_clear_new['suggested_discount'] = [0 if df_clear_new['qty_to_clean'][i] <= 0 else df_clear_new['suggested_discount'][i] 
                                      for i in range(len(df_clear_new))]
df_clear_new['sales_through_rate'] = 0



# In[4]:


#sales through rate
for i in range(len(df_clear_new)):
    if pd.isnull(df_clear_new['discount_percentage'][i]):
        df_clear_new['suggested_discount'][i] = 5*math.floor((df_clear_new['Gross Margin'][i]-(df_clear_new['Gross Margin'][i]*(df_clear_new['Remained Days to Ship'][i]/(df_clear_new['reminder_limit'][i]-df_clear_new['outbound_limit'][i]))))/(100-df_clear_new['Gross Margin'][i]*(df_clear_new['Remained Days to Ship'][i]/(df_clear_new['reminder_limit'][i]-df_clear_new['outbound_limit'][i])))*100/5)
    else:
        discount_interval = 1.25
        if df_clear_new['Remained Days to Ship'][i] <= 30:
            discount_interval = 2.5
    #inconsistent expiration filter value, this makes categories with short shelf life and short outbound limit day give aways unnecessary discount   
        if df_clear_new['Shelf life'][i] <= 21:
            discount_interval = 5
   #################################################         
        if df_clear_new['qty_to_clean'][i] != 0:
            if (df_clear_new['catalogue_num'][i] > 1399) or (df_clear_new['catalogue_num'][i] < 1300):
                if df_clear_new['Remained Days to Ship'][i] >= 30:
                    sales_through_rate = ((df_clear_new['past_four_days_sold_quantity'][i]/df_clear_new['purchased_days_in_four'][i])*df_clear_new['Remained Days to Ship'][i])/df_clear_new['qty_to_clean'][i]
                else:
                    sales_through_rate = ((df_clear_new['past_two_days_sold_quantity'][i]/df_clear_new['purchased_days_in_two'][i])*df_clear_new['Remained Days to Ship'][i])/df_clear_new['qty_to_clean'][i]
                    df_clear_new['sales_through_rate'][i] = sales_through_rate
                if sales_through_rate >= 0.95:
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i]
                elif sales_through_rate >= 2:
                    df_clear_new['suggested_discount'][i] = round((df_clear_new['discount_percentage'][i]*0.9)/5,0)*5
                elif (sales_through_rate < 0.95) and (sales_through_rate >= 0.85):
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i] + discount_interval
                elif (sales_through_rate < 0.85) and (sales_through_rate >= 0.75):
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i] + discount_interval*2
                elif (sales_through_rate < 0.75) and (sales_through_rate >= 0.65):
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i] + discount_interval*3
                elif sales_through_rate < 0.65:
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i] + discount_interval*4 
            elif (df_clear_new['catalogue_num'][i] >= 1300) and (df_clear_new['catalogue_num'][i] <= 1399):
                if df_clear_new['Remained Days to Ship'][i] >= 21:
                    sales_through_rate = ((df_clear_new['past_four_days_sold_quantity'][i]/df_clear_new['purchased_days_in_four'][i])*df_clear_new['Remained Days to Ship'][i] - 2)/df_clear_new['qty_to_clean'][i]
                else:
                    sales_through_rate = ((df_clear_new['past_two_days_sold_quantity'][i]/df_clear_new['purchased_days_in_two'][i])*df_clear_new['Remained Days to Ship'][i] - 2)/df_clear_new['qty_to_clean'][i]
                    df_clear_new['sales_through_rate'][i] = sales_through_rate
                if sales_through_rate >= 0.95:
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i]
                elif sales_through_rate >= 2:
                    df_clear_new['suggested_discount'][i] = round((df_clear_new['discount_percentage'][i]*0.9)/5,0)*5
                elif (sales_through_rate < 0.95) and (sales_through_rate >= 0.85):
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i] + discount_interval
                elif (sales_through_rate < 0.85) and (sales_through_rate >= 0.75):
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i] + discount_interval*2
                elif (sales_through_rate < 0.75) and (sales_through_rate >= 0.65):
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i] + discount_interval*3
                elif sales_through_rate < 0.65:
                    df_clear_new['suggested_discount'][i] = df_clear_new['discount_percentage'][i] + discount_interval*4 
        else:
            df_clear_new['suggested_discount'][i] = 0      
# In[5]:


#drop rows with suggested discount = 0
df_clear_new = df_clear_new[df_clear_new.suggested_discount > 0]

#discount rate upper limit
df_clear_new['suggested_discount'] = [75 if i > 75 else i for i in df_clear_new['suggested_discount']]

#round discount rate to multiple of 5
df_clear_new['suggested_discount'] = round(df_clear_new['suggested_discount']/5)*5

df_clear_new['Special_Price'] = (round(df_clear_new['Base Price'] * (100-df_clear_new['suggested_discount'])/100,2))-0.01

df_clear_new['New_Margin'] = round(1 - df_clear_new['Avg Cost']/df_clear_new['Special_Price'],2)*100
df_clear_new['DOI'] = round(df_clear_new['qty_to_clean']/df_clear_new['daily_sold_qty'],0)
df_clear_new['date'] = date.today().strftime("%Y-%m-%d")
df_clear_new = df_clear_new.drop(['DOI','discount_percentage'], axis = 1)
df_clear_new['promo_price'] = round((df_clear_new['Base Price'] * (100 - df_clear_new['suggested_discount'])/100),2)

df_clear_new['price_priority'] = 'N'
df_clear_new['special_price'] = 0
df_clear_new['LD_restriction_price'] = 0


# In[6]:


#long term exemption
df_clear_new = df_clear_new[df_clear_new['SKU'] != 112]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 85851]

#Mooncake exemption
df_clear_new = df_clear_new[df_clear_new['SKU'] != 26568]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 26569]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 26567]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 24971]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 25902]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 93807]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 22730]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 22068]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21484]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 26566]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 62751]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 24099]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21485]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 92634]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2007]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 96997]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 25602]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 92561]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21706]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 22002]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21708]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21711]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21712]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21988]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2010]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 56835]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 92483]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 24153]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 8950]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 31878]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 14398]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2419]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2397]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2415]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2416]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2366]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 93101]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 93732]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 93100]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 23114]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 23115]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 23117]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 16918]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21963]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2413]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 24943]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 24942]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2433]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2435]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2432]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 1624]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 15085]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 100020]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 92947]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 92950]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 92948]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 64634]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 100019]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 100018]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 19128]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 12847]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 20307]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 24788]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 64663]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21725]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 14207]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 14208]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 8819]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 8820]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21195]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21197]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 23988]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 16320]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 92462]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 21723]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 23989]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 23991]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 23992]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2141]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 90471]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 93796]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 93809]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 93808]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 22038]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2206]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2111]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 92677]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 27494]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 92676]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 22086]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 22085]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 22084]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 22083]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2129]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 26677]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2102]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 2103]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 23314]
df_clear_new = df_clear_new[df_clear_new['SKU'] != 23313]

# In[7]:


#Household restriction

#df_clear_new = df_clear_new[df_clear_new['catalogue_num'] != 1704]
df_clear_new = df_clear_new.drop(df_clear_new[(df_clear_new['catalogue_num'] == 17) & (df_clear_new['price_code'] == 'Turf')].index)


# In[8]:


#milk egg restriction
#LD inv depletion price
df_clear_new['LD_restriction_price'] = [3.39 if i == 11403 else 0 for i in df_clear_new['SKU']]
df_clear_new['LD_restriction_price'] = [4.49 if i == 13970 else 0 for i in df_clear_new['SKU']]
df_clear_new['LD_restriction_price'] = [5.99 if i == 36213 else 0 for i in df_clear_new['SKU']]
df_clear_new['LD_restriction_price'] = [5.19 if i == 65914 else 0 for i in df_clear_new['SKU']]
df_clear_new['LD_restriction_price'] = [6.39 if i == 57295 else 0 for i in df_clear_new['SKU']]
df_clear_new['LD_restriction_price'] = [7.49 if i == 65902 else 0 for i in df_clear_new['SKU']]
df_clear_new['LD_restriction_price'] = [6.79 if i == 57297 else 0 for i in df_clear_new['SKU']]

df_clear_new['class'] = np.where((df_clear_new['promo_price'] - df_clear_new['LD_restriction_price'])>=0,'algo','change')
df_clear_new['price_priority'] = ['Y' if i == 'change' else 'N' for i in df_clear_new['class']]

df_clear_new

df_clear_new.to_csv('df_clear.csv')

# # Pasted to Review

# In[9]:


scopes = ['https://www.googleapis.com/auth/spreadsheets']
credentials = ServiceAccountCredentials.from_json_keyfile_name("service_credentials.json", scopes) 
file = gspread.authorize(credentials)
sheet = file.open_by_key('1P4IKDhQZxus-e_Sro9xLJ5oxZM7QYFM_UhkPyVKamzc') 

worksheet = sheet.get_worksheet(12)
set_with_dataframe(worksheet, df_clear_new)


# # Upload SF,LA,SEA,Vegas

# In[10]:


df_clear_new['product_id'] = df_clear_new['SKU']
df_clear_new['discount_percentage']=df_clear_new['suggested_discount']
df_clear_new['special_price'] = df_clear_new['LD_restriction_price']
df_clear_new['max_order_quantity'] = 5


#中午秒杀_special price 1,2,4
df_clear_new['start_time'] = [date.today().strftime("%Y-%m-%d 12:00:00") if i in (1,2,4)
                              else date.today().strftime("%Y-%m-%d 20:00:00") if i == 10 or i == 25
                              else date.today().strftime("%Y-%m-%d 19:00:00") if i == 15 and date.today().weekday() in (0,2,3,5,6)
                              else date.today().strftime(" ") if i == 15 and date.today().weekday() in (1,4)
                            
                              else date.today().strftime("%Y-%m-%d 19:00:00") if i == 17 and date.today().weekday() in (0,2,6)
                              else date.today().strftime(" ") if i == 17 and date.today().weekday() in (1,3,4,5)
                                                                                     
                              else date.today().strftime("%Y-%m-%d 19:00:00") if i == 23 and date.today().weekday() in (0,2,3)
                              else date.today().strftime(" ") if i == 23 and date.today().weekday() in (1,4,5,6)
                                                           
                              else date.today().strftime("%Y-%m-%d 19:00:00") for i in df_clear_new['sales_org_id.1']]

df_clear_new['end_time'] =[date.today().strftime("%Y-%m-%d 12:59:00") if i in (1,2,4)
                              else date.today().strftime("%Y-%m-%d 23:59:00") for i in df_clear_new['sales_org_id.1']]

df_clear_new['is_lighting_deal'] = 'Y'
df_clear_new['price_priority'] = df_clear_new['price_priority']
df_clear_new['reason'] = 'Clearance'
df_clear_new['limit_quantity'] = round((df_clear_new['qty_to_clean'] * 0.3),0) + 1

#中午秒杀_lightning deal 1,2,4
df_clear_new['date'] = date.today().strftime("%Y-%m-%d")
df_clear_new['quantity'] = 10
df_clear_new['view_pos'] = 99
df_clear_new['quantity_upper_limit'] = round((df_clear_new['qty_to_clean'] * 0.3),0) + 1
df_clear_new['reason'] = 'Clearance'
df_clear_new['storage_type'] = df_clear_new['storage_type']

df_special_upload = df_clear_new[['sales_org_id.1','product_id','discount_percentage','special_price',
                               'max_order_quantity','start_time','end_time','is_lighting_deal','price_priority','reason','limit_quantity', 'storage_type']]

#evening flash sales, 1,2,4,25
df_special_upload_1 = df_special_upload[df_special_upload['sales_org_id.1'].isin([1,2,4,25])].copy()
df_special_upload_1['start_time'] = [date.today().strftime("%Y-%m-%d 19:00:00") if i == 1
                                     else date.today().strftime("%Y-%m-%d 20:00:00") for i in df_special_upload_1['sales_org_id.1']]
df_special_upload_1['end_time'] = date.today().strftime("%Y-%m-%d 23:59:00")  
df_special_upload_1['limit_quantity'] = round((df_clear_new['qty_to_clean'] * 0.4),0)
df_special_upload_1['quantity_upper_limit'] = round((df_clear_new['qty_to_clean'] * 0.4),0) + 1

#afternoon special price,1,2,4,25
df_special_upload_2 = df_special_upload[df_special_upload['sales_org_id.1'].isin([1,2,4,25])].copy()
df_special_upload_2['start_time'] = [date.today().strftime("%Y-%m-%d 13:00:00") if i == 1
                                     else date.today().strftime("%Y-%m-%d 12:00:00") if i == 25
                                     else date.today().strftime("%Y-%m-%d 13:00:00") for i in df_special_upload_2['sales_org_id.1']]
df_special_upload_2['end_time'] = [date.today().strftime("%Y-%m-%d 18:59:00") if i == 1
                                     else date.today().strftime("%Y-%m-%d 19:59:00") for i in df_special_upload_2['sales_org_id.1']]
df_special_upload_2['is_lighting_deal'] = 'N'
df_special_upload_2['limit_quantity'] = round((df_clear_new['qty_to_clean'] * 0.2),0) + 1


#第二天凌晨特价,1,2,4,25
tomorrow = date.today()+datetime.timedelta(days=1)

df_special_upload_3 = df_special_upload[df_special_upload['sales_org_id.1'].isin([1,2,4,25])].copy()
df_special_upload_3['start_time'] = date.today().strftime("%Y-%m-%d 23:59:59")
df_special_upload_3['end_time'] = tomorrow.strftime("%Y-%m-%d 10:59:59")
df_special_upload_3['is_lighting_deal'] = 'N'
df_special_upload_3['limit_quantity'] = round((df_clear_new['qty_to_clean'] * 0.1),0) + 1


#HOU,NY,CHI,Tampa
##第二天凌晨特价 10,7,15
df_special_upload_4 = df_special_upload[df_special_upload['sales_org_id.1'].isin([7,10,15,17,23])].copy()
df_special_upload_4['start_time'] = date.today().strftime("%Y-%m-%d 23:59:59")
df_special_upload_4['end_time'] = [tomorrow.strftime("%Y-%m-%d 10:59:00") if i in (10,15)
                                     else tomorrow.strftime("%Y-%m-%d 13:29:00") for i in df_special_upload_4['sales_org_id.1']]

df_special_upload_4['is_lighting_deal'] = 'N'
df_special_upload_4['limit_quantity'] = round((df_clear_new['qty_to_clean'] * 0.3),0) + 1

#afternoon special price,10,7,15
df_special_upload_5 = df_special_upload[df_special_upload['sales_org_id.1'].isin([7,10,15,17,23])].copy()
df_special_upload_5['start_time'] = date.today().strftime("%Y-%m-%d 14:30:00")
df_special_upload_5['end_time'] = [date.today().strftime("%Y-%m-%d 19:59:00") if i == 10
                                     else date.today().strftime("%Y-%m-%d 18:59:00") for i in df_special_upload_5['sales_org_id.1']]
df_special_upload_5['is_lighting_deal'] = 'N'
df_special_upload_5['limit_quantity'] = round((df_clear_new['qty_to_clean'] * 0.3),0) + 1

#合并所有的时段
df_special_upload_final = pd.concat([df_special_upload,df_special_upload_1,df_special_upload_2,df_special_upload_3,df_special_upload_4,df_special_upload_5])
df_special_upload_final = df_special_upload_final[['sales_org_id.1','product_id','discount_percentage','special_price',
                               'max_order_quantity','start_time','end_time','is_lighting_deal','price_priority','reason','limit_quantity','storage_type']]

df_special_upload_final = df_special_upload_final[df_special_upload_final['start_time'] != " "]


# ## BA

# In[11]:

currentdir = os.getcwd() + '/upload'
if not (os.path.exists(currentdir)):
    os.makedirs(currentdir)
os.chdir(currentdir)

df_special_upload_final.rename(columns={'sales_org_id.1':'sales_org_id'}, inplace=True)

df_special_upload_final[df_special_upload_final['sales_org_id'] == 1].to_csv('特价上传BA.csv', header = True, index = False)
df_ld_upload = df_clear_new[df_clear_new['sales_org_id.1']==1][['product_id','date','quantity','quantity_upper_limit','view_pos']]
df_ld_upload.to_csv('秒杀上传BA.csv', header = True, index = False)



# ## LA
# 

# In[12]:


df_special_upload_final.rename(columns={'sales_org_id.1':'sales_org_id'}, inplace=True)

df_special_upload_final[df_special_upload_final['sales_org_id'] == 2].to_csv('特价上传LA.csv', header = True, index = False)
df_ld_upload = df_clear_new[df_clear_new['sales_org_id.1']==2][['product_id','date','quantity','quantity_upper_limit','view_pos']]
df_ld_upload.to_csv('秒杀上传LA.csv', header = True, index = False)



# ## SEA

# In[14]:


df_special_upload_final.rename(columns={'sales_org_id.1':'sales_org_id'}, inplace=True)

df_special_upload_final[df_special_upload_final['sales_org_id'] == 4].to_csv('特价上传WA.csv', header = True, index = False)
df_ld_upload = df_clear_new[df_clear_new['sales_org_id.1']==4][['product_id','date','quantity','quantity_upper_limit','view_pos']]
df_ld_upload.to_csv('秒杀上传WA.csv', header = True, index = False)


# ## HOU

# In[15]:


df_special_upload_final.rename(columns={'sales_org_id.1':'sales_org_id'}, inplace=True)

df_special_upload_final[df_special_upload_final['sales_org_id'] == 10].to_csv('特价上传TX.csv', header = True, index = False)
df_ld_upload = df_clear_new[df_clear_new['sales_org_id.1']==10][['product_id','date','quantity','quantity_upper_limit','view_pos']]
df_ld_upload.to_csv('秒杀上传TX.csv', header = True, index = False)


# ## NY

# In[16]:


df_special_upload_final.rename(columns={'sales_org_id.1':'sales_org_id'}, inplace=True)

df_special_upload_final[df_special_upload_final['sales_org_id'] == 7].to_csv('特价上传NY.csv', header = True, index = False)
df_ld_upload = df_clear_new[df_clear_new['sales_org_id.1']==7][['product_id','date','quantity','quantity_upper_limit','view_pos']]
df_ld_upload.to_csv('秒杀上传NY.csv', header = True, index = False)


# ## ATL

# In[17]:

# ## Don't need Atlanta for now

#Mon,Wed,Sun
#df_special_upload_final.rename(columns={'sales_org_id.1':'sales_org_id'}, inplace=True)

#df_special_upload_final[df_special_upload_final['sales_org_id'] == 17].to_csv('特价上传ATL.csv', header = True, index = False)
#df_ld_upload = df_clear_new[df_clear_new['sales_org_id.1']==17][['product_id','date','quantity','quantity_upper_limit','view_pos']]
#df_ld_upload.to_csv('秒杀上传ATL.csv', header = True, index = False)


# ## CHI

# In[18]:


#Mon,Tue,Wed,Thur,Sat
df_special_upload_final.rename(columns={'sales_org_id.1':'sales_org_id'}, inplace=True)

df_special_upload_final[df_special_upload_final['sales_org_id'] == 15].to_csv('特价上传IL.csv', header = True, index = False)
df_ld_upload = df_clear_new[df_clear_new['sales_org_id.1']==15][['product_id','date','quantity','quantity_upper_limit','view_pos']]
df_ld_upload.to_csv('秒杀上传IL.csv', header = True, index = False)


# ## Tampa

# In[19]:


#Mon,Thurs
df_special_upload_final.rename(columns={'sales_org_id.1':'sales_org_id'}, inplace=True)

df_special_upload_final[df_special_upload_final['sales_org_id'] == 23].to_csv('特价上传FL.csv', header = True, index = False)
df_ld_upload = df_clear_new[df_clear_new['sales_org_id.1']==23][['product_id','date','quantity','quantity_upper_limit','view_pos']]
df_ld_upload.to_csv('秒杀上传FL.csv', header = True, index = False)


# ## Boston

# In[20]:


df_special_upload_final.rename(columns={'sales_org_id.1':'sales_org_id'}, inplace=True)

df_special_upload_final = df_special_upload_final[df_special_upload_final['sales_org_id']  == 7]
df_special_upload_final['sales_org_id'] = 12

df_special_upload_final.to_csv('特价上传MA.csv', header = True, index = False)
df_ld_upload = df_clear_new[df_clear_new['sales_org_id.1']==7][['product_id','date','quantity','quantity_upper_limit','view_pos']]
df_ld_upload.to_csv('秒杀上传MA.csv', header = True, index = False)




# In[ ]:




