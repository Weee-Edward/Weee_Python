#!/usr/bin/env python
# coding: utf-8

# In[11]:


import pandas as pd
import math
import pymysql
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import mysql.connector
from datetime import date
import datetime
from gspread_dataframe import set_with_dataframe
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import gspread_dataframe as gd
import psycopg2

def sku():
    Query = '''
    SELECT
        inv.product_id 'SKU',
        i.sales_org_id,
        inv.warehouse_number,
        concat(inv.warehouse_number,' - ',i.title) 'warehouse',
        p.short_title 'Name',
        p.short_title_en,
        p.catalogue_num,
        pp.price_code,
        p.ethnicity,
        concat(p.weee_buyer_id,' - ',wb.name) 'owner',
        concat(left(p.catalogue_num,2),'-',c.label_en) 'Department',
        concat(left(p.catalogue_num,4),'-',c1.label_en) 'Category',
        ifnull(concat(left(p.sub_catalogue_num,6),'-',c2.label_en),'') 'Sub_Category',
        p.storage_type,
        p.status,
        p.tier,
        pp.price,
        IFNULL(pp.base_price,pp.price) 'Base Price',
        pp.original_price 'cost',
        ps.avg_price 'average cost',
        inv.available_qty
    FROM
        weee_inv.inv_inventory inv
            LEFT JOIN 
        weee_comm.gb_product p on inv.product_id = p.id
            LEFT JOIN 
        weee_comm.gb_catalogue c on left(p.catalogue_num,2) = c.num
            LEFT JOIN
        weee_comm.gb_catalogue c1 on p.catalogue_num = c1.num
            LEFT JOIN 
        weee_comm.gb_catalogue c2 on p.sub_catalogue_num = c2.num
            LEFT JOIN
        weee_comm.gb_inventory i on inv.warehouse_number = i.id
            LEFT JOIN 
        weee_comm.gb_product_sales ps on inv.product_id = ps.product_id and ps.sales_org_id = i.sales_org_id
            LEFT JOIN
        weee_comm.gb_product_price pp on inv.product_id = pp.product_id and pp.sales_org_id = ps.sales_org_id
            LEFT JOIN
        weee_comm.gb_inventory_qty iq on inv.product_id = iq.product_id and inv.warehouse_number = iq.inventory_id 
            LEFT JOIN
        weee_comm.gb_weee_buyer wb on wb.user_id = p.weee_buyer_id
    WHERE
        #inv.available_qty != 0
        left(p.catalogue_num,2) in ('13')
        and p.storage_type = 'R'
        and (p.ethnicity in ('chinese', 'mainstream','japanese','korean'))
        #and inv.product_id = 13137
    ;
    '''
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
    data = pd.read_sql_query(Query, conn)

    server.close()
    print('sku info done')
    return data 

def inv_map():
    Query = '''
    SELECT
        id 'warehouse_number',
        sales_org_id,
        concat(id,' - ',title) 'physical_warehouse'
    FROM
        weee_comm.gb_inventory
    WHERE
        status = 'A'
    ;
    '''
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
    data = pd.read_sql_query(Query, conn)

    server.close()
    print('inv map done')
    return data 

def wms():
    Query = '''
    SELECT
        sl.warehouse_number,
        t.item_number AS `SKU`,
        sum(trans.quantity + trans.allocated_qty) AS `Quantity`,
        ie.outbound_shelf_life_limit,
        ie.receiving_shelf_life_limit,
        item.shelf_life AS `Shelf life`,
        #ie.reminder_shelf_life_limit - ie.outbound_shelf_life_limit 'warning_period',
        DATE(CONVERT_TZ(FROM_UNIXTIME( t.expire_dtm ), '+00:00', wh.time_zone)) AS `Expiration Date`,
        Floor((t.expire_dtm - ie.outbound_shelf_life_limit * 24 * 3600 - UNIX_TIMESTAMP())/24/3600) AS `Days Left`,
        DATE(CONVERT_TZ(FROM_UNIXTIME(t.expire_dtm - ie.outbound_shelf_life_limit * 24 * 3600-24*3600), '+00:00', wh.time_zone)) as 'Outbound Limit Date',
        ii.in_dtm

    FROM
        wh_storage_location_item t
            JOIN 
        wh_storage_location sl ON t.storage_location_id = sl.rec_id 
            AND 
        sl.location_type IN ( 3, 4 )
            JOIN 
        wh_item item ON t.item_number = item.item_number 
            AND 
        item.check_shelf_life = 1 
            AND 
        item.storage_type IN (2)
            JOIN 
        wh_item_extend ie ON ie.item_number = item.item_number
            JOIN 
        wh_warehouse wh ON t.warehouse_number = wh.warehouse_number
            JOIN 
        wh_inventory_transaction trans ON t.item_number = trans.item_number 
            AND 
        t.location_no = trans.location_no 
        AND t.warehouse_number = trans.warehouse_number 
            LEFT JOIN (
        SELECT 
            ii.item_number, 
            ii.quantity, 
            date(from_unixtime(ii.in_dtm)) 'in_dtm',
            i.warehouse_number
        FROM 
            wms.wh_inbound_item ii
                left join 
            wh_inbound i on i.inbound_number = ii.inbound_number
        where 
            ii.rec_id in (
            select distinct 
                max(ii.rec_id) 
            from 
                wms.wh_inbound_item ii 
                    left join 
                wh_inbound i on i.inbound_number = ii.inbound_number
            group by 
                ii.item_number, i.warehouse_number)
            order by 
                ii.in_dtm desc
        ) ii on t.item_number = ii.item_number 
            and 
        sl.warehouse_number = ii.warehouse_number
   WHERE
        -- sl.warehouse_number = %(inv_id)s
        t.expire_dtm > UNIX_TIMESTAMP()
        and t.expire_dtm - ie.outbound_shelf_life_limit * 24 * 3600 > UNIX_TIMESTAMP()
        and ii.in_dtm < date_add(current_date, interval -4 day)
        and item.category_id between 1300 and 1399
    group by 
        t.item_number,sl.warehouse_number,t.expire_dtm
    order by 
        sl.warehouse_number asc,
        t.item_number asc,
        Floor((t.expire_dtm - ie.outbound_shelf_life_limit * 24 * 3600 - UNIX_TIMESTAMP())/24/3600) asc;
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
    print('wms done')
    server.close()
    return data

def order():
    Query = '''
        select
            product_id as "SKU",
            sales_org_id,
            order_day as "Date",
            sum(quantity) as "sales_qty"
            
        from
            metrics.order_product
        where
            order_day > dateadd(day,-4,current_date)
            and order_day != current_date
            and payment_mode = 'F'
            and storage_type = 'R'
            and parent_num in ('13')
        group by 1,2,3'''
    
    conn = psycopg2.connect(host="redshift.sayweee.net",
                            port = 5439,
                            database="dev",
                            user="jiong",
                            password="LuCqi1qHV3bk8WZz")
    cursor = conn.cursor()
    records = pd.read_sql_query(Query,conn)
    conn.close()
    cursor.close()
    print('order done')
    return records

def in_stock_data():
    Query = '''
    select
        plh.product_id as "SKU",
        plh.sales_org_id,
        -- plh.rec_create_time,
        DATE_ADD((DATE(CONVERT_TZ(plh.rec_create_time, '+00:00', so.time_zone))), interval 1 DAY) AS "Date",
        plh.available,
        -- plh.day,
        so.time_zone

    from weee_job.product_list_history plh
    join weee_comm.gb_sales_org so on plh.sales_org_id = so.id
    and plh.day > '2022-05-15'
    '''
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
    data = pd.read_sql_query(Query, conn)

    server.close()
    print('inv map done')
    return data 

def df_yest_booking():
    Query = '''
        select
            product_id as "SKU",
            sales_org_id,
            order_day as "Date",
            sum(quantity) as "booking"
            
        from
            metrics.order_product
        where
            order_day >= dateadd(day,-1,current_date)
            and payment_mode = 'F'
            and storage_type = 'R'
            and parent_num in ('13')
        group by 1,2,3'''
    
    conn = psycopg2.connect(host="redshift.sayweee.net",
                            port = 5439,
                            database="dev",
                            user="jiong",
                            password="LuCqi1qHV3bk8WZz")
    cursor = conn.cursor()
    records = pd.read_sql_query(Query,conn)
    conn.close()
    cursor.close()
    print('booking done')
    return records


# In[12]:


df_sku = sku()
df_map = inv_map()
df_wms = wms()
df_order = order()
df_in_stock= in_stock_data()

df_yest_booking = df_yest_booking()
df_yest_booking
df_yest_booking.to_csv('df_yest_booking.csv')


# In[13]:


df_order['SKU'] = df_order['sku']
df_yest_booking['SKU'] = df_yest_booking['sku']
df_order['Date'] = df_order['date']
df_yest_booking['Date'] = df_yest_booking['date']
df_wms['warehouse_number'] = [int(i) for i in df_wms['warehouse_number']]

df_wms['SKU'] = [int(i) for i in df_wms['SKU']]
df_sku['SKU'] = [int(i) for i in df_sku['SKU']]
df_in_stock['SKU'] = [int(i) for i in df_in_stock['SKU']]
df_in_stock['available'] = [int(i) for i in df_in_stock['available']]


# In[14]:


# merges
df_order_instock = df_order.merge(df_in_stock, on=['SKU','sales_org_id','Date'], how = 'left')
df_order_instock

df_order_instock.to_csv('df_order_instock.csv')

df_wms = df_map.merge(df_wms, on=['warehouse_number'], how = 'right')
df_wms = df_wms.drop(columns = ['warehouse_number'])
df_clear_wms = df_sku.merge(df_wms, on=['sales_org_id','SKU'], how = 'right')

df_clear  = df_clear_wms.merge(df_order_instock, on=['sales_org_id','SKU'], how = 'right')

df_clear = df_clear[(df_clear['available_qty']>0)].reset_index(drop=True)

df_clear


# In[15]:


#drop data that in_stock_rate = 0
df_clear = df_clear.drop(df_clear[(df_clear['available'] == 0)].index)


# In[16]:


import pandas as pd
df_avg_daily_sales = df_clear.groupby(['SKU','sales_org_id']).agg({'sales_qty': ['mean']})
df_avg_daily_sales.columns = ['avg_daily_sales']
df_avg_daily_sales = df_avg_daily_sales.reset_index()
df_avg_daily_sales.to_csv('df_avg_daily_sales.csv')


# In[17]:


df_clear_wms_avg_daily_sales = df_clear_wms.merge(df_avg_daily_sales, on=['sales_org_id','SKU'], how = 'left')
df_clear_wms_avg_daily_sales

df_clear = df_clear_wms_avg_daily_sales.merge(df_yest_booking, on=['sales_org_id','SKU'], how = 'left')
df_clear['booking'] =  [0 if pd.isnull(i) else i for i in df_clear['booking']]
df_clear


# In[18]:


# create running total
df_clear['Running Total'] = 0

for i in range(len(df_clear['SKU'])):
    if i != 0:
        if (df_clear['SKU'][i] == df_clear['SKU'][i-1]) & (df_clear['sales_org_id'][i] == df_clear['sales_org_id'][i-1]):
            df_clear['Running Total'][i] = df_clear['Running Total'][i-1] + df_clear['Quantity'][i] - df_clear['booking'][i]
        else:
            df_clear['Running Total'][i] = df_clear['Quantity'][i] - df_clear['booking'][i]
    else:
        df_clear['Running Total'][i] = df_clear['Quantity'][i] - df_clear['booking'][i]


# In[19]:


# calculate doi
df_clear['three_day_run_rate'] = df_clear['avg_daily_sales']

df_clear['DOI'] = 0
for i in range(len(df_clear['DOI'])):
    if pd.isnull(df_clear['three_day_run_rate'][i]):
        df_clear['DOI'][i] = None
    else:
        if df_clear['sales_org_id'][i] == 4:
            df_clear['DOI'][i] = df_clear['available_qty'][i]/df_clear['three_day_run_rate'][i]
        else:
            df_clear['DOI'][i] = df_clear['Quantity'][i]/df_clear['three_day_run_rate'][i]


# In[20]:


# calculate amt to clean
df_clear['Alert'] = 'N'
df_clear['Amt to clear'] = 0
for i in range(len(df_clear['Alert'])):
    if float(df_clear['DOI'][i]) > df_clear['Days Left'][i] and df_clear['DOI'][i] != None:
        df_clear['Alert'][i] = 'Y'
        df_clear['Amt to clear'][i] = math.ceil((df_clear['DOI'][i] - df_clear['Days Left'][i])*df_clear['three_day_run_rate'][i])
    if df_clear['three_day_run_rate'][i] == 0 or pd.isnull(df_clear['three_day_run_rate'][i]) or df_clear['DOI'][i] == math.inf:
        df_clear['Alert'][i] = 'N/A'
        df_clear['Amt to clear'][i] = df_clear['Running Total'][i]
        
        


# In[21]:


df_clear['avg cost'] = [max(df_clear['cost'][i],df_clear['average cost'][i]) for i in range(len(df_clear))]

df_clear['Gross Margin'] = round(1-df_clear['avg cost']/df_clear['Base Price'],2)*100

df_clear['suggested_discount'] = 0
for i in range(len(df_clear)):
    if df_clear['Days Left'][i] < df_clear['Shelf life'][i] and df_clear['Alert'][i] == 'Y':
        df_clear['suggested_discount'][i] = 5*round(((df_clear['Gross Margin'][i]-(df_clear['Gross Margin'][i]*(df_clear['Days Left'][i]/(df_clear['Shelf life'][i]-df_clear['outbound_shelf_life_limit'][i]))))/(100-df_clear['Gross Margin'][i]*(df_clear['Days Left'][i]/(df_clear['Shelf life'][i]-df_clear['outbound_shelf_life_limit'][i]))))*100*((df_clear['DOI'][i]-df_clear['Days Left'][i])/df_clear['DOI'][i])/5)
    if df_clear['Alert'][i] == 'N/A':
        if df_clear['Gross Margin'][i] >= 38:
            df_clear['suggested_discount'][i] = 15
        elif df_clear['Gross Margin'][i] >= 32:
            df_clear['suggested_discount'][i] = 10
        else:
            df_clear['suggested_discount'][i] = 5
            


# In[22]:


df_clear = df_clear[df_clear.suggested_discount > 0]

df_clear['promo_price'] = round((df_clear['Base Price'] * (100 - df_clear['suggested_discount'])/100),2)

df_clear['price_priority'] = 'N'
df_clear['special_price'] = 0
df_clear['LD_restriction_price'] = 0


# In[23]:


##long term exemption


#Temporary deletion, kriz
df_clear = df_clear[df_clear['SKU'] != 36213]


# In[24]:


#Household restriction

#df_clear_new = df_clear_new[df_clear_new['catalogue_num'] != 1704]
df_clear = df_clear.drop(df_clear[(df_clear['catalogue_num'] == 17) & (df_clear['price_code'] == 'Turf')].index)


# In[25]:


#milk egg restriction
#LD inv depletion price

#df_clear.loc[df_clear["SKU"] == 11403, "LD_restriction_price"] = 3.39 + 0.5
#df_clear.loc[df_clear["SKU"] == 13970, "LD_restriction_price"] = 4.49 + 0.5
#df_clear.loc[df_clear["SKU"] == 36213, "LD_restriction_price"] = 5.99 + 0.5
#df_clear.loc[df_clear["SKU"] == 65914, "LD_restriction_price"] = 5.19 + 0.5
#df_clear.loc[df_clear["SKU"] == 57295, "LD_restriction_price"] = 6.39 + 0.5
#df_clear.loc[df_clear["SKU"] == 65902, "LD_restriction_price"] = 7.49 + 0.5
#df_clear.loc[df_clear["SKU"] == 57297, "LD_restriction_price"] = 6.79 + 0.5
#df_clear['class'] = np.where((df_clear['promo_price'] - df_clear['LD_restriction_price'])>0,'algo','change')
#df_clear['price_priority'] = ['Y' if i == 'change' else 'N' for i in df_clear['class']]

#df_clear.to_csv('0602 data.csv')


# In[26]:


df_export = df_clear[['Alert','Amt to clear','suggested_discount','DOI','Days Left','Outbound Limit Date',
                      'Running Total','booking','sales_org_id','warehouse','physical_warehouse','SKU','Name',
                      'short_title_en','Department','Category','Sub_Category','status','tier','Quantity','three_day_run_rate',
                      'Shelf life','receiving_shelf_life_limit','outbound_shelf_life_limit','in_dtm','owner','cost','Base Price',
                      'average cost','Gross Margin','available_qty']]

df_export.to_csv('export11.csv',index = False)

df_export = df_export[df_export['Alert'] == 'Y']
df_export1 = df_export[['Alert','suggested_discount','DOI','Days Left','Outbound Limit Date','Amt to clear',
                        'sales_org_id','SKU','Name','short_title_en','Department','Quantity','three_day_run_rate',
                       'Shelf life','receiving_shelf_life_limit','outbound_shelf_life_limit','in_dtm']]


# # Upload Files

# In[31]:


df_export1['sales_org_id'] = df_export1['sales_org_id']
df_export1['product_id'] = df_export1['SKU']
df_export1['discount_percentage']=df_export1['suggested_discount']
df_export1['special_price'] = 1
df_export1['special_price'] = [i-0.1 if round(i-math.floor(i),2) == 0.09 else i for i in df_export1['special_price']]
df_export1['max_order_quantity'] = 5

df_export1['start_time'] = date.today().strftime("2022-06-04 00:01:00")
df_export1['end_time'] = date.today().strftime("2022-06-06 23:59:00")

df_export1['is_lighting_deal'] = 'Y'
df_export1['price_priority'] = 'N'
df_export1['reason'] = 'Expiring Control'
df_export1['limit_quantity'] = round((df_export1['Amt to clear']),0)

df_special_upload = df_export1[['sales_org_id','product_id','discount_percentage','special_price',
                               'max_order_quantity','start_time','end_time','is_lighting_deal',
                                'price_priority','reason','limit_quantity']]

df_special_upload.to_csv('Speical_price_upload.csv',header = True, index = False)


# # Pasted to Review

# In[142]:


scopes = ['https://www.googleapis.com/auth/spreadsheets']
credentials = ServiceAccountCredentials.from_json_keyfile_name("service_credentials.json", scopes) 
file = gspread.authorize(credentials)
sheet = file.open_by_key('1P4IKDhQZxus-e_Sro9xLJ5oxZM7QYFM_UhkPyVKamzc') 


worksheet = sheet.get_worksheet(11)
set_with_dataframe(worksheet, df_export1)



