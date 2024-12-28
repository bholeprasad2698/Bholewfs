
#==========================================
# LM Metrics - D0 dump
#==========================================
import sys
sys.path.append(r'D:\vs wrkspce')   
import datetime
import os
from datetime import date
from datetime import datetime, timedelta
import gspread
import gspread_dataframe
import pandas as pd
import redshift_connector
import sql_explorer as exp
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import datetime as dt
from datetime import datetime, timedelta
# import duckdb
import numpy as np
import json
import requests
import pytz
from pytz import timezone

# import dtale
credentials = {
  "type": "service_account",
  "project_id": "vocal-honor-377310",
  "private_key_id": "417961e1b4a8021364d4f4e724f3fc6490bb5523",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCaTrPrKXETp/0H\n9HOkF3vOx8h91YCK1PP4TjQUd6VegUR1F9Gt23DwQXUIvXd+2QZBMKi/9j7e+5j2\ncwyEma72kWIrUdDXBdEEh7l6R1qfw8XcC5+DLoLzGWc0w21yZzgiALj6WeMv12sH\n0V8eGuGu/wnDGmqMMk2fvPM1AHYYtkUlJkPaRXSpG3tw8sma/m/nfRB1g6xBnEIM\nsp99YHwH4yMUkmw4bn7/s8ZoMMqMixgVNad2AAyPpXDNcmTvsUc+yLW07WiLJ5tk\nv1fj+WzLBD0g45SB97csROSp99k3z/mXAf5RD31+BVMQZmgX/Mem+GixxxJk0bFR\nTyRdUWxzAgMBAAECggEAL6VVkKs/Kx6X1rj4bBaEOBkgIxqlkjinDGi5VAiNm1y2\n0qEEMXasrMLJbGV0XEqOz9pCgON8DkYJuS9VEiySBbhmY5HjtkEphQiTkNovdV1x\n3rwICO13qbCSWYxuYwDUKEuo0kSnDcKqXcOcZyNNxEcjsIabl6aAqW4iep8l2AMV\nGyi/qgu0CbnwBtSsh8lcocQDJiSDoVHXXq/WYGGp7NG8aRcwKyU8NpPYuv+AJIg1\nH3MhquUbGCOzkKKJ27RwSqHd57rBZGmE9fu5pWBTzEsdw215D1rqJTgoA5E/lxh0\nw+vEMPCCXaTSkZDLPLZPR5xvz9N1RYbRJ1WGbjsyYQKBgQDKomiDkh6REMC+/hd3\nhhow6rt/GRL2Ani5gCMJW8w/hTBxHNEvhvUZel193Kroyy1WnohAswtEt93JNjYs\nWuQ6Lb0eKXmkoQnFIU8IKdYSahjKfSGmxhw/jml/4UWAGZvG6EHyNpXyWm0ffQn6\neOKrl5iE1pjVPR2mRqIza62YnQKBgQDC8hg+czkUaKlZ0FyrcMMycufc1VW3170N\nx5/d/Af8kVOYyEtd9d8d0aU7jf/lcu1leMyo7Ksg1m3JpGp9uh3qHJslQ2KltxJv\nFycLC4h+TH1pPvdb77m/KtuuhBAtMLlV/6hQJ8ZEKqN7tIOVrg+VlXnOWLxRYj/t\nG7HgwQpkTwKBgQDFtYDUHxXD5Nyrfa/X5vhXEjCu6gAeGHtCQ0vsLa0zrPh+5OcV\nwFAU89eUnmIDkXpDMZsvxYIRInU7hbGFxYk5WrdTXpRZlOa7eKxsAqXkgbB+oWjc\nGCAnwwQcyefN/S6I/MSbV7cmKCSgvJen05sWYWtm8Rtds6viOLi6Ay314QKBgQDC\nXyBNJBWFg9VW96luRsBZTLhiN1OAOlsFokSD0QcljMENVKfQx5Xu5VkaSDsdS7nn\nsspco5z84NAWfRiwwyGQi3UlckqpcB+xJCSJnrY5N4rTpTR9Nki9kr33AzYd2Lby\nSDZJtV66GMloTlPkqehf/Om2FEOv4YZbo4F31wHFgQKBgFl0dPrMIJo3JqADU7xN\nEqL69/aJihD7p+uP3MoOMvB+zdQU9/HQ1nwKGIQA4W9TV/z1Z1bdMBMtyMBUtQag\n/+zfX/4qiRlhLtDG4WMbDgCClnl7xs4WfOo3IJnL2wnJQL9M69CbzVBsFBaFyptc\nac2uwX4kTfIK8UTW+ZmSg8IV\n-----END PRIVATE KEY-----\n",
  "client_email": "wf-2414@vocal-honor-377310.iam.gserviceaccount.com",
  "client_id": "118208137370431684165",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/wf-2414%40vocal-honor-377310.iam.gserviceaccount.com"
}
gc=gspread.service_account_from_dict(credentials)
url='1rLA4ww6zPPD8EagKqgbKrTfvEUVxYPz4dwsJQkhqqoo'


def read_sheet(sheet_key,tab_name):
    gc = gspread.service_account_from_dict(credentials)
    master=gc.open_by_key(sheet_key)
    wks_in=master.worksheet(tab_name)
    pc=get_as_dataframe(wks_in , evaluate_formulas=True, skiprows=0)
    pc.dropna(axis=1, how='all',inplace=True)
    pc.dropna(axis=0, how='all',inplace=True)
    pc=pc.loc[:, ~pc.columns.str.contains('^Unnamed')]
    return pc

def write_sheet(sheet_key,tab_name,df):
    gc = gspread.service_account_from_dict(credentials)
    master=gc.open_by_key(sheet_key)
    wks_out=master.worksheet(tab_name)
    set_with_dataframe(wks_out,df)
    print('written to sheet')
    return None

def clean_sheet(key,tab_name_range):
    gc=gspread.service_account_from_dict(credentials)
    master=gc.open_by_key(key)
    master.values_clear(tab_name_range)
    print('sheet cleared')
    return None



# conn = redshift_connector.connect(
#      host='redshift-cluster-2.c1oevbkxsrwj.ap-south-1.redshift.amazonaws.com',
#      database='reporting',
#      port=5439,
#      user='richard',
#      password='Richard@WF123')
# def query_execute(query):
#     cursor = conn.cursor()
#     res=cursor.execute(query)
#     df: pd.DataFrame = res.fetch_dataframe()
#     print('RDS query executed')
#     return df

# data from base cutoff master sheet -------------------------------------------------------------------



key="17-xEsWCq-Xz1z817yo_5Lge3gskqRJCJkDZoMgnmoG4"

d_off=read_sheet(key,'docket cutoffs')
#drop rows where 1st column is nan
d_off=d_off[d_off.iloc[:,0].notna()]


off_cols=['Warehouse Group', 'Category', 'LP Type', 'Docket CutOff', 'Column1',
       'Scan CutOff', 'CONCAT', 'Docket CutOff Date Time',
       'Docket CutOff Time', 'Docket Cutoff D-1 Date Time',
       'Scanning Cutoff Time', 'Scanning Cutoff', 'Scanning Design Cutoff']


d_off['Scanning Cutoff Time']=pd.to_datetime(d_off['Scanning Cutoff Time']).dt.time


d_off['Docket CutOff'].info()
d_off.info()


# start_date=date.today()-timedelta(days=90)
# today=date.today()
# diwali_docket_q= """


#     select d.affiliate,
#     d.affiliate_id,
#      d.cart_id,
#      d.cart_name,
#      d.docket_number,
#      d.docket_name,
#      d.warehouse,
#      d.warehouse_group,
#      d.is_docket_active as docketactive_flg,
#      d.order_random_id,
#      d.item_sku as itemsku,
#      d.product_type,
#      d.product_category,
#      d.item_quantity as item_qty,
#      d.revenue,
#      d.delivered_timestamp as delivered_dttm,
#      d.rts_timestamp as rts_dttm,
#      d.scanned_timestamp as scanned_dttm,
#      d.installation_timestamp as installation_dttm,
#      (case when d.docket_name LIKE '%LOCAL%' then 'Local'
#      else '3P Partner' end) as lp_type,
#      d.docket_timestamp as docket_dttm,
#      d.attempted as attempted_flg,
#      d.docketed as docketed_flg,
#      d.delivered as delivered_flg,
#      d.installed as installed_flg,
#      d.rts_done as rts_flg,
#      d.promised_edd as pdd,
#      d.warehouse_group as shipping_location_name
#     from diwalidocket_rpt d
#     left join (select order_status,
#                         cart_id,
#                         affiliate_id,
#                         promised_edd,
#                         invoice_number,
#                         docket_number
#     from cartitemlvl_rpt) c on d.cart_id = c.cart_id and d.affiliate_id = c.affiliate_id and d.invoice_credit_number = c.invoice_number
#     where (DATE(d.docket_timestamp) between '""" + str(start_date) + """' AND '""" + str(today) + """')
#     and d.is_docket_active = 1
#     and d.order_type in (NULL,0,5,7)
#     and c.order_status not in ('Cancelled','Hold','RTS','Confirmed')
#     and d.order_random_id is not null
#     and d.product_type != 'NA'
#     and c.docket_number = d.docket_number
#     """





# df_base=query_execute(diwali_docket_q)



start_date=date.today()-timedelta(days=100)
day120=date.today()-timedelta(days=120)
today=date.today()

#########
# Docket_rpt query with filters on all tables

query1=   """        (SELECT "Wakefit" as affiliate,
                COALESCE(wc.order_type, wc1.order_type) as order_type,
                0 as affiliate_id,
                a.cart_id,
                COALESCE(wc.cart_id, wc1.cart_id) as cart_name,
                a.docket_number,
                (SELECT sp.partner_name
                from wf_master_shipping_partners sp
                WHERE sp.id = a.shipper_id) as docket_name,
                (select name from wf_master_warehouse wms
                where wms.id = a.warehouse_id) as warehouse,
                (select city_code from wf_master_warehouse wms
                where wms.id = a.warehouse_id) as warehouse_group,
                a.is_active as docketactive_flg,
                a.order_id as order_random_id,
                a.item_sku as itemsku,
                get_product_type(a.item_sku) as product_type,
                get_product_category(a.item_sku) as product_category,
                a.quantity as item_qty,
                ((COALESCE(wc.net_price, wc1.net_price) * a.quantity)/COALESCE(wc.item_quantity, wc1.item_quantity)) as revenue,
                COALESCE(wai.delivered_timestamp, wc.delivered_timestamp, wc1.delivered_timestamp) as delivered_dttm,
                COALESCE(wai.rts_timestamp,wc.rts_timestamp, wc1.rts_timestamp) as rts_dttm,
                b.scanned_timestamp as scanned_dttm,
                COALESCE(wai.installation_date,wc.installation_date, wc1.installation_date) as installation_dttm,
                a.invoice_timestamp as docket_dttm,
                COALESCE(wc.promised_edd, wc1.promised_edd) as pdd,
                COALESCE(wf.mobile_number, wf1.mobile_number) as mobile_number,
                COALESCE(wf.alternate_mobile_number, wf1.alternate_mobile_number) as alternate_mobile_number,
                COALESCE(wc.promised_dispatch_timestamp,wc1.promised_dispatch_timestamp) as promised_dispatch_timestamp,
                COALESCE(wf.pincode,wf1.pincode) as pincode  
            from wf_invoice_credit a
            LEFT JOIN wf_addon_invoices wai
            ON a.invoice_credit_number = wai.invoice_number
            LEFT JOIN wf_cart_items wc
            ON (wc.invoice_number = a.invoice_credit_number and wc.is_addon_invoice = 0)
            LEFT JOIN wf_cart_items wc1
            ON (wc1.id = a.cart_id and a.aff_id = 0 and wc1.is_addon_invoice = 1)
            LEFT JOIN wf_cart_shipment_stopped_log b

            ON (a.invoice_credit_number = b.invoice_number AND b.shipment_stop_reason = -2)

            LEFT JOIN wf_order_history wf
            ON wf.cart_id  = wc.cart_id
            LEFT JOIN wf_order_history wf1
            ON wf1.cart_id  = wc1.cart_id
            WHERE (CASE WHEN COALESCE(wc.orm_order_status, wc1.orm_order_status) is null
                THEN COALESCE(wc.orm_order_status, wc1.orm_order_status)
                ELSE get_order_status_name(COALESCE(wc.orm_order_status, wc1.orm_order_status))
                END ) not in ('Cancelled','Hold','RTS','Confirmed')
            AND (date(a.docket_timestamp)  BETWEEN  '""" + str(start_date.strftime('%Y-%m-%d')) + """' AND '""" + str(today.strftime('%Y-%m-%d'))  + """')
            and (date(wai.docket_timestamp)  BETWEEN '""" + str(start_date.strftime('%Y-%m-%d')) + """' AND '""" + str(today.strftime('%Y-%m-%d'))  + """'  or wai.docket_timestamp is null)
            and (date(wc.docket_timestamp)  BETWEEN '""" + str(start_date.strftime('%Y-%m-%d')) + """' AND '""" + str(today.strftime('%Y-%m-%d'))  + """'  or wc.docket_timestamp is null)
            and (date(wc1.docket_timestamp)  BETWEEN '""" + str(start_date.strftime('%Y-%m-%d')) + """' AND '""" + str(today.strftime('%Y-%m-%d'))  + """'  or wc1.docket_timestamp is null)
            and (date(b.scanned_timestamp) >= '""" + str(day120.strftime('%Y-%m-%d')) + """'  or b.scanned_timestamp is null)
            and (date(COALESCE(wai.delivered_timestamp, wc.delivered_timestamp, wc1.delivered_timestamp) ) >= current_date-3 or COALESCE(wai.delivered_timestamp, wc.delivered_timestamp, wc1.delivered_timestamp)  is null)
            and COALESCE(wc.order_type, wc1.order_type) IN (NULL,8,0,2,7)


            AND a.ref_invoice_number is null
            AND a.docket_timestamp is not null
            AND a.is_active=1
            AND a.aff_id = 0)

  """


###########

df_base1=exp.run_query(query1,'akshay.sasheendran@wakefit.co','aBbAmn8&')



query2=   """       
  (SELECT get_platform(COALESCE(wc.affiliate_id, wc1.affiliate_id)) as affiliate,
                COALESCE(wc.order_type, wc1.order_type) as order_type,
                COALESCE(wc.affiliate_id, wc1.affiliate_id) as affiliate_id,
                a.cart_id,
                COALESCE(wc.cart_id, wc1.cart_id) as cart_name,
                a.docket_number,
                (SELECT sp.partner_name
				from wf_master_shipping_partners sp
                WHERE sp.id = a.shipper_id) as docket_name,
                (select name from wf_master_warehouse wms
                where wms.id = a.warehouse_id) as warehouse,
                (select city_code from wf_master_warehouse wms
                where wms.id = a.warehouse_id) as warehouse_group,
                a.is_active as docketactive_flg,
                COALESCE(wc.order_random_id, wc1.order_random_id) as order_random_id,
                a.item_sku as itemsku,
                get_product_type(a.item_sku) as product_type,
                get_product_category(a.item_sku) as product_category,
                a.quantity as item_qty,
                (COALESCE(wc.item_price,wc1.item_price) * a.quantity) as revenue,
                COALESCE(wai.delivered_timestamp, wc.delivered_timestamp, wc1.delivered_timestamp) as delivered_dttm,
                COALESCE(wai.rts_timestamp,wc.rts_timestamp, wc1.rts_timestamp) as rts_dttm,
                b.scanned_timestamp as scanned_dttm,
                COALESCE(wai.installation_date,wc.installation_date, wc1.installation_date) as installation_dttm,
                a.invoice_timestamp as docket_dttm,
                COALESCE(wc.promised_edd, wc1.promised_edd) as pdd,
                COALESCE(wc.mobile_number, wc1.mobile_number) as mobile_number,
                COALESCE(wc.alternate_mobile_number, wc1.alternate_mobile_number) as alternate_mobile_number,
                COALESCE(wc.promised_dispatch_timestamp, wc1.promised_dispatch_timestamp) as promised_dispatch_timestamp,
                COALESCE(wc.pincode,wc1.pincode) as pincode             
            from wf_invoice_credit a
            LEFT JOIN wf_addon_invoices wai
            ON a.invoice_credit_number = wai.invoice_number

            LEFT JOIN wf_affiliate_orders wc

            ON (wc.invoice_number = a.invoice_credit_number and wc.is_addon_invoice = 0)
            LEFT JOIN wf_affiliate_orders wc1


            ON (wc1.id = a.cart_id and a.aff_id = wc1.affiliate_id and wc1.is_addon_invoice = 1)
            LEFT JOIN wf_cart_shipment_stopped_log b

            ON (a.invoice_credit_number = b.invoice_number AND b.shipment_stop_reason = -2)
            WHERE (CASE WHEN COALESCE(wc.orm_order_status, wc1.orm_order_status) is null
                THEN COALESCE(wc.orm_order_status, wc1.orm_order_status)
                ELSE get_order_status_name(COALESCE(wc.orm_order_status, wc1.orm_order_status))
                END ) not in ('Cancelled','Hold','RTS','Confirmed')
            AND (date(a.docket_timestamp)  BETWEEN '""" + str(start_date.strftime('%Y-%m-%d')) + """' AND '""" + str(today.strftime('%Y-%m-%d'))  + """'  )

            and (date(wai.docket_timestamp)  BETWEEN '""" + str(start_date.strftime('%Y-%m-%d')) + """' AND '""" + str(today.strftime('%Y-%m-%d'))  + """'  or wai.docket_timestamp is null)
            and (date(wc.docket_timestamp)  BETWEEN '""" + str(start_date.strftime('%Y-%m-%d')) + """' AND '""" + str(today.strftime('%Y-%m-%d'))  + """'  or wc.docket_timestamp is null)
            and (date(wc1.docket_timestamp)  BETWEEN '""" + str(start_date.strftime('%Y-%m-%d')) + """' AND '""" + str(today.strftime('%Y-%m-%d'))  + """'  or wc1.docket_timestamp is null)
            and (date(b.scanned_timestamp) >= '""" + str(day120.strftime('%Y-%m-%d')) + """' or b.scanned_timestamp is null)
            and (date(COALESCE(wai.delivered_timestamp, wc.delivered_timestamp, wc1.delivered_timestamp)) >= current_date-3 or COALESCE(wai.delivered_timestamp, wc.delivered_timestamp, wc1.delivered_timestamp) is null)
            and COALESCE(wc.order_type, wc1.order_type) IN (NULL,8,0,2,7)
            AND a.ref_invoice_number is null 
            AND a.docket_timestamp is not null
            AND a.is_active=1
            AND a.aff_id != 0)

  """


###########

df_base2=exp.run_query(query2,'akshay.sasheendran@wakefit.co','aBbAmn8&')

df_base = pd.concat([df_base1, df_base2], ignore_index=True)

df_base['alternate_mobile_number'] = pd.to_numeric(df_base['alternate_mobile_number'], errors='coerce')

df_base['alternate_mobile_number'] = df_base['alternate_mobile_number'].fillna(0).astype(np.int64)

df_base['pincode'] = df_base['pincode'].astype(str)


df_base['lp_type'] = np.where(df_base['docket_name'].str.contains('Local', case=False), 'Local', '3P Partner')


# df_base=df_base[df_base['docket_number'].isin(z)]

###################
#####
##
########
#####

# db=pd.read_csv(r'D:\vs wrkspce\Pex\LDB1.csv')
# red=pd.read_csv(r'D:\vs wrkspce\Pex\Local1.csv')

# ldb=db['docket_number'].tolist()
# ldb=tuple(ldb)

# re=red[~red['docket_number'].isin(ldb)]
# vs=re['docket_number'].tolist()
# df_base['docket_number'] = pd.to_numeric(df_base['docket_number'], errors='coerce', downcast='integer')
# c=df_base[df_base['docket_number'].isin(vs)]

# df_base=c

###################
#####
##
########
#####

# df_base[df_base.values=='23120400015724']

# Product Category mapping ----------------------------------------------------------------
if 'Product Category' in df_base.columns:
    del df_base['Product Category']


df_base.drop(columns=['PC_x', 'PC_y'],inplace=True,errors='ignore')

pt=read_sheet("1r3Ec51QnU0k454kETNcuLy2busSHcRz3uqzVVfi_mUo","prod cat")
pt=pt[['PT','PC']]
pt=pt[pt.iloc[:,0].notna()]

pt.drop_duplicates(subset=['PT'], keep='first',inplace=True)


df_base=df_base.merge(pt.set_index('PT'),left_on='product_type',right_on='PT',how='left')
df_base = df_base.loc[:, ~df_base.columns.duplicated()]
df_base.rename(columns={'PC':'Product Category'},inplace=True)
df_base[df_base['Product Category'].isna()]['product_type'].value_counts().to_frame().reset_index()
df_base1=df_base

df_base1['Product Category'].fillna(df_base1['product_category'],inplace=True)


df_base1['Product Category'] = np.where(df_base1['product_type'] == 'Office Chair', 'Mattress', df_base1['Product Category'])

# sku_list = ['WSFAFLPN1FWBG','WSFAFLPN1FWBL','WSFAFLPN1FWGR','WSFAFLPN1FWPL','WSFAFLPN2FWBG',
#             'WSFAFLPN2FWBL','WSFAFLPN2FWGR','WSFAFLPN2FWPL','WSFAFLPN3FWBG','WSFAFLPN3FWBL',
#             'WSFAFLPN3FWGR','WSFAFLPN3FWPL']

# df_base1['Product Category'] = np.where(df_base1['itemsku'].isin(sku_list), 'Mattress', df_base1['Product Category'])


# LP_type refix
# def lp_type(row):
#     # if row of docket_name contains LOCAL then Local else 3P Partner
#     if 'LOCAL' in row['docket_name']:
#         val='Local'
#     else:
#         val='3P Partner'
#     return val

# df_base1['lp_type']=df_base1.apply(lp_type,axis=1)
# df_base1['lp_type']=np.where(df_base1['lp_type']=='LP','3P Partner',df_base1['lp_type'])



#
d_off=d_off[['Warehouse Group', 'Category', 'LP Type',   'Column1',
             'CONCAT','Docket CutOff Time','Scanning Cutoff Time']]




df_base1['warehouse']=df_base1['warehouse'].str[:3]

# df_base1['Product Category'] = df_base1.apply(lambda row: 'Sofa' if row['warehouse'] in ['BLR', 'HYD'] and row['itemsku'] in sku_list else row['Product Category'], axis=1)



df_base1['lp_type']=np.where(df_base1['lp_type']=='LP','3P Partner',df_base1['lp_type'])

df_base1['docket_name']=df_base1['docket_name'].astype(str)
df_base2=df_base1.merge(d_off,left_on=['warehouse','Product Category','lp_type'],right_on=['Warehouse Group','Category','LP Type'],how='left')
df_base2[df_base2['Warehouse Group'].isna()][['warehouse','Product Category','lp_type']].drop_duplicates() # validate if returning no rows
df_base2[['docket_name','lp_type']].drop_duplicates().sort_values('lp_type')


# lp type Final Fix
def lp_type_fix(row):
    if row['docket_name']=='Others':
        val='B2B'
    elif row['docket_name']=='DTDC':
        val= '3P Partner'
    elif row['docket_name']=='Retail Delivery':
        val= 'Local'
    else:
        val=row['lp_type']
    return val


df_base2['lp_type']=df_base2.apply(lp_type_fix,axis=1)



# rename scanned_dttm to dispatchfromwarehouse_dttm
df_base2.rename(columns={'scanned_dttm':'dispatchfromwarehouse_dttm'},inplace=True)
#rename docket_dttm to docket_timestamp
df_base2.rename(columns={'docket_dttm':'docket_timestamp'},inplace=True)



df_base2['dispatchfromwarehouse_dttm']=pd.to_datetime(df_base2['dispatchfromwarehouse_dttm'])
df_base2['docket_timestamp'].max()


# DCF & SCF ----------------------------------------------------

# add column called today which is today's date


df_base2['today']=pd.to_datetime('today').date()
#  add column called previous day which is today's date -1
df_base2['previous_day']=df_base2['today']-timedelta(days=1)


# d_off['Docket CutOff Time']=pd.to_datetime(d_off['Docket CutOff Time']).dt.time

df_base2['Docket CutOff Time']=pd.to_datetime(df_base2['Docket CutOff Time'].str.strip()).dt.time
# df_base2['Docket CutOff Time'] = pd.to_datetime(df_base2['Docket CutOff Time'], format='%H:%M:%S').dt.time

df_base2=df_base2[df_base2['Docket CutOff Time'].notna()]

def dc_off(row):
    val = np.nan
    if row['Docket CutOff Time'] is not None:
        if row['Column1'] == 'Same Day':
            # Convert date to datetime and add time
            val = datetime.combine(row['today'], row['Docket CutOff Time'])
        elif row['Column1'] == 'Previous day' or row['Column1'] == 'Previous Day':
            # Convert date to datetime and add time
            val = datetime.combine(row['previous_day'], row['Docket CutOff Time'])
    return val

df_base2['DCOFF'] = df_base2.apply(dc_off, axis=1)
df_base2['docket_timestamp'] = pd.to_datetime(df_base2['docket_timestamp'])

df_base2['DCOFF'].unique()

#######
# df_base2['DCOFF']=df_base2['DCOFF']-timedelta(days=1)
######




def dcf(row):

    if row['docket_timestamp'] <= row['DCOFF']:
        val = '1'
    else:
        val = '0'
    return val

df_base2['DCF'] = df_base2.apply(dcf, axis=1)
df_base2.columns

# rename delivered_dttm to delivered_timestamp
df_base2.rename(columns={'delivered_dttm':'delivered_timestamp'},inplace=True)
df_base2['delivered_timestamp'] = pd.to_datetime(df_base2['delivered_timestamp'])

def delivered_before_dcoff(row):
    if row['delivered_timestamp'] is not None:
        if row['delivered_timestamp'] < row['DCOFF']:
            val = '1'
        else:
            val = '0'
    else:
        val = '0'
    return val

df_base2['delivered_before_dcoff'] = df_base2.apply(delivered_before_dcoff, axis=1)


# df_base2['Scanning Cutoff Time']=pd.to_datetime(df_base2['Scanning Cutoff Time']).dt.time

df_base2['Scanning Cutoff Time']=pd.to_datetime(df_base2['Scanning Cutoff Time'],format='%H:%M:%S').dt.time


df_base2['dispatchfromwarehouse_dttm'] = pd.to_datetime(df_base2['dispatchfromwarehouse_dttm'])

def sc_off(row):
    val = np.nan
    if not pd.isna(row['Scanning Cutoff Time']):
        # Convert date to datetime and add time
        val = datetime.combine(row['today'], row['Scanning Cutoff Time'])
    return val

df_base2['SCOFF'] = df_base2.apply(sc_off, axis=1)

############
# df_base2['SCOFF']=df_base2['SCOFF']-timedelta(days=1)
#####

def scf(row):
    if row['dispatchfromwarehouse_dttm'] <= row['SCOFF']:
        val = '1'
    else:
        val = '0'
    return val

df_base2['SCF'] = df_base2.apply(scf, axis=1)


def base_data(row):
    if row['DCF'] == '1' and row['SCF'] == '1':
        val = '1'

    elif row['DCF'] == '0' and row['SCF'] == '1':
        val = '1'
    elif row['DCF'] == '1' and row['SCF'] == '0':
        val = '1'
    elif row['DCF'] == '0' and row['SCF'] == '0':
        val = '0'
    else:
        val = np.nan
    return val



df_base2['Base Data'] = df_base2.apply(base_data, axis=1)


# inflating base data for todays scan when both DCF and SFC are 0

# def base_data_inflation(row):
#     if row['DCF'] == '0' and row['SCF'] == '0' and row['dispatchfromwarehouse_dttm'] <  datetime.today().replace(hour=0, minute=0, second=0, microsecond=0):
#         val = '0'
#     else:
#         val = '1'
#     return val

# df_base2['Base Data']=df_base2.apply(base_data_inflation, axis=1)

# df_base2['Base Data'].value_counts()

# # filter df_base2 where DCF and SCF = 0

# df_base2[['DCF','SCF']].drop_duplicates()

# df_base2[(df_base2['DCF']=='0')&(  df_base2['SCF']=='0' )&( df_base2['Base Data']=='1')]['dispatchfromwarehouse_dttm'].min()


# import dtale

# create column Final base where base data is 1 and delivvered before dcoff is 0
df_base2['Final Base'] = np.where((df_base2['Base Data'] == '1') & (df_base2['delivered_before_dcoff'] == '0'), '1', '0')



# ---------------------------------------------------------------------------------------------
#  LM - Base picks up and continues from here


df_base3=df_base2[df_base2['Final Base']=='1']



df_base3['Final Base'].astype(int).sum()

rem=['RTS', 'Cancelled','Hold']
#remove rows in order_status that are in list rem
# df_base2=df_base2[~df_base2['order_status'].isin(rem)]

df_base2

def lp_3pl_filter(row):
    if row['lp_type'] == '3P Partner':
        if row['dispatchfromwarehouse_dttm'] < row['DCOFF']:
            val = '1'
        else:
            val = '0'
    else:
        val = '0'
    return val

df_base3['3PL Filter'] = df_base3.apply(lp_3pl_filter, axis=1)


def DCF_Scan(row):
    if row['dispatchfromwarehouse_dttm'] <= row['DCOFF']-timedelta(days=1):
        val=0

    elif row['dispatchfromwarehouse_dttm'] >= row['DCOFF']  :
        val=0

    else:
        val = '1'
    return val



df_base3['DCF_Scan'] = df_base3.apply(DCF_Scan, axis=1)


df_base3=df_base3[df_base3['3PL Filter']=='0']


df_base3['pdd'] = pd.to_datetime(df_base3['pdd'], errors='coerce')

current_date = datetime.now().date()

df_base3['pdd breached'] = df_base3['pdd'].apply(lambda x: 1 if pd.notna(x) and x.date() <= current_date else 0)



# df_base3=df_base3[df_base3['delivered_timestamp'].isna()]
# df_base3['affiliate_id'] = df_base3['affiliate_id'].astype(int)

df_base3['new_SCOFF']=df_base3['SCOFF']-timedelta(days=1)

def check_condit(row):
    if row['DCF'] == '1' and row['SCF'] == '1':
        if row['dispatchfromwarehouse_dttm'] < row['new_SCOFF'] and row['delivered_timestamp'] is None:
            return '1'
        else:
            return '0'
    else:
        return '0'
df_base3['scanned_before_prev_scoff'] = df_base3.apply(check_condit, axis=1)


lmbase1 = df_base3[df_base3['Final Base'] == '1']

lmbase = lmbase1[~(lmbase1['scanned_before_prev_scoff'] == '1')]


# lm
# base = lmbase[~(lmbase['delivered_timestamp'] < lmbase['DCOFF'])]
# lmbase = lmbase[~(lmbase['delivered_timestamp'] < datetime.today())]
today_min = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

lmbase = lmbase[~(lmbase['delivered_timestamp'] <today_min)]
# df_base3[df_base3.values=='23120400015710']
# c=lmbase[lmbase['delivered_timestamp'].notna()]

# c['delivered_timestamp'].ax()

lm=lmbase[lmbase['lp_type']=='Local']


lm.reset_index(inplace=True)
lm.rename(columns={'index': 'SNO'}, inplace=True)

# lm[['attempted_flg', 'docketed_flg', 'delivered_flg', 'installed_flg', 'rts_flg']] = np.random.randint(2, size=(len(lm), 5))

lm['shipping_location_name'] = lm['warehouse_group'].copy()

lm["attempted_flg"] = lm["dispatchfromwarehouse_dttm"].apply(lambda x : 0 if x is pd.NaT else 1)
lm["docketed_flg"] = lm["docket_timestamp"].apply(lambda x : 0 if x is pd.NaT else 1)
lm["delivered_flg"] = lm["delivered_timestamp"].apply(lambda x : 0 if x is pd.NaT else 1)
lm["installed_flg"] = lm["installation_dttm"].apply(lambda x : 0 if x is pd.NaT else 1)
lm["rts_flg"] = lm["rts_dttm"].apply(lambda x : 0 if x is pd.NaT else 1)




lm=lm[['SNO', 'affiliate', 'affiliate_id', 'cart_id', 'cart_name',
       'docket_number', 'docket_name', 'warehouse', 'warehouse_group',
       'docketactive_flg', 'order_random_id', 'itemsku', 'product_type',
       'product_category', 'item_qty', 'revenue', 'delivered_timestamp',
       'rts_dttm', 'dispatchfromwarehouse_dttm', 'installation_dttm',
       'lp_type', 'docket_timestamp', 'attempted_flg', 'docketed_flg',
       'delivered_flg', 'installed_flg', 'rts_flg', 'pdd',
       'shipping_location_name', 'Product Category', 'Warehouse Group',
       'Category', 'LP Type', 'Column1', 'CONCAT', 'Docket CutOff Time',
       'Scanning Cutoff Time', 'today', 'previous_day', 'DCOFF', 'DCF',
       'delivered_before_dcoff', 'SCOFF', 'SCF', 'Base Data', 'Final Base',
       '3PL Filter', 'DCF_Scan', 'pdd breached', 'new_SCOFF',
       'scanned_before_prev_scoff','order_type','mobile_number','alternate_mobile_number','promised_dispatch_timestamp','pincode']]
lm['product_category'].value_counts()


# clean_sheet('1ns3GwF9p4FY7WHUdVWeQbgjmaReQMs3bS23awsglNCk','Akshay Local_Dump D0')
# write_sheet('1ns3GwF9p4FY7WHUdVWeQbgjmaReQMs3bS23awsglNCk','Akshay Local_Dump D0',lm)











orders=lm['docket_number'].unique()

orders = tuple(filter(lambda x: not pd.isna(x), orders))

len(orders)
# create a list of tuples from orders in chuncks of 5000 each
ids = tuple([tuple(orders[x:x+500]) for x in range(0, len(orders), 500)])
frames=[]
for i in ids:
    print(len(i))

    wf_sl=f'''
    (SELECT
        logs.docket_number,
        logs.is_forward,
        logs.old_pickup_date AS old_pickup_date,
        logs.new_requested_date AS new_requested_date,
        logs.updated_timestamp,
        l.update_by_role,
        orm.name AS updated_by_id,
        logs.planning_remarks,
        LEFT(wf_master_warehouse.city_code, 3) AS Warehouse_name
    FROM
        wf_locus_logs_history AS logs
    JOIN
        wf_logistics_status_update_history l ON l.id = logs.sheet_upload_id
    JOIN
        wf_invoice_credit ON wf_invoice_credit.docket_number = logs.docket_number
    JOIN
        wf_master_warehouse ON wf_master_warehouse.id = wf_invoice_credit.warehouse_id
    JOIN
        wf_orm_team_members orm ON orm.login_id = l.updated_by_id            
    WHERE
    logs.docket_number in {i}


    ORDER BY
        logs.docket_number)
    UNION
    (SELECT
        logs.docket_number,
        logs.is_forward,
        logs.old_pickup_date AS old_pickup_date,
        logs.new_requested_date AS new_requested_date,
        logs.updated_timestamp,
        0 AS update_by_role,
        'NIL' AS updated_by_id,
        logs.planning_remarks,
        LEFT(wf_master_warehouse.city_code, 3) AS Warehouse_name
    FROM
        wf_schedule_date_logs_data AS logs
    JOIN
        wf_invoice_credit ON wf_invoice_credit.docket_number = logs.docket_number
    JOIN
        wf_master_warehouse ON wf_master_warehouse.id = wf_invoice_credit.warehouse_id


                
    WHERE
    logs.docket_number in {i}


    ORDER BY
        logs.docket_number)

    '''
    dfs=exp.run_query(wf_sl,'akshay.sasheendran@wakefit.co','aBbAmn8&')
    frames.append(dfs)
df_sl=pd.concat(frames)

df_crm=df_sl


# # crete a tuple out of docket_number from lm df
# docket_numbers = tuple(lm['docket_number'])
# docket_numbers

# # remove nan from docket_numbers tuple
# docket_numbers = tuple(filter(lambda x: not pd.isna(x), docket_numbers))


# crm_query_new=f'''

# SELECT
#     logs.docket_number,
#     logs.is_forward,
#     logs.old_pickup_date AS old_pickup_date,
#     logs.new_requested_date AS new_requested_date,
#     logs.updated_timestamp,
#     l.update_by_role,
#     orm.name AS updated_by_id,
#     logs.planning_remarks,
#     LEFT(wf_master_warehouse.city_code, 3) AS Warehouse_name
# FROM
#     wf_locus_logs_history AS logs
# JOIN
#     wf_logistics_status_update_history l ON l.id = logs.sheet_upload_id
# JOIN
#     wf_invoice_credit ON wf_invoice_credit.docket_number = logs.docket_number
# JOIN
#     wf_master_warehouse ON wf_master_warehouse.id = wf_invoice_credit.warehouse_id
# JOIN
#     wf_orm_team_members orm ON orm.login_id = l.updated_by_id



             
# WHERE
# logs.docket_number in {str(docket_numbers)}


# ORDER BY
#     logs.docket_number

# '''



# df_crm=exp.run_query(crm_query_new,'akshay.sasheendran@wakefit.co','aBbAmn8&')

# Create two datetime var inhp date and pod_date , inhp_date=today at 10.30 AM pod_date = today 12.30 PM

inhp_date = datetime.today().replace(hour=10, minute=30,second=0)
pod_date = datetime.today().replace(hour=12, minute=30,second=0)

df_crm['updated_timestamp'] = pd.to_datetime(df_crm['updated_timestamp'])
# apply filter on df_crm based on warehouse_name if value in ('GRG', 'BLR', 'MUM', 'PUN', 'CHN', 'HYD', 'KOL') apply filter on inhp_date  on updated_timestamp

def updated_filter(row):
    if row['Warehouse_name'] in ('GRG','FBD','BLR', 'MUM', 'PUN', 'CHN', 'HYD', 'KOL') and row['updated_timestamp'] <= inhp_date:
        return True
    if row['Warehouse_name'] in ('LUK','COM','IDR','AMD','JPR','NGP','BBR','VKP','PTN','OKL','GOA','MYQ','VJW','DWK') and row['updated_timestamp'] <= pod_date:
        return True
    else:
        return False
    
df_crm['updated_filter']=df_crm.apply(updated_filter,axis=1)


df_crm['updated_filter'].value_counts()

df_crm=df_crm[df_crm['updated_filter']==True]

# create a rank against each docket number order by updated_timestamp desc
df_crm['remark_rank'] = df_crm.groupby('docket_number')['updated_timestamp'].rank(method='dense', ascending=False)
# import dtale

# filter remark_rank=1
df_crm=df_crm[df_crm['remark_rank']==1]
df_crm['docket_number'].nunique()

df_crm
#new_requested_date to datetime
df_crm['new_requested_date'] = pd.to_datetime(df_crm['new_requested_date'])

#Invalid Date Flag
# df_crm if new_requested_date is blank then no reason mentioned , if new_requested_date is <= today then invalid date , if > today valida date

def invalid_date_flag(row):
    
    if pd.isna(row['new_requested_date']):
     return 'No Reason Mentioned'
    elif row['new_requested_date'] <= datetime.today():
        return 'Invalid Date'
    else:
        return 'Valid Date'

df_crm['invalid_date_flag']=df_crm.apply(invalid_date_flag,axis=1)

#convert df_crm docket_nunber to obj

df_crm['docket_number']=df_crm['docket_number'].astype(str)





key="1r3Ec51QnU0k454kETNcuLy2busSHcRz3uqzVVfi_mUo"

crm_remarks=read_sheet(key,'CRM - Remarks')

crm_remarks.dropna(subset=['Attribution'],inplace=True)

# remove unamed cols in crm_remarks
crm_remarks.drop(columns=crm_remarks.columns[crm_remarks.columns.str.contains('unnamed',case=False)],inplace=True)
crm_remarks.drop_duplicates(inplace=True)
# pull crm remarks from gsheet

# join crm remarks on df_crm
df_crm=df_crm.merge(crm_remarks.set_index('Planning Failure Remarks'),left_on='planning_remarks',right_on='Planning Failure Remarks',how='left')

#Cx Dependency Flag - =IF(Attribution="Cx - Attribution",1,0)

df_crm['Cx Dependency Flag']=np.where(df_crm['Attribution']=='Cx - Attribution',1,0)


# join dr_crm on lm on docket_number
final=pd.merge(lm,df_crm,on='docket_number',how='left')

final['Attribution'].isna().sum()



final['Docket Ageing']=datetime.today()-pd.to_datetime(final['docket_timestamp'])


final['Docket Ageing']=final['Docket Ageing'].dt.days


def docket_ageing_grp(row):
    if row['Docket Ageing']<1:
        return 0
    elif row['Docket Ageing']<2:
        return 1
    elif row['Docket Ageing']<3:
        return 2
    elif row['Docket Ageing']<4:
        return 3
    else:
        return '3+'

final['Docket Ageing Grp']=final.apply(docket_ageing_grp,axis=1)

final.drop_duplicates(subset=['docket_number'], keep='first',inplace=True)


final=final[['SNO', 'affiliate', 'affiliate_id', 'cart_id', 'cart_name',
       'docket_number', 'docket_name', 'warehouse', 'warehouse_group',
       'docketactive_flg', 'order_random_id', 'itemsku', 'product_type',
       'product_category', 'item_qty', 'revenue', 'delivered_timestamp',
       'rts_dttm', 'dispatchfromwarehouse_dttm', 'installation_dttm',
       'lp_type', 'docket_timestamp', 'attempted_flg', 'docketed_flg',
       'delivered_flg', 'installed_flg', 'rts_flg', 'pdd',
       'shipping_location_name', 'Product Category', 'Warehouse Group',
       'Category', 'LP Type', 'Column1', 'CONCAT', 'Docket CutOff Time',
       'Scanning Cutoff Time', 'today', 'previous_day', 'DCOFF', 'DCF',
       'delivered_before_dcoff', 'SCOFF', 'SCF', 'Base Data', 'Final Base',
       '3PL Filter', 'DCF_Scan', 'pdd breached', 'new_SCOFF',
       'scanned_before_prev_scoff', 'is_forward',
       'old_pickup_date', 'new_requested_date', 'updated_timestamp',
       'update_by_role', 'updated_by_id', 'planning_remarks', 'Warehouse_name',
       'updated_filter', 'remark_rank', 'invalid_date_flag', 'Attribution',
       'Cx Dependency Flag', 'Docket Ageing', 'Docket Ageing Grp','order_type','mobile_number','alternate_mobile_number','promised_dispatch_timestamp','pincode']]

final['alternate_mobile_number']=final['alternate_mobile_number'].astype(str)



from pytz import timezone

tz = timezone('Asia/Kolkata')
today_ist = datetime.now(tz)
report_date=today_ist.date()



final['report_date'] = report_date



final['promised_dispatch_timestamp'] = pd.to_datetime(final['promised_dispatch_timestamp'], errors='coerce')

final['pdd'] = pd.to_datetime(final['pdd'], errors='coerce')


def priority_flag(row):
    promised_edd = row['pdd']
    
    if pd.isnull(promised_edd):
        return 'NA'
    
    today = datetime.now().date()
    days_diff = (promised_edd.date() - today).days
    
    if days_diff <= 3:
        return 'P0'
    elif days_diff == 4:
        return 'P1'
    elif days_diff == 5:
        return 'P2'
    elif days_diff == 6:
        return 'P3'
    else:
        return 'P4'

# Apply the function to create the Priority_flag column
final['Priority_flag'] = final.apply(priority_flag, axis=1)

final=final[final['docket_name']!='BLR10 LOCAL']


final=final[['SNO', 'affiliate', 'affiliate_id', 'cart_id', 'cart_name',
       'docket_number', 'docket_name', 'warehouse', 'warehouse_group',
       'docketactive_flg', 'order_random_id', 'itemsku', 'product_type',
       'product_category', 'item_qty', 'revenue', 'delivered_timestamp',
       'rts_dttm', 'dispatchfromwarehouse_dttm', 'installation_dttm',
       'lp_type', 'docket_timestamp', 'attempted_flg', 'docketed_flg',
       'delivered_flg', 'installed_flg', 'rts_flg', 'pdd',
       'shipping_location_name', 'Product Category', 'Warehouse Group',
       'Category', 'LP Type', 'Column1', 'CONCAT', 'Docket CutOff Time',
       'Scanning Cutoff Time', 'today', 'previous_day', 'DCOFF', 'DCF',
       'delivered_before_dcoff', 'SCOFF', 'SCF', 'Base Data', 'Final Base',
       '3PL Filter', 'DCF_Scan', 'pdd breached', 'new_SCOFF',
       'scanned_before_prev_scoff', 'is_forward',
       'old_pickup_date', 'new_requested_date', 'updated_timestamp',
       'update_by_role', 'updated_by_id', 'planning_remarks', 'Warehouse_name',
       'updated_filter', 'remark_rank', 'invalid_date_flag', 'Attribution',
       'Cx Dependency Flag', 'Docket Ageing', 'Docket Ageing Grp','order_type','mobile_number','alternate_mobile_number','report_date','Priority_flag', 'promised_dispatch_timestamp','pincode']]




orders=final['docket_number'].unique()

orders = tuple(filter(lambda x: not pd.isna(x), orders))
len(orders)
# create a list of tuples from orders in chuncks of 5000 each
ids = tuple([tuple(orders[x:x+500]) for x in range(0, len(orders), 500)])
frames=[]
for i in ids:
    print(len(i))

    wf_sl=f'''
    select docket_number,create_timestamp,lp_order_status from wf_shipsy_forward_callback_log
    WHERE docket_number IN {i}
    AND lp_order_status = 'reachedathub'
    order by create_timestamp

    '''
    dfs=exp.run_query(wf_sl,'akshay.sasheendran@wakefit.co','aBbAmn8&')
    frames.append(dfs)
df_sl=pd.concat(frames)

df_doc=df_sl

df_doc['_rank'] = df_doc.groupby('docket_number')['create_timestamp'].rank(method='dense', ascending=False)

df_doc=df_doc[df_doc['_rank']==1]

df_doc.rename(columns={'create_timestamp':'reconcile_timestamp'},inplace=True)


final=final.merge(df_doc[['docket_number','reconcile_timestamp']],on='docket_number',how='left')
final['reconcile_timestamp'] = pd.to_datetime(final['reconcile_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
final['dispatchfromwarehouse_dttm'] = pd.to_datetime(final['dispatchfromwarehouse_dttm']).dt.strftime('%Y-%m-%d %H:%M:%S')

def rec_flg(row):
    # Convert string timestamp to datetime if needed
    reconcile_time = pd.to_datetime(row['reconcile_timestamp'])
    dispatch_time = pd.to_datetime(row['dispatchfromwarehouse_dttm'])
    
    if dispatch_time < reconcile_time:
        return '1'
    else:
        return '0'

final['reconcile_flag'] = final.apply(rec_flg, axis=1)





clean_sheet('1j2LQg-sUmIlR_b_2oDyobu67dj2RV50Zs0Jld50i-GM','D0 Raw Local')
write_sheet('1j2LQg-sUmIlR_b_2oDyobu67dj2RV50Zs0Jld50i-GM','D0 Raw Local',final)


dockno = tuple(final['docket_number'])

dockno = tuple(filter(lambda x: not pd.isna(x), dockno))

# final['docket_number']=final['docket_number'].fillna(0).astype(np.int64)



cartdoc=f'''

select t1.cart_id,t1.aff_id, t1.docket_number,t2.first_docketdt from wf_invoice_credit  as t1

inner join (select cart_id ,aff_id , min(docket_timestamp) as first_docketdt from wf_invoice_credit 
          
          group by 1,2) as t2 on t1.cart_id = t2.cart_id and t1.aff_id = t2.aff_id

where t1.docket_number in {str(dockno)}


'''
dfs=exp.run_query(cartdoc,'akshay.sasheendran@wakefit.co','aBbAmn8&')

dfs['docket_number']=dfs['docket_number'].astype(str)

dd12=final.merge(dfs[['docket_number','first_docketdt']],on='docket_number',how='left')


clean_sheet('1i2gAvVnY_jP4sR4g1Cuw2gAn9Qg_WoPISoectosOpcI','Local D0')
write_sheet('1i2gAvVnY_jP4sR4g1Cuw2gAn9Qg_WoPISoectosOpcI','Local D0',dd12)



def gchat_hook_pex(text):
    url = 'https://chat.googleapis.com/v1/spaces/AAAASwB85pA/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=gAx3VVAsjWGvqtMAUtJbM40-Wczdkh3zQPTp2PkRb-I'
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    data = json.dumps({'text': text})
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()


script_name="Local D0"
now=datetime.now(tz= pytz.timezone('Asia/Kolkata'))
message=f'Script executed successfully for - {script_name} at - {now.strftime("%Y-%m-%d %H:%M:%S")} with rows*cols - {final.shape} added '
gchat_hook_pex(message)
