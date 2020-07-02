#!/usr/bin/env python3
import data_base as db

import pandas as pd


connection, cursor = db.connect_db()


with open('./data/billspromo.csv') as billspromo:
	for line in billspromo:
		cursor.copy_from(billspromo, 'billspromo', 
			columns=(
				'check_id',
				'check_date' ,
				'category_cd',
				'group_cd',
				'subgroup_cd' ,
				'promo_id' ,
				'customer_id' ,
				'discount_type' ,
				'discount_amount' ,
				'promo_nm'), sep=";", null='NULL')

with open('./data/bills.csv') as bills:
	for line in bills:
		cursor.copy_from(bills, 'bills', 
			columns=(
				'check_id',
				'check_date' ,
				'customer_id',
				'product_count',
				'before_discount_amt',
				'bonus_payment_amt',
				'bonus_points',
				'discount',
				'after_discount_amt',
				'product_category',
				'product_group',
				'subgroup',
				'category_cd',
				'group_cd',
				'subgroup_cd'), sep=";", null='NULL')

with open('./data/clients.csv') as clients:
	for line in clients:
		cursor.copy_from(clients, 'clients', 
			columns=(
				'customer_id',
				'sex',
				'loyality_registration_date',
				'first_purchase_date',
				'phone_correct_flg',
				'age',
				'children_count'), sep=";", null='NULL')

with open('./data/pers_offer_1.csv') as pers_offer:
	for line in pers_offer:
		cursor.copy_from(pers_offer, 'pers_offer', 
			columns=(
				'campaign_sk',
				'campaign_type',
				'offer_start_dt',
				'offer_end_dt',
				'RESPONSE_DTTM',
				'customer_id',
				'discount_type1',
				'discount_type',
				'discount_amt',
				'promo_id',
				'mechanics_nm',
				'msg_text',
				'item1',
				'item2'), sep=";", null='NULL')

with open('./data/pers_offer_2.csv') as pers_offer:
	for line in pers_offer:
		cursor.copy_from(pers_offer, 'pers_offer', 
			columns=(
				'campaign_sk',
				'campaign_type',
				'offer_start_dt',
				'offer_end_dt',
				'RESPONSE_DTTM',
				'customer_id',
				'discount_type1',
				'discount_type',
				'discount_amt',
				'promo_id',
				'mechanics_nm',
				'msg_text',
				'item1',
				'item2'), sep=";", null='NULL')

with open('./data/pers_offer_3.csv') as pers_offer:
	for line in pers_offer:
		cursor.copy_from(pers_offer, 'pers_offer', 
			columns=(
				'campaign_sk',
				'campaign_type',
				'offer_start_dt',
				'offer_end_dt',
				'RESPONSE_DTTM',
				'customer_id',
				'discount_type1',
				'discount_type',
				'discount_amt',
				'promo_id',
				'mechanics_nm',
				'msg_text',
				'item1',
				'item2'), sep=";", null='NULL')

with open('./data/products.csv') as products:
	for line in products:
		cursor.copy_from(products, 'products', 
			columns=(
				'product_direction_cd',
				'category_cd',
				'group_cd',
				'subgroup_cd',
				'product_direction_name',
				'category_name',
				'group_name',
				'subgroup_name'), sep=";", null='NULL')

with open('./data/campaign.csv') as campaign:
	for line in campaign:
		cursor.copy_from(campaign, 'campaign', 
			columns=(
				'campaign_sk',
				'campaign_nm',
				'campaign_type',
				'campaign_desc'), sep=";", null='NULL')
with open('./data/item.csv') as item:
	for line in item:
		cursor.copy_from(item, 'item', 
			columns=(
				'item',
				'model_item'), sep=";", null='NULL')
cursor.execute("SELECT * FROM pers_offer LIMIT 10")
result = cursor.fetchall()


db.close_connection(connection, cursor)
