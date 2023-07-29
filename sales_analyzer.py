import pandas as pd
import os
import xlwings as xw
import time
import numpy as np
import pyodbc
from database.database import get_sales_from_db 
from database.file import get_sales_from_csv 
from datetime import timedelta
from collections import defaultdict


def group_dataframe(df):
    df['Date'] = pd.to_datetime(df['Date'])
    grouped_df = df.groupby(['Store', 'Order Type', 'Date', 'Product', 'Check', 'Hour']).sum().reset_index()

    return grouped_df

def get_maps():
    print("map.xlsx should be in the Data directory.")

    folder_path = os.path.join(os.getcwd(), 'Data')
    file_path = os.path.join(folder_path, 'map.xlsx')

    app = xw.App(visible=False)
    workbook = app.books.open(file_path)

    replacement_sheet = workbook.sheets['Map']
    replacement_range = replacement_sheet.range('Replacement')
    replacement = replacement_range.value

    products_sheet = workbook.sheets['Map']
    products_range = products_sheet.range('Products')
    products = products_range.value

    ownFr_sheet = workbook.sheets['Map']
    ownFr_range = ownFr_sheet.range('ownFR')
    ownFr = ownFr_range.value

    workbook.close()
    app.quit()

    return replacement, products, ownFr

def modifyData(data, replacement, products, ownFr):
    soldProducts = data['Product'].tolist()
    stores = data['Store'].tolist()
    unmatched = []

    for i in range(len(soldProducts)):
        product_match = False
        
        for old_value, new_value in replacement:
            if old_value is not None and soldProducts[i].lower() == old_value.lower():
                soldProducts[i] = new_value
                break

        for p in products[1:]:
            if p[0] is not None and soldProducts[i].lower() == p[0].lower():
                product_match = True
                data.at[i, 'Type'] = p[1]
                data.at[i, 'Category'] = p[2]
                data.at[i, 'Group'] = p[3]
                data.at[i, 'Detail'] = p[4]
                break

        if not product_match:
            unmatched.append(soldProducts[i])

        for own in ownFr[1:]:
            if own[0] is not None and stores[i].lower() == own[0].lower():
                data.at[i, 'OWNFR'] = own[2]
                data.at[i, 'Region'] = own[1]
                break
    
    data['Product'] = soldProducts

    unmatched_unique = list(set(unmatched))
    if unmatched_unique:
        new_sales_df = pd.DataFrame({'Product': unmatched_unique})
        print(new_sales_df)
        new_sales_df.to_csv('unmatched_products.csv', index=False)
        raise Exception('Unmatched products found. The program has been stopped.')
    
    return data

def get_product_mix(modifiedData):
    product_mix = modifiedData.groupby(['Product', 'Type', 'Category', 'Group', 'Detail', 'OWNFR', 'Order Type']).agg({
        'Quantity': 'sum',
        'Sales': 'sum'
    }).reset_index()

    own_alacarte_quantity = product_mix.loc[(product_mix['Order Type'] == 'Alacarte') & (product_mix['OWNFR'] == 'OWN')].copy()
    own_alacarte_quantity.rename(columns={'Quantity': 'OWN Alacarte Quantity', 'Sales': 'OWN Alacarte Sales'}, inplace=True)

    fr_alacarte_quantity = product_mix.loc[(product_mix['Order Type'] == 'Alacarte') & (product_mix['OWNFR'] == 'FR')].copy()
    fr_alacarte_quantity.rename(columns={'Quantity': 'FR Alacarte Quantity', 'Sales': 'FR Alacarte Sales'}, inplace=True)

    total_alacarte_quantity = product_mix.loc[product_mix['Order Type'] == 'Alacarte'].copy()
    total_alacarte_quantity.rename(columns={'Quantity': 'Total Alacarte Quantity', 'Sales': 'Total Alacarte Sales'}, inplace=True)

    own_package_quantity = product_mix.loc[(product_mix['Order Type'] == 'Package') & (product_mix['OWNFR'] == 'OWN')].copy()
    own_package_quantity.rename(columns={'Quantity': 'OWN Package Quantity', 'Sales': 'OWN Package Sales'}, inplace=True)

    fr_package_quantity = product_mix.loc[(product_mix['Order Type'] == 'Package') & (product_mix['OWNFR'] == 'FR')].copy()
    fr_package_quantity.rename(columns={'Quantity': 'FR Package Quantity', 'Sales': 'FR Package Sales'}, inplace=True)

    total_package_quantity = product_mix.loc[product_mix['Order Type'] == 'Package'].copy()
    total_package_quantity.rename(columns={'Quantity': 'Total Package Quantity', 'Sales': 'Total Package Sales'}, inplace=True)

    merged_df = pd.merge(product_mix, own_alacarte_quantity, on=['Product', 'Type', 'Category', 'Group', 'Detail', 'OWNFR', 'Order Type'], how='left')
    merged_df = pd.merge(merged_df, fr_alacarte_quantity, on=['Product', 'Type', 'Category', 'Group', 'Detail', 'OWNFR', 'Order Type'], how='left')
    merged_df = pd.merge(merged_df, total_alacarte_quantity, on=['Product', 'Type', 'Category', 'Group', 'Detail', 'OWNFR', 'Order Type'], how='left')
    merged_df = pd.merge(merged_df, own_package_quantity, on=['Product', 'Type', 'Category', 'Group', 'Detail', 'OWNFR', 'Order Type'], how='left')
    merged_df = pd.merge(merged_df, fr_package_quantity, on=['Product', 'Type', 'Category', 'Group', 'Detail', 'OWNFR', 'Order Type'], how='left')
    merged_df = pd.merge(merged_df, total_package_quantity, on=['Product', 'Type', 'Category', 'Group', 'Detail', 'OWNFR', 'Order Type'], how='left')

    merged_df.fillna(0, inplace=True)

  
    merged_df = merged_df[['Product', 'Type', 'Category', 'Group', 'Detail',
                           'OWN Alacarte Quantity', 'FR Alacarte Quantity', 'Total Alacarte Quantity',
                           'OWN Package Quantity', 'FR Package Quantity', 'Total Package Quantity',
                           'OWN Alacarte Sales', 'FR Alacarte Sales', 'Total Alacarte Sales',
                           'OWN Package Sales', 'FR Package Sales', 'Total Package Sales']]
    
    merged_df.info()

    merged_df = merged_df.groupby(['Product', 'Type', 'Category', 'Group', 'Detail']).sum().reset_index()


    output_folder = output_folder = os.path.join(os.getcwd(),   'Output',f'{year}',f'{month:02d}' ,'Product Mix')
    os.makedirs(output_folder, exist_ok=True)  
    excel_path = os.path.join(output_folder, 'product_mix.xlsx')

    merged_df.to_excel(excel_path, index=False)
    print(f"Product Mix exported to '{excel_path}'.")

    return merged_df

def generate_store_mix_excel(modifiedData):

    product_mix = modifiedData.groupby(['Product', 'Store','Type', 'Category', 'Group', 'Detail', 'OWNFR', 'Order Type']).agg({
        'Quantity': 'sum',
        'Sales': 'sum'
    }).reset_index()

    pivot_table = product_mix.pivot_table(index=['Product','Order Type','Type', 'Category', 'Group', 'Detail'], columns='Store', values=['Quantity', 'Sales'], fill_value=0)

    pivot_table.columns = [f'{col[0]}_{col[1]}' for col in pivot_table.columns]

    pivot_table.reset_index(inplace=True)

    output_folder = output_folder = os.path.join(os.getcwd(),   'Output',f'{year}',f'{month:02d}' ,'Product Mix')
    os.makedirs(output_folder, exist_ok=True) 
    excel_path = os.path.join(output_folder, 'Store_mix.xlsx')

    pivot_table.to_excel(excel_path, index=False)

    print(f"Store Mix exported to '{excel_path}'.")

class ProductSale:
    def __init__(self, name, type, category, group, detail, quantity, sales) -> None:
        self.name = name
        self.type = type
        self.category = category
        self.group = group
        self.detail = detail
        self.quantity = quantity
        self.sales = sales
    
    def print_details(self):
        print("name: " + self.name)
        print("tpye: " + self.type)
        print("category: " + self.category)
        print("group: " + self.group)
        print("detail: " + self.detail)
        print("quantity: " + str(self.quantity))
        print("total_price: " + str(self.sales))
           
def initialize_check(row):
    check = {
        'Visitor': 0,
        'Beverage': 0,
        'Starter': 0,
        'Other': 0,
        'Soup': 0,
        'Dessert': 0,
        'Hot Drink': 0,
        'Sales': 0,
        'Hour': row.Hour,
        'Date': row.Date,
        'OWNFR': row.OWNFR,
        'Order Type': row['Order Type'],
        'Store': row.Store,
        'Region': row.Region,
        'Product': defaultdict(lambda: ProductSale("", "", "", "", "", 0, 0))
    }
    return check

def update_check_details(check, row):
    salesType = row.Type
    quantity = row.Quantity
    sales = row.Sales
    product = row.Product
    category = row.Category
    group = row.Group
    detail = row.Detail
    

    if salesType in ['Beverage', 'Starter', 'Other', 'Soup', 'Visitor', 'Dessert', 'Hot Drink']:
        check[salesType] += quantity
    check['Sales'] += sales
    check['Product'][product].name = product
    check['Product'][product].type = salesType
    check['Product'][product].category = category
    check['Product'][product].group = group
    check['Product'][product].detail = detail
    check['Product'][product].quantity += quantity
    check['Product'][product].sales += sales

def classify_checks(modifiedData):
    modifiedData = modifiedData.sort_values('Check')
    check_details = {}

    for i, row in modifiedData.iterrows():
        currentCheckNumb = row.Check

        if pd.notnull(currentCheckNumb):
            if currentCheckNumb not in check_details:
                check_details[currentCheckNumb] = initialize_check(row)

            update_check_details(check_details[currentCheckNumb], row)
                
    return check_details


def create_store_summary(checks):
    store_summary = {}

    for check_id, check in checks.items():
        store = check['Store']
        order_type = check['Order Type']
        sales = check['Sales']
        visitors = check['Visitor']
        ownfr = check['OWNFR']
        date = check['Date']

        if store not in store_summary:
            store_summary[store] = {
                'Alacarte Sales': 0,
                'Package Sales': 0,
                'Alacarte Visitors': 0,
                'Package Visitors': 0,
                'OWN FR': ownfr,
                'Alacarte Days Open': set(),
                'Package Days Open': set(),
                'Alacarte Check Set': set(),
                'Package Check Set': set()
            }

        if order_type == 'Alacarte':
            store_summary[store]['Alacarte Sales'] += sales
            store_summary[store]['Alacarte Visitors'] += visitors
            store_summary[store]['Alacarte Days Open'].add(date)
            store_summary[store]['Alacarte Check Set'].add(check_id)
        elif order_type == 'Package':
            store_summary[store]['Package Sales'] += sales
            store_summary[store]['Package Visitors'] += visitors
            store_summary[store]['Package Days Open'].add(date)
            store_summary[store]['Package Check Set'].add(check_id)

    for store, summary in store_summary.items():
        summary['Alacarte Days Open'] = len(summary['Alacarte Days Open'])
        summary['Package Days Open'] = len(summary['Package Days Open'])

        summary['Alacarte Check Count'] = len(summary['Alacarte Check Set'])
        summary['Package Check Count'] = len(summary['Package Check Set'])

        del summary['Alacarte Check Set']
        del summary['Package Check Set']

    return store_summary

def prepare_dataframe(store_summary):
    df = pd.DataFrame(store_summary).transpose()
    df = df.reset_index()
    df = df.rename(columns={'index': 'Stores'})
    return df

def add_columns(df):
    df['Total Check Count'] = df['Alacarte Check Count'] + df['Package Check Count']
    df['Total Sales'] = df['Alacarte Sales'] + df['Package Sales']
    df['Total Visitors'] = df['Alacarte Visitors'] + df['Package Visitors']

    return df

def order_columns(df):
    df = df[['OWN FR','Alacarte Days Open', 'Package Days Open' ,'Stores','Daily Alacarte Visitor','Daily Package Visitor', 'Daily Visitors','Alacarte Sales','Package Sales','Total Sales','Alacarte Visitors', 'Package Visitors','Total Visitors','Alacarte Check Count','Package Check Count',   'Total Check Count',   'Alacarte Visitors / Check Count', 'Package Visitors / Check Count', 'Total Visitors / Check Count']]
    return df

def convert_to_dataframe(store_summary):
    df = prepare_dataframe(store_summary)
    df = add_columns(df)
    df = calculate_additional_metrics(df) 
    df = order_columns(df) 
    return df

def calculate_additional_metrics(df):
    # Replace 0 with np.nan before division
    df['Package Days Open'] = df['Package Days Open'].replace(0, np.nan)
    df['Alacarte Days Open'] = df['Alacarte Days Open'].replace(0, np.nan)
    df['Alacarte Check Count'] = df['Alacarte Check Count'].replace(0, np.nan)
    df['Package Check Count'] = df['Package Check Count'].replace(0, np.nan)
    df['Total Check Count'] = df['Total Check Count'].replace(0, np.nan)

    # Daily Visitor calculations
    df['Daily Alacarte Visitor'] = df['Alacarte Visitors'] / df['Alacarte Days Open']
    df['Daily Package Visitor'] = df['Package Visitors'] / df['Package Days Open']
    df['Daily Visitors'] = df['Total Visitors'] / df['Alacarte Days Open']

    # Visitors per Check calculations
    df['Alacarte Visitors / Check Count'] = df['Alacarte Visitors'] / df['Alacarte Check Count']
    df['Package Visitors / Check Count'] = df['Package Visitors'] / df['Package Check Count']
    df['Total Visitors / Check Count'] = df['Total Visitors'] / df['Total Check Count']

    # Replace NaN values in these columns with 0
    df[['Daily Alacarte Visitor',
        'Alacarte Days Open', 
        'Package Days Open',
        'Alacarte Check Count',
        'Package Check Count',
        'Daily Package Visitor', 
        'Alacarte Visitors / Check Count',
        'Package Visitors / Check Count',
        'Total Visitors / Check Count']] = df[['Daily Alacarte Visitor', 
                                               'Alacarte Days Open',
                                               'Package Days Open',
                                               'Alacarte Check Count',
                                                'Package Check Count',
                                               'Daily Package Visitor', 
                                               'Alacarte Visitors / Check Count',
                                               'Package Visitors / Check Count',
                                               'Total Visitors / Check Count']].fillna(0)
    

    return df

def export_to_excel(df):

    output_folder = os.path.join(os.getcwd(),  'Output',f'{year}',f'{month:02d}' ,'System Sales')
    os.makedirs(output_folder, exist_ok=True) 
    excel_path = os.path.join(output_folder, 'store_summary.xlsx')

    df.to_excel(excel_path, index=False)
    print(f"Store summary exported to '{excel_path}'.")

def summarize_by_store(checks):
    store_summary = create_store_summary(checks)
    df = convert_to_dataframe(store_summary)
    export_to_excel(df)

def calculate_percentage_of_beverage_with_soup(df, visitor_count):
    total_soup = 0
    soup_and_beverage = 0

    for _, row in df.iterrows():
        if row['Visitor'] == visitor_count and row['Soup'] > 0:
            total_soup += 1
            if row['Beverage'] > 0:
                soup_and_beverage += 1

    if total_soup > 0:
        percentage = (soup_and_beverage / total_soup) * 100
    else:
        percentage = 0

    return percentage

def create_visitor_summary(df):
    visitor_summary = {
        'VisitorCount': ['0 Visitors', '1 Visitors', '2 Visitors', '3 Visitors', '4 Visitors', '4+ Visitors', 'Total'],
        'Visitors': [],
        'Beverage': [],
        'Starter': [],
        'Dessert': [],
        'Soup': [],
        'Hot Drink': [],
        'SoupBeveragePerc': []  
    }

    for i in range(5):
        visitors = df[df['Visitor'] == i]
        visitor_summary['Visitors'].append(visitors['Visitor'].sum())
        visitor_summary['Beverage'].append(visitors['Beverage'].sum())
        visitor_summary['Starter'].append(visitors['Starter'].sum())
        visitor_summary['Dessert'].append(visitors['Dessert'].sum())
        visitor_summary['Soup'].append(visitors['Soup'].sum())
        visitor_summary['Hot Drink'].append(visitors['Hot Drink'].sum())
        visitor_summary['SoupBeveragePerc'].append(calculate_percentage_of_beverage_with_soup(df, i))

    visitors = df[df['Visitor'] > 4]
    visitor_summary['Visitors'].append(visitors['Visitor'].sum())
    visitor_summary['Beverage'].append(visitors['Beverage'].sum())
    visitor_summary['Starter'].append(visitors['Starter'].sum())
    visitor_summary['Dessert'].append(visitors['Dessert'].sum())
    visitor_summary['Soup'].append(visitors['Soup'].sum())
    visitor_summary['Hot Drink'].append(visitors['Hot Drink'].sum())
    visitor_summary['SoupBeveragePerc'].append(calculate_percentage_of_beverage_with_soup(df, 5))

    visitor_summary['Visitors'].append(df['Visitor'].sum())
    visitor_summary['Beverage'].append(df['Beverage'].sum())
    visitor_summary['Starter'].append(df['Starter'].sum())
    visitor_summary['Dessert'].append(df['Dessert'].sum())
    visitor_summary['Soup'].append(df['Soup'].sum())
    visitor_summary['Hot Drink'].append(df['Hot Drink'].sum())
    visitor_summary['SoupBeveragePerc'].append(calculate_percentage_of_beverage_with_soup(df, 'Total'))

    visitor_summary_df = pd.DataFrame(visitor_summary)
    
    output_folder = output_folder = os.path.join(os.getcwd(),   'Output',f'{year}',f'{month:02d}' ,'Visitor Summary')
    os.makedirs(output_folder, exist_ok=True) 
    excel_path = os.path.join(output_folder, 'visitor_summary.xlsx')

    visitor_summary_df.to_excel(excel_path, index=False)
    print(f"Visitor Summary exported to '{excel_path}'.")

    return visitor_summary_df


def calculate_hourly_visitor(df):

    df['Date'] = pd.to_datetime(df['Date'])
    df['DayOfWeek'] = df['Date'].dt.dayofweek 
    
    day_groups = {'Weekday': [0, 1, 2, 3],  
                  'Friday': [4],            
                  'Weekend': [5, 6]}         
    

    output_dir = os.path.join(os.getcwd(),  'Output',f'{year}',f'{month:02d}' ,'Hourly Visitors')
    os.makedirs(output_dir, exist_ok=True)
    
    for group in day_groups:
        group_data = df[df['DayOfWeek'].isin(day_groups[group])]
        
        for order_type in group_data['Order Type'].unique():
            type_data = group_data[group_data['Order Type'] == order_type]
            
            type_visitor = type_data.groupby('Hour')['Visitor'].sum().reset_index()
            
            filename = f"{order_type}_{group}_hourly_visitor.csv"
            filepath = os.path.join(output_dir, filename)
            
            type_visitor.to_csv(filepath, index=False)
            
            print(f"Saved {filepath}")
            
        total_visitor = group_data.groupby('Hour')['Visitor'].sum().reset_index()
        
        filename = f"Total_{group}_hourly_visitor.csv"
        filepath = os.path.join(output_dir, filename)
        
        total_visitor.to_csv(filepath, index=False)
        
        print(f"Saved {filepath}")



total_time = timedelta()

month = 6
year = 2023
mod = 'data'

if mod == 'sql':
    start_time = time.time()
    df = get_sales_from_db(year,month)
    df.info()
    elapsed_time = time.time() - start_time
    total_time += timedelta(seconds=elapsed_time)
    print(f"get sales data took {elapsed_time} seconds")

    start_time = time.time()
    data = group_dataframe(df)
    data.info()
    elapsed_time = time.time() - start_time
    total_time += timedelta(seconds=elapsed_time)
    print(f"process_dataframe took {elapsed_time} seconds")

else:
    
    start_time = time.time()

    df =  get_sales_from_csv()
    data = group_dataframe(df)
    data.info()
    elapsed_time = time.time() - start_time
    total_time += timedelta(seconds=elapsed_time)
    print(f"process_dataframe took {elapsed_time} seconds")

start_time = time.time()
replacement, products, ownFr = get_maps()
elapsed_time = time.time() - start_time
total_time += timedelta(seconds=elapsed_time)
print(f"get_maps took {elapsed_time} seconds")

start_time = time.time()
modifiedData = modifyData(data, replacement, products, ownFr)
elapsed_time = time.time() - start_time
total_time += timedelta(seconds=elapsed_time)
print(f"modify_data_with_map took {elapsed_time} seconds")

modifiedData.info()

start_time = time.time()
product_mix_df = get_product_mix(modifiedData)
store_mix_df = generate_store_mix_excel(modifiedData)
elapsed_time = time.time() - start_time
total_time += timedelta(seconds=elapsed_time)
print(f"get product mix took {elapsed_time} seconds")

start_time = time.time()
checks = classify_checks(modifiedData)
elapsed_time = time.time() - start_time
total_time += timedelta(seconds=elapsed_time)
print(f"classify_checks took {elapsed_time} seconds")

start_time = time.time()
store_summary = summarize_by_store(checks)
elapsed_time = time.time() - start_time
total_time += timedelta(seconds=elapsed_time)
print(f"summarize_by_store took {elapsed_time} seconds")

start_time = time.time()
df = pd.DataFrame.from_dict(checks, orient='index')
elapsed_time = time.time() - start_time
total_time += timedelta(seconds=elapsed_time)
print(f"checks to df took {elapsed_time} seconds")

start_time = time.time()
visitor_summary_df = create_visitor_summary(df)
elapsed_time = time.time() - start_time
total_time += timedelta(seconds=elapsed_time)
print(f"visitor_summary took {elapsed_time} seconds")

start_time = time.time()
df_check_details = pd.DataFrame.from_dict(checks, orient='index')
calculate_hourly_visitor(df_check_details)
elapsed_time = time.time() - start_time
total_time += timedelta(seconds=elapsed_time)
print(f"calculate hourly visitors took {elapsed_time} seconds")


print(f"Total time: {total_time}")




