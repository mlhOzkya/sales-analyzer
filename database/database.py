import pyodbc
import pandas as pd
import os

def get_sales_from_db(year,month):
   
    server = '****'
    database = '***'
    driver = '***'

    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};Trusted_connection=yes"

    conn = pyodbc.connect(conn_str)

    if month == 12:
        next_year = year + 1
        next_month = 1
    else:
        next_year = year
        next_month = month +1
    
    # Define the dates
    date1 = f"{year}-{month:02d}-01"
    date2 = f"{next_year}-{next_month:02d}-02"

    query = f"""
DECLARE @date1 AS DATETIME;
DECLARE @date2 AS DATETIME;

SET @date1 = '{date1}';
SET @date2 = '{date2}';



SELECT 
	br.BranchName AS [Store],
	CONVERT(VARCHAR, DATEADD( DAY, DATEDIFF( dd, 0, DATEADD( HOUR, -6, h.OrderDateTime )), 0 ), 23) AS [Date],
     DATEPART(HOUR, h.OrderDateTime) AS [Hour],
     t.OrderKey AS [Check],
	(
	CASE ISNULL(h.OrderType, 0)
		WHEN 1 THEN 'Alacarte'
		WHEN 2 THEN 'Alacarte'
		WHEN 3 THEN 'Alacarte'
		WHEN 4 THEN 'Alacarte'
		WHEN 5 THEN 'Package'
		ELSE 'Undefined'
	END
	) AS [Order Type],
	t.MenuItemText AS [Product],
	t.Quantity AS [Quantity],
	(t.ExtendedPrice * ISNULL((h.AmountDue / NULLIF(h.SubTotal,0)), 0)) AS [Sales]
FROM
	[***].[***].[dbo].OrderTransactions t WITH (NOLOCK)
	INNER JOIN [***].[***].[dbo].OrderHeaders h WITH (NOLOCK) ON h.OrderKey=t.OrderKey
	INNER JOIN [***].[***].[dbo].efr_Branchs br WITH (NOLOCK) ON br.BranchID = t.BranchID
WHERE
	1=1
	AND t.OrderDateTime BETWEEN @date1 AND @date2
	AND t.LineDeleted = 0
	AND h.LineDeleted = 0
   
    """

    cursor = conn.cursor()
    cursor.execute(query)
    
    columns = [column[0] for column in cursor.description]

    rows = cursor.fetchall()
    tr = pd.DataFrame.from_records(rows, columns=columns)

    if month == 12:
        next_year = year + 1
        next_month = 1
    else:
        next_year = year
        next_month = month +1

    date1 = f"{year}-{month:02d}-01"
    date2 = f"{next_year}-{next_month:02d}-02"


    query = f"""
DECLARE @date1 AS DATETIME;
DECLARE @date2 AS DATETIME;

SET @date1 = '{date1}';
SET @date2 = '{date2}';


SELECT 
	br.BranchName AS [Store],
	CONVERT(VARCHAR, DATEADD( DAY, DATEDIFF( dd, 0, DATEADD( HOUR, -6, h.OrderDateTime )), 0 ), 23) AS [Date],
     DATEPART(HOUR, h.OrderDateTime) AS [Hour],
     t.OrderKey AS [Check],
	(
	CASE ISNULL(h.OrderType, 0)
		WHEN 1 THEN 'Alacarte'
		WHEN 2 THEN 'Alacarte'
		WHEN 3 THEN 'Alacarte'
		WHEN 4 THEN 'Alacarte'
		WHEN 5 THEN 'Package'
		ELSE 'Undefined'
	END
	) AS [Order Type],
	t.MenuItemText AS [Product],
	t.Quantity AS [Quantity],
	(t.ExtendedPrice * ISNULL((h.AmountDue / NULLIF(h.SubTotal,0)), 0)) AS [Sales]
FROM
	[***].[***].[dbo].OrderTransactions t WITH (NOLOCK)
	INNER JOIN [***].[***].[dbo].OrderHeaders h WITH (NOLOCK) ON h.OrderKey=t.OrderKey
	INNER JOIN [***].[***].[dbo].efr_Branchs br WITH (NOLOCK) ON br.BranchID = t.BranchID
WHERE
	1=1	
	AND t.OrderDateTime BETWEEN @date1 AND @date2
	AND t.LineDeleted = 0
	AND h.LineDeleted = 0
   
    """

    cursor = conn.cursor()
    cursor.execute(query)
    
    columns = [column[0] for column in cursor.description]

    rows = cursor.fetchall()
    cr = pd.DataFrame.from_records(rows, columns=columns)
    
    data = pd.concat([cr, tr], ignore_index=True)

    conn.close()

    data['Sales'] = data['Sales'].astype(float)
    data['Quantity'] = data['Quantity'].astype(float)
    data['Date'] = pd.to_datetime(data['Date'], format='%Y-%m-%d')
    data = data[(data['Date'].dt.month == month) ]

    output_folder = output_folder = os.path.join(os.getcwd(),   'Output',f'{year}',f'{month:02d}' ,'Data')
    os.makedirs(output_folder, exist_ok=True)  
    csv_path = os.path.join(output_folder, 'data.csv')
    data.to_csv(csv_path, index=False)
    print(f"Data exported to '{csv_path}'.")

    return data
