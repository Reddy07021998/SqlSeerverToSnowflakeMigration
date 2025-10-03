import pyodbc

# Connection details
server = 'localhost'       # or '127.0.0.1'
database = 'KKRENTAL'     # your database name
username = 'KK_Admin'            # your SQL Server username
password = 'Kk07022024!@'  # your SQL Server password
driver = 'ODBC Driver 17 for SQL Server'  # make sure you have ODBC driver installed

# Connect to SQL Server
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_str)

cursor = conn.cursor()

# Test query
cursor.execute("SELECT GETDATE() AS CurrentDateTime;")
row = cursor.fetchone()
print("Connection Successful! Current SQL Server date/time:", row.CurrentDateTime)

# List tables in the database
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';")
tables = cursor.fetchall()
print("Tables in database:", [t[0] for t in tables])

# Close connection
cursor.close()
conn.close()


import pyodbc
print(pyodbc.drivers())


# KK_Admin WITH PASSWORD = 'Kk07022024!@'