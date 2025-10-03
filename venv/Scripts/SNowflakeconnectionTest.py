import snowflake.connector

conn = snowflake.connector.connect(
    user='KK_Admin',
    password='Kk07022024!@',
    account='JRAAHIX-KK25650',
    role='ACCOUNTADMIN',
    warehouse='COMPUTE_WH',
    database='KKRENTALS',
    schema='KKRENTALS'
)

try:
    cs = conn.cursor()
    cs.execute("SELECT CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
    print(cs.fetchone())

    # Example: list tables in the schema
    cs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'KKRENTALS' AND table_catalog = 'KKRENTALS'")
    for row in cs.fetchall():
        print(row)

finally:
    cs.close()
    conn.close()
