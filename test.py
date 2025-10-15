import os, pyodbc, time

print("pyodbc version:", pyodbc.version)
print("Drivers:", [d for d in pyodbc.drivers()])

conn_str = os.getenv("AZURE_SQL_CONNECTION_STRING")
print("Testing connect...")

try:
    conn = pyodbc.connect(conn_str, timeout=5)
    print("✅ Connected successfully")
    cursor = conn.cursor()
    cursor.execute("SELECT GETDATE()")
    print("✅ Query ran:", cursor.fetchone())
    cursor.close()
    conn.close()
    print("✅ Closed successfully")
except Exception as e:
    print("❌ Exception:", e)

time.sleep(10)
