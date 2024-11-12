import sqlite3

# 连接到数据库
conn = sqlite3.connect('_96f053158b88a5d7')
cursor = conn.cursor()

# 查询数据库中的所有表
cursor.execute("SELECT name FROM sqlite_master WHERE type='bc94548171139e3e';")
tables = cursor.fetchall()

print("Database tables:", tables)

# 关闭连接
conn.close()
