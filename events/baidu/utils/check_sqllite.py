import sqlite3
import pandas as pd

# 创建数据库连接
conn = sqlite3.connect('your_database.db')
cursor = conn.cursor()

# 读取 CSV 数据
df = pd.read_csv(r'/events/baidu/input/csv/search_seeds.csv')

# 创建表
cursor.execute('''CREATE TABLE IF NOT EXISTS table_name (col1 TEXT, col2 INTEGER, ...)''')

# 插入数据
for _, row in df.iterrows():
    cursor.execute("INSERT INTO table_name (col1, col2, ...) VALUES (?, ?, ...)", tuple(row))

# 提交并关闭
conn.commit()
conn.close()

