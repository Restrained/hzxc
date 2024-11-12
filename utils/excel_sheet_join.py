import pandas as pd


def read_excel_sheets_to_list(excel_path):
    # 定义一个列表来存储所有表格数据
    all_data = []

    # 从 Table 1 到 Table 32 逐个读取
    for i in range(1, 33):
        sheet_name = f'Table {i}'
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        all_data.append(df)  # 将每个 DataFrame 存入列表中

    return all_data


# 使用示例

# 使用示例
if __name__ == '__main__':

    read_excel_sheets_to_list(r'C:\Users\PY-01\Downloads\CSCD中国科学引文数据库来源期刊列表23-24 (1).xlsx')
