from bs4 import BeautifulSoup


def parse_html_from_file(file_path):
    # 从指定路径读取 HTML 内容
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()

    # 创建 BeautifulSoup 对象
    soup = BeautifulSoup(html_content, 'html.parser')

    # 查找所有 class 为 "c-result result" 的 div
    divs = soup.find_all('div', class_='c-result result')

    # 初始化一个列表来存储符合条件的文本内容
    result_texts = []

    # 遍历找到的 div 元素
    for div in divs:
        # 检查是否包含 <span>官方</span>
        if div.find('span', string='官方'):
            # 在包含“官方”的 div 中查找 csv_data-module 为 "lgsoe" 的子 div
            target_div = div.find('div', {'csv_data-module': 'lgsoe'})


            if target_div:
                url_div = target_div.find('div', class_='single-text')
                if url_div:
                    result_texts.append(url_div.get_text(strip=True))
    return result_texts


# 示例调用
file_path = r'/events/baidu/input/html/baidu.html'
result = parse_html_from_file(file_path)
print(result)
