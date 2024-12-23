from time import sleep

import pandas as pd
import requests

MAX_RETRIES = 1
TIMEOUT = 30  # 设置请求超时为10秒

def ensure_http_prefix(url):
    """检查URL是否包含http前缀，若没有则添加http://"""
    if not url.startswith(('http://', 'https://')):
        return 'http://' + url
    return url

def get_company_name_from_url(url):
    url = ensure_http_prefix(url)
    retries = 0

    payload = {}
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Connection': 'keep-alive',
        'Cookie': 'JSESSIONID=151E5E899FE252F56739D3D0058CCACB; wkxt3_csrf_token=321153cc-7baa-4e1e-8e02-5f46c6303e65; JSESSIONID=976F4C524B511030AC3CA5E522810173',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
    }



    while retries < MAX_RETRIES:
        try:
            # 发送请求并获取页面内容
            response = requests.request("GET", url, headers=headers, data=payload, timeout=TIMEOUT)
            response.raise_for_status()  # 如果响应码不是 200，会抛出异常
            content = response.text

            # 匹配规则
            if "玛格泰克" in content:
                return "北京玛格泰克科技发展有限公司"
            elif "北京勤云科技" in content:
                return "北京勤云科技发展有限公司"
            elif "北京仁和汇智" in content:
                return "北京仁和汇智信息技术有限公司"
            elif "本系统由中国知网提供技术支持" in content or "本网站由中国知网维护" in content:
                return "中国知网"
            elif "威海圣为" in content:
                return "威海圣为电子科技有限公司"
            elif "西安三才" in content:
                return "西安三才科技实业有限公司"
            else:
                return ""

        except requests.Timeout:
            print(f"Request to {url} timed out. Retrying... ({retries + 1}/{MAX_RETRIES})")
        except requests.RequestException as e:
            # 捕获网络请求错误
            print(f"Error fetching {url}: {e}. Retrying... ({retries + 1}/{MAX_RETRIES})")

        # 增加重试前的等待时间
        retries += 1
        sleep(2)  # 每次重试间隔2秒

    # 如果重试仍然失败，则返回空字符串
    print(f"Failed to fetch {url} after {MAX_RETRIES} attempts.")
    return ""


def get_company_name_from_sld(url):
    """用于根据北大方正给出的接口验证是否为该公司设计的网址"""
    # 确保网址以https://开头
    if not url.startswith('http://') and not url.startswith('https://'):
        url = 'https://' + url  # 默认为https
    elif url.startswith('http://'):
        url = 'https://' + url[7:]  # 如果是http，改为https
    sld = url.split('.')[0].replace('https://', '')
    # 构建请求头
    payload = {}
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Connection': 'keep-alive',
        'Referer': url,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'Cookie': 'acw_tc=3ccdc15c17314871549987875e139234a60d5ca029c2f6a8bc2aa158370fb8'
    }

    # 构建API请求的URL
    api_url = f"http://www.zaihaixue.com/rc-pub/front/site/findBySld?sld={sld}"

    try:
        # 发送GET请求
        response = requests.request("GET", api_url, headers=headers, data=payload, timeout=TIMEOUT)
        response.raise_for_status()  # 如果响应码不是 200，会抛出异常

        # 解析JSON响应
        json_data = response.json()

        # 检查返回的data中的id和subId字段
        data = json_data.get("csv_data", {})
        if data:
            if data.get("id") != 6 and data.get("subId") != "0":
                return "北京北大方正电子有限公司"
            else:
                return ""  # 如果条件不满足，返回空字符串
        else:
            return ""

    except requests.Timeout:
        print(f"Request to {url} timed out.")
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")

    return ""  # 如果出现任何异常，返回空字符串


def process_csv_with_sld(input_file, output_file):
    # 读取CSV文件
    df = pd.read_csv(input_file)

    # 检查是否包含 officialWebsite 列
    if 'officialWebsite' not in df.columns:
        print("No 'officialWebsite' column found!")
        return

    # 创建新列 'companyName' 以存放匹配到的公司名
    df['companyName'] = df['officialWebsite'].apply(get_company_name_from_sld)

    # 只在有匹配的情况下添加公司名
    df = df[df['companyName'] != ""]

    # 保存结果到新CSV文件
    df.to_csv(output_file, index=False)
    print(f"Processed CSV saved to {output_file}")


def process_csv(input_file, output_file):
    # 读取CSV文件
    df = pd.read_csv(input_file)

    # 检查是否包含 officialWebsite 列
    if 'officialWebsite' not in df.columns:
        print("No 'officialWebsite' column found!")
        return

    # 创建新列 'companyName' 以存放匹配到的公司名
    df['companyName'] = df['officialWebsite'].apply(get_company_name_from_url)

    # 只在有匹配的情况下添加公司名
    df = df[df['companyName'] != ""]

    # 保存结果到新CSV文件
    df.to_csv(output_file, index=False)
    print(f"Processed CSV saved to {output_file}")


def compare_and_extract(input_file, result_file, unmatched_output_file):
    # 读取原始输入表
    df_input = pd.read_csv(input_file)

    # 读取结果表（已经包含公司名称的表）
    df_result = pd.read_csv(result_file)

    # 对比输入表和结果表，找出没有匹配到的记录
    unmatched_df = df_input[~df_input['nameOfTheJournal'].isin(df_result['nameOfTheJournal'])]

    # 保存未匹配的数据到新的CSV文件
    unmatched_df.to_csv(unmatched_output_file, index=False)
    print(f"Unmatched csv_data saved to {unmatched_output_file}")


if __name__ == "__main__":
    # 示例调用
    process_csv_with_sld(r'/events/journalClassification/input/raw_data_v3.csv_data',
                         r'/events/journalClassification\output\output_v4.csv_data')
    # process_csv_with_sld(r'D:\pyProject\hzcx\journalClassification\input\raw_data_v2.csv_data', r'D:\pyProject\hzcx\journalClassification\output\output_v2.csv_data')
    # compare_and_extract(r'D:\pyProject\hzcx\journalClassification\input\raw_data.csv_data', r'D:\pyProject\hzcx\journalClassification\output\output_v3.csv_data', r"D:\pyProject\hzcx\journalClassification\input\raw_data_v3.csv_data")
