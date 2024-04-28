import json

from bs4 import BeautifulSoup
import time
import os
import requests
import redis
import math
import pandas as pd
import warnings

warnings.filterwarnings('ignore')
redis_cli = redis.Redis(host='192.168.0.122', port=6379, password="")
REDIS_KEYWORD = "前瞻眼"


def get_total():
    result_rul_dic = {}
    # result_rul_dic['沪市A股'] = 'hssc01'
    # result_rul_dic['深市A股'] = 'hssc02'
    # result_rul_dic['A+B股'] = 'hssc06'
    # result_rul_dic['A+H股'] = 'hssc07'
    result_rul_dic['中小股'] = 'hssc08'
    # result_rul_dic['创业板'] = 'hssc09'

    return result_rul_dic


def get_basic_url(value, page_index):
    # page_index=3
    query_rul = f'https://stock.qianzhan.com/hs/selectListedCompanies?fl_code={value}&page={page_index}&ctrl=hs'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }
    tunnel = "n175.kdltps.com:15818"
    username = ""
    password = ""
    proxies = {
        "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
        "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
    }

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'page start down', query_rul)
    res = requests.get(query_rul, proxies=proxies)
    # res = requests.get(query_rul, headers=headers)
    if res.status_code != 200:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'page fail down', query_rul, res.status_code)

    data = res.content.decode(res.apparent_encoding)

    return BeautifulSoup(data, 'html.parser')


def get_dataInfo_url(key, value):
    bs = get_basic_url(value, 1)

    dataNum = bs.find_all('span')
    # if len(dataNum):
    spanvalue = dataNum[0].text
    # print(spanvalue)
    pageNum = int(math.ceil(int(spanvalue) / 200))

    for page_index in range(1, pageNum + 1):
        bs = get_basic_url(value, page_index)
        for query_info in bs.find_all('a'):
            if 'href' in query_info.attrs and query_info['href'].startswith('/hs/gongsixinxi'):
                get_html(key, query_info['href'].replace('/hs/gongsixinxi_', "").replace('.html', ""))
            if bs.find('a', attrs={'class': 'notcur'}, text='下一页') and not bs.find('a', text='下一页'):
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '共获取', page_index)


def getStockInfo(query_type, url):
    # query_url = f'https://stock.qianzhan.com/hs/{query_type}{url}.html'
    query_url = f'https://stock.qianzhan.com/hs/{query_type.replace("_", "")}/{url}'
    # https: // stock.qianzhan.com / hs / zichanfuzhai / 603133.SH
    # 'https://stock.qianzhan.com/hs.caiwufenxi/603133.SH'

    # https: // stock.qianzhan.com / hs / zichanfuzhai_603133.SH.html

    payload = "range=5&baogaoqi=4,0&leixing=1&konghang="
    headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "Cookie": "qznewsite.uid=rumvsu3yq1qnerhqzfryda53; qz.newsite=D26CBFA7F8364591F3AFE54F7CA2BD23C998337C9943521ED3507EAB74AE6AB872EAE16C2079446C07802BADCDA2D1B6C1BA9DEABBC3E721574D7FB6D8C9D7B7CBF63FCAA9DF3015789A4930C2B1144938A206E43E5FB9CA70649650250A07B0B7BC30D5AF020CB0138886EE26EDDE0FFCC42E111B3A9CF7ABCA6F6EC5C959D8037FBF95; Hm_lvt_044fec3d5895611425b9021698c201b1=1709260131,1709278871,1709522919,1709776001; Hm_lpvt_044fec3d5895611425b9021698c201b1=1709776028",
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }
    tunnel = "n175.kdltps.com:15818"
    username = ""
    password = ""
    proxies = {
        "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
        "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
    }
    res = requests.request("POST", query_url, data=payload, headers=headers, proxies=proxies)
    # res = requests.get(query_url, data=payload, headers=headers, proxies=proxies)
    # res = requests.get(query_url, headers=headers)

    if res.status_code != 200:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'page fail down', query_url, res.status_code)
        return None
    return res.content.decode('utf-8', errors='ignore')


def get_html(key, url):
    try:
        file_path = os.path.join("D:\\QIANZHAN\\", key, url)
        type_lst = ['zichanfuzhai_', 'lirun_', 'xianjinliuliang_', 'caiwufenxi_']
        # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'page start down', key, url)
        for query_type in type_lst:
            data = getStockInfo(query_type, url)
            soup = BeautifulSoup(data, 'html.parser')
            table = soup.find('table')
            header_row = table.find('thead').find('tr')
            header_cells = [cell.text for cell in header_row.find_all(['th', 'td'])]
            df_list = pd.read_html(str(table))
            two_dim_array = [df.values.tolist() for df in df_list]
            file_name = f'{url}_{query_type}.txt'
            # print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),'page start down',query_url,file_name)
            # if os.path.exists(os.path.join(file_path, file_name)):
            #     continue
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            with open(os.path.join(file_path, file_name), 'w', encoding='utf-8') as f:
                f.write(','.join(map(str, header_cells)) + '\n')
                for row in two_dim_array[0]:
                    row_str = ','.join(map(str, row))
                    f.write(row_str + '\n')

            # redis_cli.sadd(REDIS_KEYWORD, os.path.join(file_path, file_name) + '\n' + url)

    except Exception as e:
        print(f'{e}')
        print("获取失败", url)
        time.sleep(1)
        return False


if __name__ == '__main__':
    result_rul_dic = get_total()
    for key in result_rul_dic.keys():
        get_dataInfo_url(key, result_rul_dic[key])
