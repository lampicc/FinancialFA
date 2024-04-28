import os
import pandas as pd
import numpy as np
import warnings
from dataCalculate import getInfoByDataFrame
import time
import traceback
from prettytable import PrettyTable
from tabulate import tabulate

warnings.filterwarnings('ignore')

base_type_list = ['沪市A股', '深市A股', 'A+B股', 'A+H股', '中小股', '创业板']
# base_type_list = ['沪市A股']
base_dir_list = [os.path.join('D:\\QIANZHAN', file_path) for file_path in base_type_list]
# print(base_dir_list)
type_lst = ['_zichanfuzhai_', '_lirun_', '_xianjinliuliang_', '_caiwufenxi_']
# 应用一个函数来格式化每个元素，使其具有固定长度
fixed_length = 15


def format_element(element):
    fill_char = ' '
    return str(element).ljust(fixed_length, fill_char)


def data_to_string(base_dir):
    print("读取的文件类型路径：", base_dir)
    #   base_dir=base_dir_list[0]
    entries = os.listdir(base_dir)
    # 通过检查每个条目是否是目录来过滤出子目录名
    subdir = [entry for entry in entries if os.path.isdir(os.path.join(base_dir, entry))]
    # subdir = ['603133.SH']
    for file in subdir:
        try:
            # print(file)
            flag, result_df = getInfoByDataFrame(base_dir, type_lst, file)
            if flag:
                result_df = result_df.rename(
                    columns={'2023年三季报': '2023年9月', '2022年年报': '2022年', '2021年年报': '2021年', '2020年年报': '2020年'})
                result_df = result_df.reindex(columns=result_df.columns[::-1])  # 翻转列名
                maxlen = result_df.index.str.len().max()
                new_row_data = {'2020年': '-----------', '2021年': '-----------', '2022年': '-----------',
                                '2023年9月': '-----------'}
                new_index = '-'.ljust(maxlen - 2, '-')  # 新行的行名（索引）,-2是因为后续会增加'| '
                new_row_df = pd.DataFrame([new_row_data], index=[new_index])
                result_df = pd.concat([new_row_df, result_df.iloc[0:]], ignore_index=False)
                result_df = result_df.rename(index=lambda x: '' + x + ' ')
                result_df = result_df.rename(index=lambda x: '|' + x + '')
                result_df = result_df.replace([0.0, -0.0], '')
                result_df = result_df.astype(str)
                # 格式化列名
                formatted_columns = [col.ljust(fixed_length) for col in result_df.columns]
                result_df.columns = formatted_columns
                # 格式化行索引
                formatted_index = [str(idx).ljust(maxlen) for idx in result_df.index]
                result_df.index = formatted_index
                result_df = result_df.applymap(format_element)
                csv_string = result_df.to_csv(index=True, header=True, sep='|')
                result_string = "|主要财务指标(万元)".ljust(maxlen, ' ') + csv_string
                result_string = result_string.replace('|', '| ').replace("\r", "|\r").replace('"', '')
                # lines = result_string.split('\n')
                # row_lengths = [len(line) for line in lines]
                # print(row_lengths)
                # print(result_string)
                file_name = f'{file}.txt'
                with open(os.path.join(base_dir, file, file_name), 'w', encoding='UTF-8') as f:
                    f.write(result_string)
                # print("成功处理了：", file)
        except Exception as e:
            print(f'{e}')
            traceback.print_exc()
            print("获取失败", base_dir + '\\' + file)
            time.sleep(1)


def data_to_tabulate(base_dir):
    print("读取的文件类型路径：", base_dir)
    #   base_dir=base_dir_list[0]
    entries = os.listdir(base_dir)
    # 通过检查每个条目是否是目录来过滤出子目录名
    subdir = [entry for entry in entries if os.path.isdir(os.path.join(base_dir, entry))]
    # subdir = ['600004.SH']
    for file in subdir:
        try:
            print(file)
            flag, result_df = getInfoByDataFrame(base_dir, type_lst, file)
            if flag:
                result_df = result_df.rename(
                    columns={'2023年三季报': '2023年9月', '2022年年报': '2022年', '2021年年报': '2021年', '2020年年报': '2020年'})
                result_df = result_df.reindex(columns=result_df.columns[::-1])  # 翻转列名
                result_df = result_df.reset_index(names='主要财务指标（万元）')
                result_df = result_df.replace([0.0, -0.0, '0'], '')
                # 将 DataFrame 转换为 tabulate 格式
                table_str = tabulate(result_df, headers='keys', tablefmt='pretty', stralign="left", numalign="left",
                                     showindex=False)
                table_str = table_str.replace('+', '|')
                lines = table_str.strip().split('\n')
                lines = lines[1:-1]
                # row_lengths = [len(line) for line in lines]
                # print(row_lengths)
                table_str = '\n'.join(lines)
                # print(table_str)
                file_name = f'{file + "tabulate"}.txt'
                with open(os.path.join(base_dir, file, file_name), 'w', encoding='UTF-8') as f:
                    f.write(table_str)
        except Exception as e:
            print(f'{e}')
            traceback.print_exc()
            print("获取失败", base_dir + '\\' + file)
            time.sleep(1)


if __name__ == '__main__':
    for base_dir in base_dir_list:
        if os.path.exists(base_dir):
            data_to_tabulate(base_dir)
            # data_to_string(base_dir)
