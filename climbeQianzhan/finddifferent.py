import os
import pandas as pd

# 根目录路径
# root_dir = 'D:\\QIANZHAN\\沪市A股'

base_type_list = ['沪市A股', '深市A股', 'A+B股', 'A+H股', '中小股', '创业板']
# base_type_list = ['沪市A股']
base_dir_list = [os.path.join('D:\\QIANZHAN', file_path) for file_path in base_type_list]

# 输出文件路径
output_file = 'different_strings.txt'

# 用于存储不同字符串和文件名的字典
different_strings = {}
i = 0
for item_dir in base_dir_list:
    # 遍历根目录下的所有子目录和文件
    for subdir, dirs, files in os.walk(item_dir):

        for file in files:

            if file.endswith('.SH_zichanfuzhai_.txt'):
                # 构建文件的完整路径
                file_path = os.path.join(subdir, file)

                # 读取txt文件的第一列到pandas Series中
                try:
                    df = pd.read_csv(file_path, sep=',', header=None)  # 假设txt文件是以制表符分隔的，没有标题行
                    df_tmp = pd.read_csv(file_path, sep=',', header=0,index_col=0)
                    first_column = df.iloc[:, 1]

                    if '公司类型' in df_tmp.index.values and df_tmp.loc['公司类型', '2022年年报'] == '银行':
                        i=i + 1
                        print("遇到了金融机构的数据，不进行处理：", file, i)
                    else:
                        # 遍历第一列中的每个字符串
                        for index, string in first_column.items():
                            # 如果字符串不在字典中，则添加它
                            if string not in different_strings.keys():
                                different_strings[string] = []
                                # 将当前文件名添加到字符串的列表中
                                different_strings[string].append(file)
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
                    continue  # 跳过当前文件并继续处理其他文件

# 将结果写入到另一个txt文件中
with open(output_file, 'w') as f:
    for string, files in different_strings.items():
        for file in files:
            f.write(f"{string}\t{file}\n")

print(f"Done. Different strings and their filenames have been written to {output_file}.")
