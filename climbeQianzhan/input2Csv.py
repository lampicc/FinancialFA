import os
import csv

base_type_list = ['沪市A股', '深市A股', 'A+B股', 'A+H股', '中小股', '创业板']
# base_type_list = ['沪市A股']
base_dir_list = [os.path.join('D:\\QIANZHAN', file_path) for file_path in base_type_list]

# 输出文件路径
output_file = 'questioncsvcom.csv'

# 用于存储不同字符串和文件名的字典
data = [['company', 'table']]
i = 0
for item_dir in base_dir_list:
    # 遍历根目录下的所有子目录和文件
    for subdir, dirs, files in os.walk(item_dir):

        for file in files:

            if file.endswith('tabulate.txt'):
                # 构建文件的完整路径
                file_path = os.path.join(subdir, file)

                # 读取txt文件的第一列到pandas Series中
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        content = file.read()
                    data.append([subdir.split("\\")[-1], content])
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")
                    continue  # 跳过当前文件并继续处理其他文件

# 将结果写入到另一个csv文件中
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    for row in data:
        writer.writerow(row)

print(f"Done. Strings have been written to {output_file}.")
