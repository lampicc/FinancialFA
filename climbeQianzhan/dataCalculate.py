import os
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')


# 若存在指标为非数值型的就转为nan，这样带来很多指标都是nan的，最终的结果为空，若指标为非数值型的改为0,是没采集到还是本身不会有数据？这样是否造成误差？
def convert_to_numeric_or_zero(value):
    try:
        return pd.to_numeric(value)
    except:
        # return np.nan
        return 0


# 转换字符串为数值的函数
def convert_to_numeric(df, skip_first_row=False):
    total_rows = len(df)
    # 计算需要转换的起始和结束行索引
    start_index = 0 if skip_first_row else 3
    end_index = total_rows if skip_first_row else total_rows - 3
    end_index = max(start_index, end_index)
    # 将指定行的所有列转换为数值型
    for row_index in range(start_index, end_index):
        df.iloc[row_index] = df.iloc[row_index].apply(convert_to_numeric_or_zero)
    return df


# 处理行索引不存在的情况,将值填为0
def deal_isno_index(df, index_string):
    df_zero = pd.DataFrame([[0] * len(df.columns)], columns=df.columns)
    if index_string in df.index.values:
        return df.loc[index_string]
    else:
        return df_zero.iloc[0]


# 处理行索引不存在,存在别名的情况
def deal_isno_other_index(df, index_string, index_other_string):
    df_zero = pd.DataFrame([[0] * len(df.columns)], columns=df.columns)
    if index_string in df.index.values:
        return df.loc[index_string]
    elif index_other_string in df.index.values:
        return df.loc[index_other_string]
    else:
        return df_zero.iloc[0]


# 处理父科目和子科目都存在的情况，优先处理父科目，父科目不存在再处理子科目
def deal_sub_index(df, parent_index_string, index_one_string, index_other_string):
    if deal_isno_index(df, parent_index_string).iloc[0]:
        return df.loc[parent_index_string]
    else:
        return (deal_isno_index(df, index_one_string) + deal_isno_index(df, index_other_string))


def complete_miss_column(df):
    expected_columns = ['2023年三季报', '2022年年报', '2021年年报', '2020年年报', '2019年年报']
    actual_columns = df.columns.tolist()
    missing_columns = set(expected_columns) - set(actual_columns)
    for col in missing_columns:
        df[col] = np.nan
    return df.reindex(columns=expected_columns)  # 对列重新排序后，需去除最后一个元素‘2019年年报’


# 定义一个函数，用于处理末尾的零和小数点
def trim_trailing_zeros(x):
    if x == 0:  # 如果x是0，直接返回'0'
        return '0'
    else:
        # 将数字转换为字符串，然后移除末尾的零和小数点
        return str(x).rstrip('0').rstrip('.') if '.' in str(x) else str(x)


def getInfoByDataFrame(base_dir, type_lst, file):
    file_path = os.path.join(base_dir, file)
    zichanfuzhai_df_tmp = complete_miss_column(
        pd.read_csv(file_path + '\\' + file + type_lst[0] + '.txt', sep=',', header=0,
                    index_col=0))
    # zichanfuzhai_df_tmp.loc['公司类型'].apply(lambda x: '银行' in str(x)).any()
    if '公司类型' in zichanfuzhai_df_tmp.index.values and '银行' in zichanfuzhai_df_tmp.loc['公司类型'].tolist():
        print("遇到了金融机构的数据，不进行处理：", file)
        return False, None
    else:
        zichanfuzhai_df = convert_to_numeric(complete_miss_column(
            pd.read_csv(file_path + '\\' + file + type_lst[0] + '.txt', sep=',', header=0, index_col=0)))
        lirun_df = convert_to_numeric(complete_miss_column(
            pd.read_csv(file_path + '\\' + file + type_lst[1] + '.txt', sep=',', header=0, index_col=0)))
        xianjinliuliang_df = convert_to_numeric(complete_miss_column(
            pd.read_csv(file_path + '\\' + file + type_lst[2] + '.txt', sep=',', header=0, index_col=0)))
        caiwufenxi_df = convert_to_numeric(complete_miss_column(
            pd.read_csv(file_path + '\\' + file + type_lst[3] + '.txt', sep=',', header=0, index_col=0)), True)
        row_names = ['(现金+EBITDA)/短期债务', 'CAPEX/CFO(%)', 'CAPEX/营业收入(%)', 'CFO/付息债务', 'CFO/净利润(%)',
                     'CFO/营收', 'EBIDTA', 'EBIT/利息费用', 'EBITDA/营收', 'FCF/付息债务', '存货周转率', '短期债务/付息债务',
                     '付息债务', '付息债务/EBITDA', '付息债务结构比(%)', '经营活动现金净流量', '净利润', '净资产收益率(ROE)(%)',
                     '流动比率(%)', '流动资产周转率', '毛利率(%)', '其中：短期债务', '速动比率(%)', '所有者权益', '现金/短期债务',
                     '现金流量利息保障倍数', '销售净利润率(%)', '已获利息倍数', '营收增长率(%)', '营业利润', '营业收入',
                     '应收账款周转率(次)', '长期债务', '资本支出', '资产负债率(%)', '资产负债率(剔除合同负债和预收账款)(%)',
                     '总资产', '总资产收益率(ROA)(%)', '总资产增长率(%)']
        df = pd.DataFrame(index=row_names, columns=lirun_df.columns)
        total_Profit = lirun_df.index[lirun_df.index.str.contains('、利润总额')]
        net_Profit = lirun_df.index[lirun_df.index.str.contains('、净利润')]
        df_tmp = pd.DataFrame(columns=lirun_df.columns)
        df_tmp.loc['现金'] = deal_isno_index(xianjinliuliang_df, '期末现金及现金等价物余额(元)')
        # df_tmp.loc['EBIT'] = lirun_df.loc['五、利润总额(元)'] + lirun_df.loc['其中：利息费用(元)']
        df_tmp.loc['EBIT'] = \
            (lirun_df.loc[total_Profit] + deal_isno_index(lirun_df, '其中：利息费用(元)')).iloc[0]

        df_tmp.loc['EBITDA'] = df_tmp.loc['EBIT'] + deal_isno_index(xianjinliuliang_df,
                                                                    '固定资产和投资性房地产折旧(元)') + deal_sub_index(
            xianjinliuliang_df, '无形资产及长期待摊费用等摊销(元)', '无形资产摊销(元)', '长期待摊费用摊销(元)')
        # df_tmp.loc['短期债务'] = zichanfuzhai_df.loc['流动负债合计(元)']
        df_tmp.loc['短期债务'] = deal_isno_index(zichanfuzhai_df, '短期借款(元)') + deal_isno_index(zichanfuzhai_df,
                                                                                           '其中：应付票据(元)') + deal_isno_index(
            zichanfuzhai_df, '一年内到期的非流动负债(元)') + deal_isno_index(zichanfuzhai_df, '其中：交易性金融负债(元)')

        df_tmp.loc['CAPEX'] = deal_isno_other_index(xianjinliuliang_df, '购建固定资产、无形资产和其他长期资产支付的现金(元)',
                                                    '购建固定资产、无形资产和其他长期资产所支付的现金(元)') + deal_isno_other_index(
            xianjinliuliang_df, '取得子公司及其他营业单位支付的现金净额(元)', '取得子公司及其他营业单位支付的现金(元)')

        df_tmp.loc['CFO'] = deal_isno_index(xianjinliuliang_df, '经营活动产生的现金流量净额(元)')
        df_tmp.loc['营业收入'] = deal_isno_other_index(lirun_df, '营业收入(元)', '一、营业收入(元)')
        # df_tmp.loc['付息债务'] = zichanfuzhai_df.loc['短期借款(元)'] + zichanfuzhai_df.loc['应付利息(元)'] + zichanfuzhai_df.loc[
        #     '租赁负债(元)'] + zichanfuzhai_df.loc['预计负债(元)'] + zichanfuzhai_df.loc['递延所得税负债(元)']
        df_tmp.loc['付息债务'] = df_tmp.loc['短期债务'] + deal_isno_index(zichanfuzhai_df, '长期借款(元)') + deal_isno_index(
            zichanfuzhai_df, '应付债券(元)')
        # df_tmp.loc['净利润'] = lirun_df.loc['六、净利润(元)']
        df_tmp.loc['净利润'] = lirun_df.loc[net_Profit].iloc[0]
        df_tmp.loc['利息费用'] = deal_isno_index(lirun_df, '其中：利息费用(元)')
        df_tmp.loc['FCF'] = df_tmp.loc['CFO'] - df_tmp.loc['CAPEX']
        df_tmp.loc['长期债务'] = deal_isno_index(zichanfuzhai_df, '长期借款(元)') + deal_isno_index(zichanfuzhai_df, '应付债券(元)')
        df_tmp.loc['平均流动资产总额'] = deal_isno_index(zichanfuzhai_df, '流动资产合计(元)').rolling(window=2).mean().shift(-1)

        df.loc['(现金+EBITDA)/短期债务'] = (df_tmp.loc['现金'] + df_tmp.loc['EBITDA']) / np.where(df_tmp.loc['短期债务'] != 0,
                                                                                          df_tmp.loc['短期债务'], np.nan)
        df.loc['CAPEX/CFO(%)'] = (df_tmp.loc['CAPEX'] / np.where(df_tmp.loc['CFO'] != 0, df_tmp.loc['CFO'],
                                                                 np.nan)) * 100
        df.loc['CAPEX/营业收入(%)'] = (df_tmp.loc['CAPEX'] / np.where(df_tmp.loc['营业收入'] != 0, df_tmp.loc['营业收入'],
                                                                  np.nan)) * 100
        df.loc['CFO/付息债务'] = df_tmp.loc['CFO'] / np.where(df_tmp.loc['付息债务'] != 0, df_tmp.loc['付息债务'], np.nan)
        df.loc['CFO/净利润(%)'] = (df_tmp.loc['CFO'] / np.where(df_tmp.loc['净利润'] != 0, df_tmp.loc['净利润'], np.nan)) * 100
        df.loc['CFO/营收'] = df_tmp.loc['CFO'] / np.where(df_tmp.loc['营业收入'] != 0, df_tmp.loc['营业收入'], np.nan)
        df.loc['EBIDTA'] = df_tmp.loc['EBITDA'] / 10000
        df.loc['EBIT/利息费用'] = df_tmp.loc['EBIT'] / np.where(df_tmp.loc['利息费用'] != 0, df_tmp.loc['利息费用'], np.nan)
        df.loc['EBITDA/营收'] = df_tmp.loc['EBITDA'] / np.where(df_tmp.loc['营业收入'] != 0, df_tmp.loc['营业收入'], np.nan)
        df.loc['FCF/付息债务'] = df_tmp.loc['FCF'] / np.where(df_tmp.loc['付息债务'] != 0, df_tmp.loc['付息债务'], np.nan)
        df.loc['存货周转率'] = caiwufenxi_df.loc['存货周转率(次)']
        df.loc['短期债务/付息债务'] = df_tmp.loc['短期债务'] / np.where(df_tmp.loc['付息债务'] != 0, df_tmp.loc['付息债务'], np.nan)
        df.loc['付息债务'] = df_tmp.loc['付息债务'] / 10000
        df.loc['付息债务/EBITDA'] = df_tmp.loc['付息债务'] / np.where(df_tmp.loc['EBITDA'] != 0, df_tmp.loc['EBITDA'], np.nan)
        df.loc['付息债务结构比(%)'] = (df_tmp.loc['付息债务'] / np.where(deal_isno_index(zichanfuzhai_df, '负债合计(元)') != 0,
                                                              deal_isno_index(zichanfuzhai_df, '负债合计(元)'),
                                                              np.nan)) * 100
        df.loc['经营活动现金净流量'] = df_tmp.loc['CFO'] / 10000
        df.loc['净利润'] = df_tmp.loc['净利润'] / 10000
        df.loc['净资产收益率(ROE)(%)'] = deal_isno_index(caiwufenxi_df, '净资产收益率(%)')
        df.loc['流动比率(%)'] = deal_isno_index(caiwufenxi_df, '流动比率')
        # df.loc['流动资产周转率'] = df_tmp.loc['营业收入'] / np.where(deal_isno_index(zichanfuzhai_df, '流动资产合计(元)') != 0,
        #                                                   deal_isno_index(zichanfuzhai_df, '流动资产合计(元)'), np.nan)
        df.loc['流动资产周转率'] = df_tmp.loc['营业收入'] / np.where(df_tmp.loc['平均流动资产总额'] != 0, df_tmp.loc['平均流动资产总额'], np.nan)
        df.loc['毛利率(%)'] = deal_isno_index(caiwufenxi_df, '销售毛利率(%)')
        df.loc['其中：短期债务'] = df_tmp.loc['短期债务'] / 10000
        df.loc['速动比率(%)'] = deal_isno_index(caiwufenxi_df, '速动比率')
        df.loc['所有者权益'] = deal_isno_index(zichanfuzhai_df, '归属于母公司股东权益合计(元)') / 10000
        df.loc['现金/短期债务'] = df_tmp.loc['现金'] / np.where(df_tmp.loc['短期债务'] != 0, df_tmp.loc['短期债务'], np.nan)
        df.loc['现金流量利息保障倍数'] = df_tmp.loc['CFO'] / np.where(deal_isno_index(zichanfuzhai_df, '利息支出(元)') != 0,
                                                            deal_isno_index(zichanfuzhai_df, '利息支出(元)'), np.nan)
        df.loc['销售净利润率(%)'] = deal_isno_index(caiwufenxi_df, '销售净利率(%)')
        df.loc['已获利息倍数'] = deal_isno_index(caiwufenxi_df, '已获利息倍数(倍)')
        df.loc['营收增长率(%)'] = deal_isno_index(caiwufenxi_df, '营业收入增长率(%)')
        df.loc['营业利润'] = deal_isno_index(caiwufenxi_df, '营业利润(元)') / 10000
        df.loc['营业收入'] = df_tmp.loc['营业收入'] / 10000
        df.loc['应收账款周转率(次)'] = deal_isno_index(caiwufenxi_df, '应收账款周转率(次)')
        df.loc['长期债务'] = df_tmp.loc['长期债务'] / 10000
        df.loc['资本支出'] = df_tmp.loc['CAPEX'] / 10000
        df.loc['资产负债率(%)'] = deal_isno_index(caiwufenxi_df, '资产负债率(%)')
        df.loc['资产负债率(剔除合同负债和预收账款)(%)'] = ((deal_isno_index(zichanfuzhai_df, '负债合计(元)') - deal_isno_index(
            zichanfuzhai_df, '合同负债(元)') - deal_isno_index(zichanfuzhai_df, '预收款项(元)')) / np.where(
            deal_isno_index(zichanfuzhai_df, '资产总计(元)') != 0, deal_isno_index(zichanfuzhai_df, '资产总计(元)'),
            np.nan)) * 100
        df.loc['总资产'] = deal_isno_index(zichanfuzhai_df, '资产总计(元)') / 10000
        df.loc['总资产收益率(ROA)(%)'] = deal_isno_index(caiwufenxi_df, '总资产报酬率ROA(%)')
        df.loc['总资产增长率(%)'] = deal_isno_index(caiwufenxi_df, '总资产增长率(%)')
        df = df.replace([np.nan], 0).round(4).applymap(trim_trailing_zeros)
        df = df.drop(columns=['2019年年报'])  # 删除2019年年报列
        return True, df
