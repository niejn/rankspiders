from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd











def main():
    con = 'sqlite:///../exchange.sqlite'
    engine = create_engine(con, echo=True)
    sql_cmd = '''SELECT report_date, PARTICIPANTABBR1, CJ1, exchange, instrumentid, productname FROM ranks
      WHERE report_date >= date('2018-06-01') and report_date < date('2018-06-06')
      AND (PARTICIPANTABBR1 == '中信期货' or PARTICIPANTABBR1=="海通期货") ORDER BY report_date, instrumentid
      '''
    df = pd.read_sql_query(sql=(sql_cmd), con=engine)
    # grouped = df.groupby(['合约品种', ])
    # df.set_index(['ind1','ind2']
    # df = df.set_index(['PARTICIPANTABBR1', ''])
    print(df)
    df_zhongxin = df[df['PARTICIPANTABBR1'] == '中信期货']
    df_haitong = df[df['PARTICIPANTABBR1'] == '海通期货']
    result = pd.merge(df_zhongxin, df_haitong, on=['report_date', 'instrumentid'], suffixes=('_中信', '_海通'))
    print(result)
    result = result[['report_date', 'instrumentid', 'CJ1_中信', 'CJ1_海通',  ]]
    print(result)
    df_dict = {'report_date': '成交日期', 'instrumentid': '合约代码',
               'CJ1_中信': '中信期货', 'CJ1_海通': '海通期货', }
    result.rename(columns=df_dict, inplace=True)
    result['差值'] = result['中信期货'] - result['海通期货']
    result['成交日期'] = result['成交日期'].apply(lambda x: x.split()[0])

    print(result['成交日期'])
    # df.groupby(['Country', 'Item_Code'])[["Y1961", "Y1962", "Y1963"]].sum()
    result_sum = result.groupby(['成交日期',])[['中信期货', '海通期货', '差值']].sum()
    result_sum['合约代码'] = '单日统计'
    result_sum.reset_index(inplace=True)
    print(result_sum)
    # 成交日期	合约代码	中信期货	海通期货	差值
    # df.groupby('A').agg({'B': ['min', 'max'], 'C': 'sum'})
    result = result.append(result_sum)
    print(result)
    # result.sort_values("中信期货", inplace=True)
    result = result.sort_index(by=['成交日期', '中信期货', '海通期货'], ascending=[True, True, True])
    # result = result.sort(['成交日期', '中信期货', '海通期货'], ascending=[1, 1, 1])
    print(result)
    csv_index = ['成交日期', '合约代码', '中信期货', '海通期货', '差值', ]
    result.to_csv('统计结果.csv', encoding='gbk', index=False, columns=csv_index)
    return

if __name__ == '__main__':
    main()