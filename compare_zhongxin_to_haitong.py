from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd











def main():
    con = 'sqlite:///exchange.sqlite'
    engine = create_engine(con, echo=True)
    sql_cmd = '''SELECT  report_date as 交易日, exchange AS 交易所, sum(中信期货成交) AS 中信期货成交,
  sum(海通期货成交) AS 海通期货成交, sum(中信期货成交) - sum(海通期货成交) AS 差值
FROM
  (SELECT  zhongxin.report_date AS report_date, zhongxin.PARTICIPANTABBR1 AS 中信期货, zhongxin.CJ1 AS 中信期货成交,
  zhongxin.instrumentid AS 合约代码, zhongxin.exchange AS exchage,
  haitong.report_date, haitong.PARTICIPANTABBR1 AS 海通期货, haitong.CJ1 AS 海通期货成交, haitong.instrumentid, haitong.exchange
FROM (SELECT report_date, PARTICIPANTABBR1, CJ1, exchange, instrumentid, productname FROM ranks
WHERE PARTICIPANTABBR1 == '中信期货' AND VARIETY== 0  GROUP BY report_date, exchange, instrumentid) AS zhongxin JOIN
(SELECT report_date, PARTICIPANTABBR1, CJ1, exchange, instrumentid, productname FROM ranks
WHERE PARTICIPANTABBR1 == '海通期货' AND VARIETY== 0 ORDER BY report_date, exchange, instrumentid) AS haitong
WHERE zhongxin.report_date == haitong.report_date and zhongxin.instrumentid == haitong.instrumentid
      and zhongxin.exchange == haitong.exchange) GROUP BY report_date, exchange ORDER BY exchange, report_date;

      '''
    df = pd.read_sql_query(sql=(sql_cmd), con=engine)
    print(df)
    df['交易日'] = pd.to_datetime(df['交易日'])
    df['交易日'] = df['交易日'].dt.strftime('%Y-%m-%d')
    df.to_csv('中信期货海通期货成交量对比.csv', encoding='gbk', index=False)
    return

if __name__ == '__main__':
    main()