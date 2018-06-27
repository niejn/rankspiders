import datetime
from time import sleep

from shfe_spider import shfe_rank
from spider_dalian import get_dalian_ranks
from spider_zhenzhou import czce_scrape
from zhongjin_spider_final import cffex_rank


def main():
    # cffex_rank_by_contract()
    # return
    today =  datetime.datetime.now()+ datetime.timedelta(days=1)
    endday = datetime.datetime.now() - datetime.timedelta(days=1)
    for i in range(30):
        from shfe_spider import getLastWeekDay
        weekday = getLastWeekDay(today)
        today = weekday
        if today <= endday:
            break
        print(weekday)
        cffex_rank(year=weekday.year, month=weekday.month, day=weekday.day)
        # czce_scrape(year=weekday.year, month=weekday.month, day=weekday.day)
        # get_dalian_ranks(year=weekday.year, month=weekday.month, day=weekday.day)
        # shfe_rank(year=weekday.year, month=weekday.month, day=weekday.day)
        sleep(2)
    return

if __name__ == '__main__':
    main()