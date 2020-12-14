import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('postgresql://ccp:zjarotlf@147.46.197.124:56416/ccp?sslmode=disable')
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class StockSimulator:
    def __init__(self, stocks, start_date, end_date):  # method 1
        self.stocks = stocks
        self.start_date = pd.to_datetime(start_date)  # save every time argument as datetime type
        self.end_date = pd.to_datetime(end_date)
        
        class pricing(Base):
            __tablename__ = 'pricing'
            tradingitemid = Column(Integer, primary_key=True)
            date = Column(DateTime)
            isocode = Column(String)
            close = Column(Float)
            high = Column(Float)
            low = Column(Float)
            open = Column(Float)

        pricing = session.query(pricing).filter(pricing.tradingitemid.in_(stocks.keys()),
                                                pricing.date >= self.start_date,
                                                pricing.date <= self.end_date)
        self.pricing = pd.read_sql_query(pricing.statement, con=session.bind)

        class split(Base):
            __tablename__ = 'split'
            tradingitemid = Column(Integer, primary_key=True)
            date = Column(DateTime)
            rate = Column(Float)
            splittype = Column(String)

        split = session.query(split).filter(split.tradingitemid.in_(stocks.keys()),
                                            split.date >= self.start_date,
                                            split.date <= self.end_date)
        self.split = pd.read_sql_query(split.statement, con=session.bind)

        class dividend(Base):
            __tablename__ = 'dividend'
            tradingitemid = Column(Integer, primary_key=True)
            exdate = Column(DateTime)
            divamount = Column(Float)

        dividend = session.query(dividend).filter(dividend.tradingitemid.in_(stocks.keys()),
                                                  dividend.exdate >= self.start_date,
                                                  dividend.exdate <= self.end_date)
        self.dividend = pd.read_sql_query(dividend.statement, con=session.bind)

        # make start_date as market-open-day
        breaker = True
        while breaker:
            ids = self.pricing[self.pricing.date == self.start_date].tradingitemid.tolist()
            for st in list(stocks.keys()):
                if st not in ids:
                    self.start_date += pd.Timedelta(days=1)
                    break
                elif st != list(stocks.keys())[-1]:
                    continue
                else:
                    breaker = False

    def date_converter(self):  # method 2
        self.pricing.date = pd.to_datetime(self.pricing.date)
        self.split.date = pd.to_datetime(self.split.date)
        self.dividend.exdate = pd.to_datetime(self.dividend.exdate)

    def total_value(self):  # method 3
        self.date_converter()  # make every time variable to datetime type
        total_values = {}  # total_values has stock id as keys and
        # dictionary (where date for key and stock value for value) as values
        days = [d for d in self.pricing.date.tolist() if (d >= self.start_date) and (d <= self.end_date)]
        # list which contains target period only

        # delete days where transaction does not occur
        for d in days:
            temp = self.pricing[self.pricing.date == d]
            for k in self.stocks.keys():
                if k in temp.tradingitemid.tolist():
                    break
                days.remove(d)

        for s in self.stocks:
            pricing = self.pricing.loc[self.pricing.tradingitemid == s]
            split = self.split.loc[self.split.tradingitemid == s]
            dividend = self.dividend.loc[self.dividend.tradingitemid == s]
            values = {}  # date for key and stock value for value

            nums = {}  # date for key, the number of the stock for value
            num = self.stocks[s]  # initial number of the stock
            for d in days:  # construct nums
                if not split[split.date == d].empty:
                    num *= float(split[split.date == d].rate)
                nums[d] = num

            divide = 0
            for d in nums:  # construct values, d == day
                n = nums[d]  # n == the number of stock

                temp_date = d
                while pricing[pricing.date == d].empty:  # if no transaction, go to past
                    temp_date -= pd.Timedelta(days=1)
                price = float(pricing[pricing.date == temp_date].close) * n

                if not dividend[dividend.exdate == d].empty:
                    divide += float(dividend[dividend.exdate == d].divamount) * n

                values[d] = price + divide

            total_values[s] = values  # construct total_values

            total_returns = {}  # which will be returned
            for d in days:
                value_sum = 0
                for S in total_values:
                    value_sum += total_values[S][d]
                total_returns[d] = value_sum
        return total_returns  # return dictionary type

nums = {2656191: 10, 2610835: 100}
s = StockSimulator(nums, '2018-8-18', '2018-10-20')
print(s.total_value())