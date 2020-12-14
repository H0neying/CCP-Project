import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import cufflinks as cf
import plotly.graph_objects as go
import plotly.express as px
from urllib.request import urlopen
import json

with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
    counties = json.load(response)

engine = create_engine('postgresql://ccp:zjarotlf@147.46.197.124:56416/ccp?sslmode=disable')
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class StockSimulator:
    def __init__(self, stocks):  # method 1
        self.stocks = stocks
        for l in self.stocks.values(): # save every time argument as datetime type
            l[1] = pd.to_datetime(l[1])
            l[2] = pd.to_datetime(l[2])
        self.start_date = min([self.stocks[s][1] for s in self.stocks])
        self.end_date = max([self.stocks[s][2] for s in self.stocks])

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

        days = [d for d in self.pricing.date.tolist() if (d >= self.start_date) and (d <= self.end_date)]
        days = list(set(days))
        days.sort()
        self.days = days
        # list which contains target period only

        self.total_values = None

    def date_converter(self):  # method 2
        self.pricing.date = pd.to_datetime(self.pricing.date)
        self.split.date = pd.to_datetime(self.split.date)
        self.dividend.exdate = pd.to_datetime(self.dividend.exdate)

    def value(self):
        self.date_converter()  # make every time variable to datetime type
        total_values = {}  # total_values has stock id as keys and
        # dictionary (where date for key and stock value for value) as values

        for s in self.stocks.keys():
            pricing = self.pricing.loc[self.pricing.tradingitemid == s]
            split = self.split.loc[self.split.tradingitemid == s]
            dividend = self.dividend.loc[self.dividend.tradingitemid == s]
            values = {}  # date for key and stock value for value

            init_num = self.stocks[s][0]
            buy = self.stocks[s][1]
            sell = self.stocks[s][2]

            while pricing[pricing.date == buy].empty:  # if no transaction
                buy += pd.Timedelta(days=1)
            while pricing[pricing.date == sell].empty:  # if no transaction
                sell += pd.Timedelta(days=1)

            nums = {}  # date for key, the number of the stock for value
            num = init_num  # initial number of the stock
            for d in self.days:  # construct nums
                if d < buy:
                    nums[d] = 0
                elif not split[split.date == d].empty:
                    num *= float(split[split.date == d].rate)
                nums[d] = num

            divide = 0
            for d in self.days:  # construct values, d == day
                if d < buy:
                    values[d] = 0
                elif d <= sell:
                    n = nums[d]  # n == the number of stock

                    temp_date = d
                    while pricing[pricing.date == temp_date].empty:  # if no transaction, go to past
                        temp_date -= pd.Timedelta(days=1)
                    price = float(pricing[pricing.date == temp_date].close) * n

                    if not dividend[dividend.exdate == d].empty:
                        divide += float(dividend[dividend.exdate == d].divamount) * n

                    values[d] = price + divide
                else:
                    values[d] = values[sell]

            total_values[s] = values  # construct total_values
        self.total_values = total_values

        total_returns = {}  # which will be returned
        for d in self.days:
            value_sum = 0
            for S in total_values:
                value_sum += total_values[S][d]
            total_returns[d] = value_sum

        return total_returns  # return dictionary type

    def daily_value(self, *args):
        self.value()
        portfolio = pd.DataFrame({'date': self.value().keys(), 'total': self.value().values()})
        for s in args:
            temp = pd.DataFrame({s: self.total_values[s].values()})
            portfolio = pd.concat([portfolio, temp], axis=1)
        self.time_series_to_graph(portfolio)  # graphically describe
        return portfolio

    def time_series_to_graph(self, df):
        fig = px.line(df, x='date', y=[c for c in df.columns if c != 'date'])
        fig.update_layout(title='Portfolio Value', xaxis_title='Date', yaxis_title='Value($)')
        fig.update_layout(
            xaxis=dict(
                showline=True,
                showgrid=True,
                gridcolor='rgb(245, 245, 245)',
                showticklabels=True,
                linecolor='rgb(0, 0, 0)',
                linewidth=2,
                ticks='outside',
                tickfont=dict(
                    family='Arial',
                    size=12,
                    color='rgb(82, 82, 82)'),
                rangeselector=dict(
                    buttons=list([dict(count=1, label='1m', step='month', stepmode='backward'),
                                  dict(count=6, label='6m', step='month', stepmode='backward'),
                                  dict(count=1, label="YTD", step="year", stepmode="todate"),
                                  dict(count=1, label='1y', step='year', stepmode='backward'),
                                  dict(step='all')])
                ),
                rangeslider=dict(visible=True),
                type='date'
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='rgb(245, 245, 245)',
                zeroline=True,
                showline=True,
                linecolor='rgb(0, 0, 0)',
                linewidth=2,
                showticklabels=True,
            ),
            autosize=True,
            margin=dict(
                autoexpand=True,
                l=100,
                r=20,
                t=110,
            ),
            showlegend=False,
            plot_bgcolor='white'
        )
        fig.show()

    def how_many_companies(self):
        id_lst = self.stocks.keys()

        class tradingitem(Base):
            __tablename__ = 'tradingitem'
            tradingitemid = Column(Integer, primary_key=True)
            abbreviation = Column(String)
            isocountry3 = Column(String)

        tradingitem = session.query(tradingitem).filter(tradingitem.tradingitemid.in_(id_lst),
                                                        tradingitem.isocountry3.in_(['USA'])
                                                        )
        tradingitem = pd.read_sql_query(tradingitem.statement, con=session.bind)

        df = tradingitem.abbreviation.value_counts()
        df = pd.DataFrame({'state': df.index, 'num': df.values})
        fig = px.choropleth(df,
                            geojson=counties,
                            locations='state',
                            locationmode="USA-states",
                            color='num',
                            color_continuous_scale="Viridis",
                            range_color=(0, 10),
                            scope="usa",
                            labels={'num': 'counter'}
                            )
        fig.show()


#######################################################################################################################
# This code is written by Jongheon Yeo @ SNU ECE
# Below is Input Example

'''
test = {2656191: [10, '2016-8-18', '2018-10-20'],
        2610835: [20, '2017-8-18', '2018-11-20'],
        49031561: [10, '2016-5-20', '2019-6-25']}
s = StockSimulator(test)
print(s.daily_value(2656191, 49031561))
'''