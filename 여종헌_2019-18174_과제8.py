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

cf.go_offline()
engine = create_engine('postgresql://ccp:zjarotlf@147.46.197.124:56416/ccp?sslmode=disable')
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


# Q1
def time_series_to_graph(df):
    fig = go.Figure()
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
    # Column names should be altered appropriately
    fig.add_trace(go.Scatter(x=df['date'], y=df['close'],
                             mode='lines')).show()


# Q2
def how_many_companys(id_lst):
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


# 2
lst = [2631520, 2631609, 49031561]
how_many_companys(lst)
