from core.db import settings, sync_engine
import streamlit as st
import logging
import pandas as pd
from logging.handlers import RotatingFileHandler
import plotly.graph_objects as go
from sqlalchemy import text

logging.basicConfig(
    format='%(asctime)s : %(name)s  : %(funcName)s : %(levelname)s : %(message)s'
    , level=logging.DEBUG if settings.debug_logs else logging.INFO
    , handlers=[
        RotatingFileHandler(
            filename="logs/Dashboard.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB per file,
            backupCount=7  # keep 7 backups
        ),
        logging.StreamHandler()  # Continue to log to the console as well
    ]
)


def fetch_all_data_from_db():
    with sync_engine.connect() as session:
        df_agg1 = pd.DataFrame(session.execute(text(f"SELECT * FROM {settings.dw_schema}.cow_aggregation_data")).all())
        df_insights1 = pd.DataFrame(session.execute(text(f"SELECT * FROM {settings.dw_schema}.cow_price_improvement")).all())
    return df_agg1, df_insights1


st.set_page_config(page_title="Insights Dashboard",
                   layout='wide',
                   page_icon=':balloon:',
                   initial_sidebar_state="collapsed")

st.header(f"Insights Dashboard")

with st.container(border=True):
    df_agg, df_insights = fetch_all_data_from_db()
    st.subheader('Analysis')
    col1, col2, col3, col4, col5 = st.columns(5)
    with st.container(border=True):
        col1.metric(
            label='No Trades',
            value=sum(df_agg['no_txn'])
        )
        col2.metric(
            label='Units Bought',
            value=round(sum(df_agg['total_units_bought']))
        )
        col3.metric(
            label='Units Sold',
            value=round(sum(df_agg['total_units_sold']))
        )
        col4.metric(
            label='Value Sold ($)',
            value=round(sum(df_agg['total_usd_value_sold']))
        )
        col5.metric(
            label='Value Bought ($)',
            value=round(sum(df_agg['total_usd_value_bought']))
        )

    # Ensure the 'date' column is of datetime type
    df_insights['date'] = pd.to_datetime(df_insights['date'])

    # Sort the DataFrame by date
    df = df_insights.sort_values('date')

    # Create traces for each token
    tokens = df['cow_token'].unique()
    traces = []

    for token in tokens:
        token_df = df[df['cow_token'] == token]
        trace = go.Scatter(
            x=token_df['date'],
            y=token_df['avg_price_difference'],
            mode='lines+markers',
            name=token
        )
        traces.append(trace)

    # Define the layout
    layout = go.Layout(
        title='Average Price Difference Over Time of Tokens on CoW Compared to CoinGecko',
        xaxis=dict(title='Date'),
        yaxis=dict(title='Average Price Difference'),
        legend=dict(title='Token'),
        hovermode='x unified'
    )

    # Create the figure
    fig = go.Figure(data=traces, layout=layout)

    with st.container(border=True):
        st.plotly_chart(fig)
