from core.config import settings
import streamlit as st
import logging
from logging.handlers import RotatingFileHandler
import plotly.graph_objects as go
import datetime

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


st.set_page_config(page_title="Insights Dashboard",
                   layout='wide',
                   page_icon=':balloon:',
                   initial_sidebar_state="collapsed")

st.header(f"Insights Dashboard")

with st.container(border=True):
    st.subheader('Analysis')
    fig = go.Figure()
    st.plotly_chart(fig)


