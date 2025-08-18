import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os

# -----------------
# CONFIG
# -----------------
from config import TICKERS, STOCK_TABLE

load_dotenv()

@st.cache_resource(ttl=3600)
def get_engine():
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_HOST = os.getenv("POSTGRES_HOST")
    DB_PORT = os.getenv("POSTGRES_PORT")
    DB_NAME = os.getenv("POSTGRES_DB")
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(DATABASE_URL)
engine = get_engine()

# -----------------
# FUNCTIONS
# -----------------
@st.cache_data
def get_trends_df(tickers, start_date, end_date, init_investment):
	"""Get only investment value trends over time for each ticker"""
	if tickers:
		query = f"""
			WITH historicals AS (
				SELECT date, ticker, close,
					SUM(COALESCE(daily_return, 0)) OVER (
						PARTITION BY ticker
						ORDER BY date
						ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
					) AS cumulative_return
				FROM {STOCK_TABLE}
				WHERE ticker IN ({', '.join(['%s'] * len(tickers))})
				AND date BETWEEN %s AND %s
			)

			SELECT date, ticker, close, 
			%s * (1 + (cumulative_return / 100)) AS abs_return 
			FROM historicals
			ORDER BY date, ticker
		"""
		params = tuple(tickers) + (start_date, end_date, init_investment)
		with engine.connect() as conn:
			trends_df = pd.read_sql_query(query, conn, params=params)
		return trends_df
	else: 
		return pd.DataFrame()

def get_fin_returns_df(tickers, start_date, end_date):
	""" Get only final return percentages for each ticker"""
	if tickers:
		query = f"""
			WITH historicals AS (
				SELECT date, ticker, close, ingest_ts,
					SUM(COALESCE(daily_return, 0)) OVER (
						PARTITION BY ticker
						ORDER BY date
						ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
					) AS cumulative_return
				FROM {STOCK_TABLE}
				WHERE ticker IN ({', '.join(['%s'] * len(tickers))})
				AND date BETWEEN %s AND %s
			),
			historicals_p2 AS (
				SELECT ticker, cumulative_return, ingest_ts,
					LAST_VALUE(cumulative_return) OVER (
						PARTITION BY ticker 
						ORDER BY date
						ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
					) AS final_return
				FROM historicals
				ORDER BY ticker, date
			)

			SELECT ticker, AVG(final_return) AS final_return, MAX(ingest_ts) AS last_ingested
			FROM historicals_p2
			GROUP BY ticker, ingest_ts
		"""

		with engine.connect() as conn:
			params = tuple(tickers) + (start_date, end_date)
			df = pd.read_sql_query(query, conn, params=params)
		return df
	else:
		return pd.DataFrame()

def append_relative_ticker(comp_ticker, comparison_ticker, start_date, end_date):
	query = f"""
		WITH h1 AS (
			SELECT date, ticker, close, daily_return,
				SUM(COALESCE(daily_return, 0)) OVER (
					PARTITION BY ticker
					ORDER BY date
					ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				) AS cumulative_return,
			%s AS comp_ticker
			FROM {STOCK_TABLE}
			WHERE ticker <> %s
			  AND ticker = %s
			ORDER BY ticker, date DESC
		),
		h2 AS (
			SELECT date, ticker,
				SUM(COALESCE(daily_return, 0)) OVER (
					PARTITION BY ticker
					ORDER BY date
					ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				) AS cumulative_return
			FROM {STOCK_TABLE}
			WHERE ticker = %s
			ORDER BY ticker, date DESC
		) 

		SELECT h1.date, h1.ticker, h2.ticker AS comp_ticker, 
			   h1.cumulative_return - h2.cumulative_return AS pct_diff
		FROM h1
		JOIN h2 ON h1.comp_ticker = h2.ticker AND h1.date = h2.date
		WHERE h1.date BETWEEN %s and %s;
	"""

	params = (comp_ticker, comp_ticker, comparison_ticker, 
		   	  comp_ticker, start_date, end_date)
	with engine.connect() as conn:
		appended_df = pd.read_sql_query(query, conn, params=params)
	return appended_df

@st.cache_data
def get_sp500_info():
	query = f"""
		With cte AS (
		  	SELECT sp500.ticker_symbol, sp500.security_name, 
			   sp500.gics_sector, st.daily_return, st.date,
			   ROW_NUMBER() OVER(
				 PARTITION BY sp500.ticker_symbol
				 ORDER BY st.date DESC
			   ) AS row_num
			FROM sp500_companies sp500
			JOIN {STOCK_TABLE} st ON sp500.ticker_symbol = st.ticker
		)

		SELECT ticker_symbol, security_name, gics_sector, daily_return
		FROM cte
		WHERE row_num = 1
		ORDER BY daily_return DESC;
	"""
	with engine.connect() as conn:
		sp500_df = pd.read_sql_query(query, conn)
	sp500_df["daily_return"] = sp500_df["daily_return"].round(2)
	sp500_df.columns = ["Ticker", "Name", "Sector", "Return Today"]
	return sp500_df

@st.cache_data
def get_ticker_map():
	sp500_df = get_sp500_info()
	return dict(zip(sp500_df["Ticker"], sp500_df["Name"]))

ticker_map = get_ticker_map()

# formatting function
def format_ticker(option: str) -> str:
	return f"{option} ({ticker_map.get(option, 'Unknown')})"


# -----------------
# APP
# -----------------
st.title("S&P 500 Analyzer")
overview_tab, compare_tab, = st.tabs(["Overview", "Compare"])

with compare_tab:
	# add ticker multi-selection field
	tickers_input = st.multiselect(
		"📈 Base Tickers", 
		options=TICKERS,
		default="AAPL", 
		max_selections=5, 
		format_func=format_ticker
	)

	if not tickers_input:
		st.warning("No base ticker(s) selected")
	
	col1, col2 = st.columns([1, 1])
	with col1:
		investment_input = st.number_input("💵 Initial investment", value=100000, min_value=1)

	with col2:
		start_min = datetime(2020, 1, 1)
		start_max = datetime.today()

		dates = st.date_input(
			"📆 Date range", value=(start_min, start_max), 
			max_value=start_max, format="YYYY.MM.DD"
		)

	if len(dates) != 2: 
		st.warning("Missing start or end date in range")
		st.stop()

	# 1. return graph
	trends_df = get_trends_df(tickers_input, dates[0], dates[1], investment_input)
	fin_returns_df = get_fin_returns_df(tickers_input, dates[0], dates[1])

	if trends_df.empty:
		st.warning("No data found.")
	else:
		st.subheader(f"${investment_input:,} Invested in These Stocks is Now...")

		line = " | ".join(
			f"**{ticker}:** \${investment_input * (1 + (fin_returns_df.loc[fin_returns_df['ticker']==ticker]['final_return'].values[0]/100)):,.2f}"
			for ticker in tickers_input
		)
		st.markdown(line)

		last_updated = fin_returns_df['last_ingested'].iloc[0]
		st.caption(f"Last updated: {last_updated:%Y-%m-%d %I:%M %p}")

		chart_1 = px.line(
			trends_df, x='date', y='abs_return', color='ticker',
			hover_name='ticker', height=330,
			custom_data=['ticker', 'date', 'abs_return']
		)

		chart_1.update_traces(
			hovertemplate="<b>%{customdata[0]}</b><br>" +
				"<b>Date:</b> %{x|%Y-%m-%d}<br>" +
				"<b>Value:</b> $%{customdata[2]:,.0f}<extra></extra>",
		)

		chart_1.update_layout(
			xaxis_title=None,
			yaxis_title="Value ($)"
		)

		st.plotly_chart(chart_1)

	# 2. relative graph
	st.subheader(f"🚀 Relative Returns")
	comp_ticker = st.selectbox("Compare To", options=TICKERS, format_func=format_ticker)

	if not tickers_input:
		st.warning("No base ticker(s) selected")

	for ticker in tickers_input:
		if ticker != comp_ticker:
			relative_to_df = append_relative_ticker(comp_ticker, ticker, dates[0], dates[1])
			fig = go.Figure()

			for i in range(len(relative_to_df) - 1):
				color = "#ff4b4b" if relative_to_df.loc[i, "pct_diff"] < 0 else "#1ed760"
				rel_chart_date = relative_to_df['date'].iloc[i]
				curr_rel_return = relative_to_df['pct_diff'].iloc[i]
				fin_rel_return = relative_to_df['pct_diff'].iloc[0]

				fig.add_trace(
					go.Scatter(
						x=relative_to_df["date"].iloc[i:i+2],
						y=relative_to_df["pct_diff"].iloc[i:i+2],
						mode="lines",
						line=dict(color=color, width=2),
						showlegend=False,
						hoverinfo="text",
						text=[
							f"<b>{ticker}</b><br>Date: {rel_chart_date:%Y-%m-%d}"
							f"<br>Return: {curr_rel_return:.2f}%"
						] * 2
					)
				)

				fig.update_layout(
					title=dict(
						text=f"⚔️ {ticker} outperforms {comp_ticker} by {fin_rel_return:.2f} percentage points",
						font=dict(color="#ff4b4b" if fin_rel_return < 0 else "#1ed760")
					),
					xaxis_title=None,
					yaxis_title="Return (pp)",
					height=330
				)

			st.plotly_chart(fig)

with overview_tab:
	st.caption(f"Last updated: {last_updated:%Y-%m-%d %I:%M %p}")
	
	def format_daily_return(val):
		if val > 0: return f"⬆ {val:.2f}%"
		elif val < 0: return f"⬇ {abs(val):.2f}%"
		return f"— {val:.2f}%"
		
	def color_daily_return(val):
		if "⬆" in val: color = "#1ed760"
		elif "⬇" in val: color = "#ff4b4b"
		else: color = "#31333f"
		return f"color: {color}"
	
	sp500_info = get_sp500_info()
	sp500_info.index += 1
	sectors_filter = st.selectbox(
		label="Sector Filter", 
		options=["🌟 All"] + sp500_info["Sector"].unique().tolist()
	)

	if sectors_filter != "🌟 All":
		sector_df = sp500_info[sp500_info["Sector"] == sectors_filter]
	else:
		sector_df = sp500_info

	top_n = min(len(sector_df) // 2, 20)
	top_gainers = sector_df.sort_values(by="Return Today", ascending=False).head(top_n)
	top_losers = sector_df.sort_values(by="Return Today", ascending=True).head(top_n)
	top_gainers["Return Today"] = top_gainers["Return Today"].apply(format_daily_return)
	top_losers["Return Today"] = top_losers["Return Today"].apply(format_daily_return)

	st.subheader("📈 Top Gainers")
	st.dataframe(top_gainers.style
		.map(color_daily_return, subset=["Return Today"])
	)

	st.subheader("📉 Top Losers")
	st.dataframe(top_losers.style
		.map(color_daily_return, subset=["Return Today"])
	)
	