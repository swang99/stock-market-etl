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
from config import STOCK_TABLE

load_dotenv()

@st.cache_resource(ttl=1800)
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
@st.cache_data(ttl=1800)
def load_historical_data(tickers, start_date, end_date):
	placeholders = ', '.join(['%s'] * len(tickers))
	query = f"""
		SELECT date, ticker, close, daily_return
		FROM {STOCK_TABLE}
		WHERE ticker IN ({placeholders})
		  AND date BETWEEN %s AND %s
		ORDER BY date, ticker
	"""
	params = tuple(tickers) + (start_date, end_date)
	with engine.connect() as conn:
		df = pd.read_sql_query(query, conn, params=params)
	return df

def compute_trends(df, init_investment):
	df = df.copy()
	df["daily_return"] = df["daily_return"].fillna(0)
	df["daily_return"] = 1 + df["daily_return"]
	df["cumulative_return"] = df.groupby("ticker")["daily_return"].cumprod()
	df["abs_return"] = init_investment * df["cumulative_return"]
	return df

def compute_final_returns(df):
	final_df = (
		df.groupby("ticker", as_index=False)
		  .agg({'cumulative_return': 'last'})
		  .rename(columns={'cumulative_return': 'final_return'})
	)
	return final_df

def compute_relative_returns(df, base_ticker, comp_ticker):
	base = df[df['ticker'] == base_ticker][['date', 'cumulative_return']]
	comp = df[df['ticker'] == comp_ticker][['date', 'cumulative_return']]
	merged = base.merge(comp, on='date', suffixes=('_base', '_comp'))
	merged['pct_diff'] = 100 * (merged['cumulative_return_base'] - merged['cumulative_return_comp'])
	return merged

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
	sp500_df.columns = ["Ticker", "Name", "Sector", "Return Today"]
	return sp500_df

@st.cache_data(ttl=3600)
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
TICKERS = sorted(ticker_map.keys())

with overview_tab:
	#st.caption(f"Last updated: {last_updated:%Y-%m-%d %I:%M %p}")
	
	def format_daily_return(val):
		if val > 0: return f"⬆ {100 * val:.2f}%"
		elif val < 0: return f"⬇ {100 * abs(val):.2f}%"
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


with compare_tab:
	# add ticker multi-selection field
	tickers_input = st.multiselect(
		"📈 Base Tickers", 
		options=TICKERS,
		default="AAPL", 
		max_selections=5, 
		format_func=format_ticker
	)

	comp_ticker = st.selectbox("Compare To", options=TICKERS, format_func=format_ticker)

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
	base_comp = tickers_input
	base_comp.append(comp_ticker)
	
	hist_df = load_historical_data(base_comp, dates[0], dates[1])
	trends_df = compute_trends(hist_df, investment_input)
	fin_returns_df = compute_final_returns(trends_df)

	if trends_df.empty:
		st.warning("No data found.")
	else:
		st.subheader(f"${investment_input:,} Invested in These Stocks is Now...")
		
		line = " | ".join(
			f"**{ticker}:** \${investment_input * (1 + (fin_returns_df.loc[fin_returns_df['ticker']==ticker]['final_return'].values[0])):,.2f}"
			for ticker in base_comp
		)
		st.markdown(line)

		# last_updated = fin_returns_df['last_ingested'].iloc[0]
		# st.caption(f"Last updated: {last_updated:%Y-%m-%d %I:%M %p}")

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

	if not tickers_input:
		st.warning("No base ticker(s) selected")

	for ticker in tickers_input:
		if ticker == comp_ticker: continue

		rel_df = compute_relative_returns(trends_df, ticker, comp_ticker)
		fig = go.Figure()

		fin_rel_return = rel_df['pct_diff'].iloc[-1]
		fin_color = "#ff4b4b" if fin_rel_return < 0 else "#1ed760"

		hover_text = [
			f"<b>{ticker}</b><br>Date: {d:%Y-%m-%d}<br>Return: {r:.2f}"
			for d, r in zip(rel_df['date'], rel_df['pct_diff'])
		]
		
		fig.add_trace(
			go.Scatter(
				x=rel_df["date"],
				y=rel_df["pct_diff"],
				mode="lines",
				line=dict(color=fin_color, width=2),
				showlegend=False,
				hoverinfo="text",
				text=hover_text
			)
		)

		# relative return text prompt
		fig.update_layout(
			title=dict(
				text=f"⚔️ {ticker} outperforms {comp_ticker} by {fin_rel_return:.2f} percentage points",
				font=dict(color=fin_color)
			),
			xaxis_title=None,
			yaxis_title="Return (pp)",
			height=330
		)

		# highlight 0% gridline
		fig.update_layout(
			shapes=[dict(
				type="line",
				x0=rel_df['date'].min(), x1=rel_df['date'].max(),
				y0=0, y1=0,
				line=dict(width=1.5),
				layer="below" 
			)]
		)

		st.plotly_chart(fig, key=f"{ticker}_vs_{comp_ticker}")