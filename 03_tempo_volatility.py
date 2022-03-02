# Databricks notebook source
# MAGIC %md
# MAGIC # Modernizing Investment Data Platforms - volatility
# MAGIC 
# MAGIC The appetite for investment in a range of asset classes is at a historic high in 2020. As of July 2020, "Retail traders make up nearly 25% of the stock market following COVID-driven volatility" ([source](https://markets.businessinsider.com/news/stocks/retail-investors-quarter-of-stock-market-coronavirus-volatility-trading-citadel-2020-7-1029382035)). As investors gain access and trade alternative assets such as cryptocurrency, trading volumes have skyrocketed and created new data challenges. Moreover, cutting edge research is no longer restricted to institutional investors on Wall Street - today’s world of investing extends to digital exchanges in Silicon Valley, data-centric market makers, and retail brokers that are investing increasingly in AI-powered tools for investors. Data lakes have become standard for building financial data products and research, but the lack of blueprints for how to build an enterprise data lake in the cloud lakes hinder the adoption to scalable AI (such as volatility forecasting). Through a series of design patterns and real world examples, we address these core challenges and show benchmarks to illustrate why building investment data platforms on Databricks leads to cost savings and improved financial products. We also introduce [tempo](https://github.com/databrickslabs/tempo), an open source library for manipulating time series at scale.
# MAGIC 
# MAGIC ---
# MAGIC + <a href="$./01_tempo_context">STAGE0</a>: Home page
# MAGIC + <a href="$./02_tempo_etl">STAGE1</a>: Design pattern for ingesting BPipe data
# MAGIC + <a href="$./03_tempo_volatility">STAGE2</a>: Introducing tempo for calculating market volatility
# MAGIC + <a href="$./04_tempo_spoofing">STAGE3</a>: Introducing tempo for market surveillance
# MAGIC ---
# MAGIC <ricardo.portilla@databricks.com>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Fundamental Investment Research Analysis
# MAGIC 
# MAGIC Time series data sets are ubiquitous at financial institutions. From fundamental data such as earnings and corporate actions to technical tick and news data, decisions are rooted in being able to join the most up-to-date data together. Specifically, we want the ability to join contextual data (news, earnings) as of the time of technical data (e.g. trade information). This work aims to show how to build a performant time series data lake with the help of Databricks Delta Engine and Delta Lake.
# MAGIC 
# MAGIC ## Flow for This Notebook 
# MAGIC 
# MAGIC <p></p>
# MAGIC 
# MAGIC * Step 0 - Read in Mock Xpressfeed data (created from specification [here](https://www.spglobal.com/marketintelligence/en/documents/compustat-brochure_digital.pdf))
# MAGIC * Step 1 - Merge in Fundamental Xpressfeed Data into Daily Trade Calendar using AS OF Join from Databricks Lab tempo (see more information below)
# MAGIC * Step 2 - Define Peer Groups Based on Average TTM of Fundamental Metric (Return on Equity) and Use Quartiles to Partition Tickers
# MAGIC * Step 3 - Introduce Technical Intraday Tick Data - Resample Months of Data for Volatility Forecast
# MAGIC * Step 4 - Create Daily Summmary on Ticker Stats
# MAGIC 
# MAGIC 
# MAGIC ### Dependencies
# MAGIC This notebook shows a real life example using [Tempo](https://github.com/databrickslabs/tempo), an open source library developed by DataBricks labs for time series manipulation at massive scale. Although the library can be packaged as an archive, we show how analysts could quickly install scope libraries using the `%pip` magic command. 

# COMMAND ----------

# DBTITLE 1,Install Databricks Labs tempo Package
# MAGIC %pip install git+https://github.com/databrickslabs/tempo.git

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### `STEP0` - Load XPressFeed-like Data and Symbol Information to Delta Lake
# MAGIC 
# MAGIC Please follow these [steps](https://docs.databricks.com/data/data.html#import-data-1) to upload your own XPressFeed batch to Filestore

# COMMAND ----------

from pyspark.sql.functions import * 

# COMMAND ----------

from pyspark.sql.types import *

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.sql("create database if not exists tempo")


ticker_info = spark.read.format("parquet").load("dbfs:/FileStore/shared_uploads/finserv/s&p_mock_data/part_00000_tid_1519639363878959916_76cbb055_c467_423b_a040_8ed072868ec3_3704_1_c000_snappy.parquet")

capiq_mkt_fundamentals = spark.table("tempo.xpressfeed_fundamental")

display(capiq_mkt_fundamentals.select('event_dt', 'ticker', 'effdate', 'data_item_name', 'data_value'))

# COMMAND ----------

# DBTITLE 1,Read in Raw Xpressfeed Fundamental Data and Convert Date Format
from pyspark.sql.functions import *
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") 

src_df = spark.table("tempo.xpressfeed_fundamental")

src_df = (
  src_df.join(ticker_info, src_df.ticker == ticker_info.Symbol, "leftouter")
    .withColumn("company", col("Company_Name"))
)

src_df.write \
  .option("overwriteSchema", "true") \
  .mode('overwrite') \
  .format("delta") \
  .saveAsTable("tempo.xpressfeed_fundamentals_bronze")

# COMMAND ----------

# DBTITLE 1,Display S&P Symbols
capiq_mkt_fundamentals = spark.table("tempo.xpressfeed_fundamentals_bronze")
display(capiq_mkt_fundamentals.filter(col("company").isNotNull()))

# COMMAND ----------

# DBTITLE 1,Sample Data Items From Xpressfeed
# MAGIC %sql 
# MAGIC SELECT DISTINCT 
# MAGIC   data_item_name, 
# MAGIC   log(data_value) data_value
# MAGIC FROM 
# MAGIC   tempo.xpressfeed_fundamentals_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### `STEP1` - Merge In Fundamental Data into Daily Trade Calendar - Compute Trailing Twelve Month Aggregates Based on Factors
# MAGIC 
# MAGIC For each Date, For each Company Data Item, For each Company we want to filter to the data for that Company and Company Data Item that is available **as of** the date and calculate each transform based on the latest data available for each quarter. Using `tempo`, we make that AS-OF join efficient and reliable using a simple line of python code

# COMMAND ----------

# DBTITLE 1,Generate Calendar Dates
from pyspark.sql.functions import * 
data_item_ids = capiq_mkt_fundamentals.select('data_item_name').distinct()
dates = spark.range(365).withColumn("event_dt", ((col("id") + 50*365)*lit(3600*24)).cast("timestamp"))
companies = capiq_mkt_fundamentals.select('ticker').distinct()

# COMMAND ----------

# DBTITLE 1,Build Point-In-Time Calendar Per Ticker Symbol
spark.conf.set("spark.sql.crossJoin.enabled", "true")
calendar = dates.join(companies).join(data_item_ids)
display(calendar)

# COMMAND ----------

# DBTITLE 1,Add TTM for all Data Items and Dates
from tempo.tsdf import * 

import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func

windowSpec = \
  Window  \
    .partitionBy('ticker', 'data_item_name') \
    .orderBy('qtr')

ttm = \
  Window  \
    .partitionBy('ticker', 'data_item_name') \
    .orderBy('qtr') \
    .rowsBetween(-4, 0)

base_trades = TSDF(calendar, ts_col = 'event_dt', partition_cols = ['ticker', 'data_item_name'])
enriched_calendar = base_trades.asofJoin(
  TSDF(capiq_mkt_fundamentals, ts_col='event_dt', partition_cols=['ticker', 'data_item_name']), 
  right_prefix = 'asof_qtrly'
)

# prep data to attach yearly lagged values
enriched_calendar = enriched_calendar.df.withColumn("qtr", concat(year(col("event_dt")), quarter(col("event_dt"))))

capiq_w_year = capiq_mkt_fundamentals.withColumn("qtr", concat(year(col("event_dt")), quarter(col("event_dt")))). \
 withColumn("annual_lagged_data_value", lag('data_value', 4).over(windowSpec)). \
 withColumn("avg_ttm_data_value", avg('data_value').over(ttm)). \
 withColumn("sum_ttm_data_value", sum('data_value').over(ttm))

enriched_calendar = enriched_calendar.join(capiq_w_year.drop('event_dt'), ['qtr', 'ticker', 'data_item_name'])
enriched_calendar.write.mode('overwrite').saveAsTable("tempo.xpressfeed_pit_silver")

display(spark.table("tempo.xpressfeed_pit_silver").filter(col('asof_qtrly_event_dt').isNotNull()).filter(col("asof_qtrly_Industry").isNotNull()))

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS tempo.current_date_fundamentals;
# MAGIC 
# MAGIC CREATE TABLE tempo.current_date_fundamentals 
# MAGIC AS
# MAGIC SELECT 
# MAGIC   event_dt, 
# MAGIC   ticker, 
# MAGIC   data_item_name, 
# MAGIC   data_value, 
# MAGIC   sum_ttm_data_value, 
# MAGIC   avg_ttm_data_value 
# MAGIC FROM (
# MAGIC   WITH latest_dt AS (
# MAGIC     SELECT max(date) current_dt FROM tempo.silver_tick_daily_smry
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     event_dt, 
# MAGIC     ticker, 
# MAGIC     data_item_name, 
# MAGIC     data_value, 
# MAGIC     sum_ttm_data_value, 
# MAGIC     avg_ttm_data_value, 
# MAGIC     row_number() OVER (PARTITION BY event_dt, ticker, data_item_name ORDER BY avg_ttm_data_value) rn
# MAGIC   FROM tempo.xpressfeed_pit_silver a 
# MAGIC   JOIN latest_dt d
# MAGIC   ON a.event_dt = d.current_dt
# MAGIC ) foo 
# MAGIC WHERE rn = 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### `STEP2` - Define Peer Groups Based on Average TTM of Fundamental Metric (Return on Equity) and Use Quartiles to Partition Tickers

# COMMAND ----------

# DBTITLE 1,Define Peer Groups Based on One Data Item (Return on Equity %)
from pyspark.sql.functions import col
import re

all_features = spark.table("tempo.current_date_fundamentals") \
  .groupBy("ticker", "event_dt") \
  .pivot("data_item_name") \
  .agg(max("avg_ttm_data_value"))

pga_ROE = all_features.approxQuantile("return_on_equity_ratio", [0.1,0.5, 0.9], 0.1)
pga_ROE

pga_results = (
  all_features
    .withColumn(
      "peer_group_ROE", 
      when(round(col("return_on_equity_ratio")) >= round(lit(pga_ROE[2])), lit(1))
      .otherwise(
        when(round(col("return_on_equity_ratio")) >= round(lit(pga_ROE[1])), lit(2))
        .otherwise(
          lit(3)
        )
      )
    )
   .withColumn("rounded_peer_group_ROE", round(col("return_on_equity_ratio")))
   .withColumn("thres_1", round(lit(pga_ROE[0])))
   .withColumn("thres_2", round(lit(pga_ROE[1])))
   .withColumn("thres_3", round(lit(pga_ROE[2])))
)

pga_results = pga_results.select('ticker', 'return_on_equity_ratio', 'rounded_peer_group_ROE', 'thres_1', 'thres_2', 'thres_3', 'peer_group_ROE').distinct() 
ticks = spark.table("tempo.silver_tick_daily_smry")
forecast_input = ticks.join(pga_results, ['ticker'])

forecast_input \
  .write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode('overwrite') \
  .saveAsTable("tempo.gold_pga_forecast_input")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### `STEP3` - Introduce Technical Intraday Tick Data - Resample Months of Data for Volatility Forecasting

# COMMAND ----------

# DBTITLE 1,Introduce Technical Exchange Data
from pyspark.sql.functions import * 

trades = (
  spark.table("tempo.delta_tick_trades")
)

quotes = (
  spark.table("tempo.delta_tick_quotes")
)

liquidity_measures = TSDF(
  quotes
    .select('date', 'ticker', 'bid_price', 'ask_price', 'bid_size', 'ask_size', 'event_ts')
    .withColumn("spread", col("ask_price") - col("bid_price")), 
  partition_cols = ['ticker'], 
  ts_col = 'event_ts'
)

liquidity_measures = liquidity_measures.resample(freq = 'hr', func='closest_lead').df
liquidity_measures \
  .write \
  .option("overwriteSchema", "true") \
  .mode('overwrite') \
  .format("delta") \
  .saveAsTable("tempo.silver_tick_daily_smry")

# COMMAND ----------

display(spark.table("tempo.silver_tick_daily_smry"))

# COMMAND ----------

# DBTITLE 1,Use tempo to Collect Range Stats as Features for Forecasting Model
training_quotes = spark.table("tempo.gold_pga_forecast_input")

from tempo import *
training_quotes = TSDF(training_quotes, ts_col = 'event_ts', partition_cols = ['ticker'])
res_df = training_quotes.withRangeStats(colsToSummarize = ['spread'], rangeBackWindowSecs = 1000000).df.withColumn("spread", coalesce(col("spread"), lit(0)))

res_df \
  .write \
  .mode('overwrite') \
  .option("overwriteSchema", "true") \
  .format("delta") \
  .saveAsTable("tempo.silver_ticks_with_range_stats")

# COMMAND ----------

# MAGIC %sql select * from tempo.silver_ticks_with_range_stats where ticker= 'AAPL'

# COMMAND ----------

# DBTITLE 1,GPU Training Impact - 2.65X Cost Benefit vs CPU clusters (Switch tree_method='gpu_hist' for GPU testing)
from pyspark.sql.functions import *
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *
import pandas as pd
import mlflow
import mlflow.xgboost
import os
import sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
import numpy as np
from sklearn.metrics import mean_squared_error
from dateutil.parser import parse

import mlflow

# only use securities with enough history to project a forecast
data = spark.table("tempo.silver_ticks_with_range_stats") \
  .filter(col('count_spread') >= 64) \
  .select('event_ts', 'ticker', 'spread', 'bid_size', 'ask_size', 'mean_spread', 'min_spread', 'peer_group_ROE')

schema = StructType([StructField('event_ts', StringType(), True), 
                     StructField('ticker', StringType(), True),
                     StructField('spread', DoubleType(), True),
                     StructField('bid_size', DoubleType(), True),
                     StructField('ask_size', DoubleType(), True),
                     StructField('mean_spread', DoubleType(), True),
                     StructField('min_spread', DoubleType(), True),
                     StructField('peer_group', StringType(), True), 
                     StructField("confidence", DoubleType(), True)
                    ])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def trainXGBoost(data): 

    currency = data['ticker'].iloc[0]
    peer_group = data['peer_group_ROE'].iloc[0].astype(str)
    week = data['event_ts'].astype(str)
    data = data.fillna('0')

    with mlflow.start_run() as run:
      
      # could also apply feature engineering steps here as well
      # ML Flow setup
    
      # split data into train and test sets
      seed = 7
      
      #train, test = train_test_split(data)
      data = data.sort_values('event_ts')
      data = data.drop(['event_ts', 'ticker', 'peer_group_ROE'], axis=1).astype(float)
      X_train, X_test, y_train, y_test = train_test_split(data.drop(['spread'], axis=1), data['spread'], random_state=seed)
      y_train = y_train.fillna('0')
      
      # fit model no training data
      model = XGBRegressor(max_depth=5, tree_method='hist')
      model.fit(X_train, y_train)
  
      predicted_qualities = pd.DataFrame(model.predict(X_test), columns=["spread"])
      rmse = np.sqrt(mean_squared_error(y_test, model.predict(X_test)))
    
      # Log mlflow attributes for mlflow UI
      mlflow.log_param("max_depth", 5)
      mlflow.log_param("n_estimators", 100)
      mlflow.log_metric("RMSE", rmse)
      mlflow.set_tag('currency name', currency)
    
      predicted_qualities['ticker'] = currency
      predicted_qualities['event_ts'] = week
      predicted_qualities['bid_size'] = data['bid_size']
      predicted_qualities['ask_size'] = data['ask_size']
      predicted_qualities['mean_spread'] = data['mean_spread']
      predicted_qualities['min_spread'] = data['min_spread']
      predicted_qualities['peer_group'] = peer_group
      predicted_qualities['confidence'] = 1/rmse
      
      return predicted_qualities
  
pdata = data.groupBy('ticker').apply(trainXGBoost)
display(pdata)

# COMMAND ----------

pdata \
  .write \
  .mode('overwrite') \
  .option("overwriteSchema", "true") \
  .format("delta") \
  .saveAsTable("tempo.forecast_results")

# COMMAND ----------

# DBTITLE 1,Visualize Volatility Per Ticker
import matplotlib.pyplot as plt 

def line_plot(line1, line2, label1=None, label2=None, title='', lw=2):
    fig, ax = plt.subplots(1, figsize=(13, 7))
    ax.plot(line1, label=label1, linewidth=lw)
    ax.plot(line2, label=label2, linewidth=lw)
    ax.set_ylabel('price spread [USD]', fontsize=14)
    ax.set_title(title, fontsize=16)
    ax.legend(loc='best', fontsize=16);
    
def ts_train_test_split(df, test_size=0.2):
    split_row = len(df) - int(test_size * len(df))
    train_data = df.iloc[:split_row+1]
    test_data = df.iloc[split_row:]
    return train_data, test_data
  
train, test = ts_train_test_split(spark.table("tempo.forecast_results").filter(col("ticker") == "GM").toPandas(), test_size=0.2)
target_col = 'spread'
line_plot(train[target_col], test[target_col], 'training', 'test', title='')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### `STEP4` - Create Daily Summmary on Ticker Stats

# COMMAND ----------

symbol_trade_pct = spark.table("tempo.delta_tick_trades").join(spark.table("tempo.forecast_results").select('peer_group', 'ticker').distinct(), ['ticker']).groupBy('peer_group', 'ticker').agg(count('price').alias("trade_ct")).toPandas()

volatility_current_dt = spark.table("tempo.forecast_results").filter(col("event_ts").cast("date") == "2019-11-20").groupBy('ticker', 'peer_group', col('event_ts').cast("date")).agg(max("spread").alias("volatility")).toPandas()

# COMMAND ----------

from plotly.subplots import make_subplots
import plotly.graph_objects as go

fig = make_subplots(
    rows=2, cols=3,
    specs=[[{"type": "pie"}, {"type": "pie"}, {"type": "pie"}],
           [{"type": "bar"}, {"type": "bar"}, {"type": "bar"}]],
    subplot_titles=['Trade Volume Peer Group 1 - High Return on Equity', 'Trade Volume Peer Group 2 - Moderate Return on Equity', 'Trade Volume Peer Group 3 - Low Return on Equity', 'Volatility for High Return on Equity Peers', 'Volatility for Moderate Return on Equity Peers', 'Volatility for Low Return on Equity Peers']
)

fig.add_trace(go.Pie(values=symbol_trade_pct[symbol_trade_pct['peer_group'] == '1']['trade_ct'].tolist(), labels=symbol_trade_pct[symbol_trade_pct['peer_group'] == '1']['ticker'].tolist()), row=1, col=1)

fig.add_trace(go.Pie(values=symbol_trade_pct[symbol_trade_pct['peer_group'] == '2']['trade_ct'].tolist(), labels=symbol_trade_pct[symbol_trade_pct['peer_group'] == '2']['ticker'].tolist()), row=1, col=2)

fig.add_trace(go.Pie(values=symbol_trade_pct[symbol_trade_pct['peer_group'] == '3']['trade_ct'].tolist(), labels=symbol_trade_pct[symbol_trade_pct['peer_group'] == '3']['ticker'].tolist()), row=1, col=3)

fig.add_trace(go.Bar(x=volatility_current_dt[volatility_current_dt['peer_group'] == '1']['ticker'].tolist(), y=volatility_current_dt[volatility_current_dt['peer_group'] == '1']['volatility'].tolist()), row=2, col=1)

fig.add_trace(go.Bar(x=volatility_current_dt[volatility_current_dt['peer_group'] == '2']['ticker'].tolist(), y=volatility_current_dt[volatility_current_dt['peer_group'] == '2']['volatility'].tolist()), row=2, col=2)

fig.add_trace(go.Bar(x=volatility_current_dt[volatility_current_dt['peer_group'] == '3']['ticker'].tolist(), y=volatility_current_dt[volatility_current_dt['peer_group'] == '3']['volatility'].tolist()), row=2, col=3)

fig.update_layout(height=700, width=1500, showlegend=True)

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC + <a href="$./01_tempo_context">STAGE0</a>: Home page
# MAGIC + <a href="$./02_tempo_etl">STAGE1</a>: Design pattern for ingesting BPipe data
# MAGIC + <a href="$./03_tempo_volatility">STAGE2</a>: Introducing tempo for calculating market volatility
# MAGIC + <a href="$./04_tempo_spoofing">STAGE3</a>: Introducing tempo for market surveillance
# MAGIC ---
