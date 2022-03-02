// Databricks notebook source
// MAGIC %md
// MAGIC # Modernizing Investment Data Platforms - context
// MAGIC 
// MAGIC The appetite for investment in a range of asset classes is at a historic high in 2020. As of July 2020, "Retail traders make up nearly 25% of the stock market following COVID-driven volatility" ([source](https://markets.businessinsider.com/news/stocks/retail-investors-quarter-of-stock-market-coronavirus-volatility-trading-citadel-2020-7-1029382035)). As investors gain access and trade alternative assets such as cryptocurrency, trading volumes have skyrocketed and created new data challenges. Moreover, cutting edge research is no longer restricted to institutional investors on Wall Street - today’s world of investing extends to digital exchanges in Silicon Valley, data-centric market makers, and retail brokers that are investing increasingly in AI-powered tools for investors. Data lakes have become standard for building financial data products and research, but the lack of blueprints for how to build an enterprise data lake in the cloud lakes hinder the adoption to scalable AI (such as volatility forecasting). Through a series of design patterns and real world examples, we address these core challenges and show benchmarks to illustrate why building investment data platforms on Databricks leads to cost savings and improved financial products. We also introduce [tempo](https://github.com/databrickslabs/tempo), an open source library for manipulating time series at scale.
// MAGIC 
// MAGIC <img src="https://github.com/databrickslabs/tempo/raw/master/tempo%20-%20light%20background.svg">
// MAGIC 
// MAGIC ---
// MAGIC + <a href="$./01_tempo_context">STAGE0</a>: Home page
// MAGIC + <a href="$./02_tempo_etl">STAGE1</a>: Design pattern for ingesting BPipe data
// MAGIC + <a href="$./03_tempo_volatility">STAGE2</a>: Introducing tempo for calculating market volatility
// MAGIC + <a href="$./04_tempo_spoofing">STAGE3</a>: Introducing tempo for market surveillance
// MAGIC ---
// MAGIC <ricardo.portilla@databricks.com>
