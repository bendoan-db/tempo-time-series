// Databricks notebook source
// MAGIC %md
// MAGIC # Modernizing Investment Data Platforms - ETL
// MAGIC 
// MAGIC The appetite for investment in a range of asset classes is at a historic high in 2020. As of July 2020, "Retail traders make up nearly 25% of the stock market following COVID-driven volatility" ([source](https://markets.businessinsider.com/news/stocks/retail-investors-quarter-of-stock-market-coronavirus-volatility-trading-citadel-2020-7-1029382035)). As investors gain access and trade alternative assets such as cryptocurrency, trading volumes have skyrocketed and created new data challenges. Moreover, cutting edge research is no longer restricted to institutional investors on Wall Street - today’s world of investing extends to digital exchanges in Silicon Valley, data-centric market makers, and retail brokers that are investing increasingly in AI-powered tools for investors. Data lakes have become standard for building financial data products and research, but the lack of blueprints for how to build an enterprise data lake in the cloud lakes hinder the adoption to scalable AI (such as volatility forecasting). Through a series of design patterns and real world examples, we address these core challenges and show benchmarks to illustrate why building investment data platforms on Databricks leads to cost savings and improved financial products. We also introduce [tempo](https://github.com/databrickslabs/tempo), an open source library for manipulating time series at scale.
// MAGIC 
// MAGIC ---
// MAGIC + <a href="$./01_tempo_context">STAGE0</a>: Home page
// MAGIC + <a href="$./02_tempo_etl">STAGE1</a>: Design pattern for ingesting BPipe data
// MAGIC + <a href="$./03_tempo_volatility">STAGE2</a>: Introducing tempo for calculating market volatility
// MAGIC + <a href="$./04_tempo_spoofing">STAGE3</a>: Introducing tempo for market surveillance
// MAGIC ---
// MAGIC <ricardo.portilla@databricks.com>

// COMMAND ----------

// MAGIC %md
// MAGIC ## Context
// MAGIC Fundamental data, loosely defined as economic and financial factors used to measure a company’s intrinsic value, is available today from most financial data vendors. Two of the most common sources that are widely used include Factset and S&P Market Intelligence Platform. Both of these sources make data available via FTP, API, and a SQL server database. Since data is available via database for factor analysis, there are easy options for ingestion into Delta Lake using third parties (see [partner data integrations](https://docs.databricks.com/integrations/ingestion/index.html)) and / or native cloud capabilities. Bloomberg is one of the industry standards for market data, reference data, and hundreds of other feeds. In order to show an example of API-Based ingestion from a Bloomberg data subscription, the B-PIPE (Bloomberg data API for accessing market data sources) [emulator](https://github.com/Robinson664/bemu) will be used. The Java market data subscription client code has been modified in the code below to publish events into a Kinesis stream using the AWS SDK. 
// MAGIC 
// MAGIC NOTE: The code below is meant to be placed into 

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Write B-PIPE to Streaming Service
// MAGIC 
// MAGIC Contained in the cells below is Java client code to write API subscription data (in this case quote subscription data for a list of securities) to a Kinesis stream with one partition. Note that this is a modification of the Bloomberg API Emulator example to simply print out to System.out. This does not run out-of-the-box in a notebook but rather is expected to be extracted and placed into an IDE with the BEmu emulator. Simply replace the `RunMarketDataSubscription` with the code below to run as a producer of BPIPE data to a Kinesis stream. 

// COMMAND ----------

// DBTITLE 1,Define Portfolio
val portfolioListOfSecurities = Array("SPY US EQUITY", "AAPL 150117C00600000 EQUITY", "AMD US EQUITY")

// COMMAND ----------

// DBTITLE 1,Create a BPIPE Producer
//------------------------------------------------------------------------------
// <copyright project="BEmu_Java_Examples" file="/BEmu_Java_Examples/com/com/examples/RunMarketDataSubscription.java" company="Jordan Robinson">
//     Copyright (c) 2013 Jordan Robinson. All rights reserved.
//
//     The use of this software is governed by the Microsoft Public License
//     which is included with this distribution.
// </copyright>
//------------------------------------------------------------------------------

package com.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;
import com.bloomberglp.blpapi.Schema;

import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import java.nio.ByteBuffer;
import com.amazonaws.auth.BasicAWSCredentials;


public class RunMarketDataSubscription
{
	private final static java.text.NumberFormat formatter = java.text.NumberFormat.getCurrencyInstance();
	
	public static void RunExample() throws Exception
	{
		RunMarketDataSubscription._fields = new ArrayList<String>();
		RunMarketDataSubscription._fields.add("BID");
		RunMarketDataSubscription._fields.add("ASK");
		
		SessionOptions soptions = new SessionOptions();
		soptions.setServerHost("127.0.0.1");
		soptions.setServerPort(8194);

		RunMarketDataSubscription r = new RunMarketDataSubscription();
		MyEventHandler mevt = r.new MyEventHandler();				
        Session session = new Session(soptions, mevt);
		
        session.startAsync();
	}

    private static List<String> _fields;
	
	public class MyEventHandler implements EventHandler
	{		
		public void processEvent(Event event, Session session)
		{
			switch (event.eventType().intValue())
			{
				case Event.EventType.Constants.SESSION_STATUS:
				{
					MessageIterator iter = event.messageIterator();
					while (iter.hasNext())
					{
						Message message = iter.next();
						if (message.messageType().equals("SessionStarted"))
						{
							try
							{
								session.openServiceAsync("//blp/mktdata", new CorrelationID(-9999));
							}
							catch (Exception e)
							{
								System.err.println("Could not open //blp/mktdata for async");
							}
						}
					}
					break;
				}
				
				case Event.EventType.Constants.SERVICE_STATUS:
				{
					MessageIterator iter = event.messageIterator();
					while (iter.hasNext())
					{
						Message message = iter.next();
						
						if (message.messageType().equals("ServiceOpened"))
						{
							SubscriptionList slist = new SubscriptionList();
							slist.add(new Subscription(portfolioListOfSecurities[0], RunMarketDataSubscription._fields));
							slist.add(new Subscription(portfolioListOfSecurities[1], RunMarketDataSubscription._fields));
							slist.add(new Subscription(portfolioListOfSecurities[2], RunMarketDataSubscription._fields));
							
							try
							{
								session.subscribe(slist);
							}
							catch (Exception e)
							{
								System.err.println("Subscription error");
							}
						}
					}
					break;
				}

				case Event.EventType.Constants.SUBSCRIPTION_DATA:
				case Event.EventType.Constants.PARTIAL_RESPONSE:
				case Event.EventType.Constants.RESPONSE:	
				{
					try {
						dumpEvent(event);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} // Handle Partial Response
					break;
				}
				
                case Event.EventType.Constants.SUBSCRIPTION_STATUS:
                {
					MessageIterator iter = event.messageIterator();
					while (iter.hasNext())
					{
						Message msg = iter.next();
						
						try
						{	
							boolean fieldExceptionsExist = msg.messageType().toString().equals("SubscriptionStarted") &&
									msg.hasElement("exceptions", true);
							
							boolean securityError = msg.messageType().toString().equals("SubscriptionFailure") &&
									msg.hasElement("reason", true);
							
	                        if (fieldExceptionsExist)
	                        {
	                            Element elmExceptions = msg.getElement("exceptions");
	                            for (int i = 0; i < elmExceptions.numValues(); i++)
	                            {
	                                Element elmException = elmExceptions.getValueAsElement(i);
	                                String fieldId = elmException.getElementAsString("fieldId");
	
	                                Element elmReason = elmException.getElement("reason");
	                                String source = elmReason.getElementAsString("source");
	                                int errorCode = elmReason.getElementAsInt32("errorCode");
	                                String category = elmReason.getElementAsString("category");
	                                String description = elmReason.getElementAsString("description");
	
	                                System.err.println("field error: ");
	                                System.err.println(String.format("\tfieldId = %s", fieldId));
	                                System.err.println(String.format("\tsource = %s", source));
	                                System.err.println(String.format("\terrorCode = %s", errorCode));
	                                System.err.println(String.format("\tcategory = %s", category));
	                                System.err.println(String.format("\tdescription = %s", description));
	                            }
	                        }
	                        else if (securityError)
	                        {
	                            String security = msg.topicName();

	                            Element elmReason = msg.getElement("reason");
	                            String source = elmReason.getElementAsString("source");
	                            int errorCode = elmReason.getElementAsInt32("errorCode");
	                            String category = elmReason.getElementAsString("category");
	                            String description = elmReason.getElementAsString("description");

	                            System.err.println("security not found: ");
	                            System.err.println(String.format("\tsecurity = %s", security));
	                            System.err.println(String.format("\tsource = %s", source));
	                            System.err.println(String.format("\terrorCode = %s", errorCode));
	                            System.err.println(String.format("\tcategory = %s", category));
	                            System.err.println(String.format("\tdescription = %s", description));
	                        }
						}
						catch(Exception ex)
						{
							System.err.println(ex.getMessage());
						}

					}
                	break;
                }
				
			}
		}
	
		private void dumpEvent(Event event) throws Exception
		{
			System.out.println();
			System.out.println("eventType=" + event.eventType());
			MessageIterator messageIterator = event.messageIterator();
			SimpleDateFormat fmt = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
			
			while (messageIterator.hasNext())
			{
				Message message = messageIterator.next();
				
				String security = message.topicName();
				String sequenceNumberOfPreviousRecord = "0";
				for(int i = 0; i < RunMarketDataSubscription._fields.size(); i++)
				{
					//This ignores the extraneous fields in the response
					String field = RunMarketDataSubscription._fields.get(i);
					if(message.hasElement(field, true)) //be careful, excludeNullElements is false by default
					{
						Element elmField = message.getElement(field);
						
						String strValue;
						if(elmField.datatype() == Schema.Datatype.FLOAT64)
							strValue = RunMarketDataSubscription.formatter.format(elmField.getValueAsFloat64());
						else
							strValue = elmField.toString().trim();
						
						String output = String.format("%s, %s, %s", fmt.format(new Date()), security, strValue);
						System.out.println(output);

						// Use Kinesis client to write records retrieved from API to Kinesis stream
						AmazonKinesisClient kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials("XXXXXXXX", "XXXXXXXX"));
						String kinesisEndpointUrl = "https://kinesis.us-east-1.amazonaws.com";
						String regionName = "us-east-1";
						kinesisClient.setEndpoint(kinesisEndpointUrl);

						PutRecordRequest putRecordRequest = new PutRecordRequest();
						putRecordRequest.setStreamName( "databricks-bpipe" );
						putRecordRequest.setData(ByteBuffer.wrap( output.getBytes() ));
						putRecordRequest.setPartitionKey( "mkt-data-partitionKey" );
						putRecordRequest.setSequenceNumberForOrdering( sequenceNumberOfPreviousRecord );
						PutRecordResult putRecordResult = kinesisClient.putRecord( putRecordRequest );
						sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber();

					}
				}
			}
			System.out.println();
		}
	}
}


// COMMAND ----------

// Use Kinesis client to write records retrieved from API to Kinesis stream
AmazonKinesisClient kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(AWS_accessKey, AWS_secretKey));
String kinesisEndpointUrl = "https://kinesis.us-east-1.amazonaws.com";
String regionName = "us-east-1";
kinesisClient.setEndpoint(kinesisEndpointUrl);

// Create PutRecordRegust with bytes from API request (output) and include sequence number
PutRecordRequest putRecordRequest = new PutRecordRequest();
putRecordRequest.setStreamName( "databricks-bpipe" );
putRecordRequest.setData(ByteBuffer.wrap( output.getBytes() ));
putRecordRequest.setPartitionKey( "mkt-data-partitionKey" );
putRecordRequest.setSequenceNumberForOrdering( sequenceNumberOfPreviousRecord );
PutRecordResult putRecordResult = kinesisClient.putRecord( putRecordRequest );
sequenceNumberOfPreviousRecord = putRecordResult.getSequenceNumber();

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Consumer Code

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val kinesis = spark.readStream
  .format("kinesis")
  .option("streamName", "databricks-bpipe")
  .option("region", "us-east-1")
  .option("initialPosition", "latest")
  .option("awsAccessKey", "xxxx")
  .option("awsSecretKey", "xxxx")
  .load()

// COMMAND ----------

val schema = StructType(Seq(
  StructField("response", StringType, true)
))

// COMMAND ----------

val df = kinesis.withColumn("mktdata", col("data").cast("string")).withColumn("event_ts", split(col("mktdata"), ",")(0)).withColumn("ticker", split(split(col("mktdata"), ",")(1), " ")(1)).withColumn("quote_pr", translate(split(col("mktdata"), ",")(2), "$", "")).withColumn("event_dt", col("event_ts").cast("timestamp").cast("date"))

// COMMAND ----------

df
  .repartition(1)
  .writeStream
  .partitionBy("event_dt")
  .format("delta")
  .option("path", "/tmp/databricks/bpipe")
  .option("checkpointLocation", "/tmp/databricks/bpipe_cp")
  .start()

// COMMAND ----------

display(spark.read.format("delta").load("/tmp/databricks/bpipe"))

// COMMAND ----------

// MAGIC %md
// MAGIC ---
// MAGIC + <a href="$./01_tempo_context">STAGE0</a>: Home page
// MAGIC + <a href="$./02_tempo_etl">STAGE1</a>: Design pattern for ingesting BPipe data
// MAGIC + <a href="$./03_tempo_volatility">STAGE2</a>: Introducing tempo for calculating market volatility
// MAGIC + <a href="$./04_tempo_spoofing">STAGE3</a>: Introducing tempo for market surveillance
// MAGIC ---
