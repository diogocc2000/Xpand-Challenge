package com.company.project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.isnan
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.substring_index
import org.apache.spark.sql.functions.when

object App {
  
  def main(args : Array[String]) {

    val spark = SparkSession
        .builder()
        .config("spark.driver.memory", value = "1G")
        .config("spark.testing.memory", value = "2147480000")           // Without this configuration i get a memory error 
        .master("local[*]")                                             // Run the project localy
        .appName("Project")
        .getOrCreate()


    var df_google_reviews =  dataFrameGoogleStoreReviews(spark)     
    var df_google_store = dataFrameGoogleStore(spark)

    df_google_reviews.show()
    

    //Part 1 

    var df_1 = dataFrameAverageSentimentPolarity(spark, df_google_reviews)
    df_1.show()


    //Part 2
    
    var df_best_apps = dataFrameBestApps(spark,df_google_store)
    df_best_apps.show()


    //Part 3

    var df_3 = dataFrame3(spark,df_google_store)
    df_3.show()

    //Part 4

    var df_cleaned = dataFrameGoogleStoreCleaned(spark, df_1, df_3)
    df_cleaned.show()

    //Part 5

    var df_4 = dataFrameGoogleStoreMetrics(spark,df_cleaned)
    df_4.show()



  }


  def dataFrameGoogleStore(spark : SparkSession) : DataFrame = {

    val customSchema = StructType(Array(
    StructField("App", StringType, false),
    StructField("Category", StringType, true),
    StructField("Rating", DoubleType, true),
    StructField("Reviews", LongType, true),
    StructField("Size", StringType, true),
    StructField("Installs", StringType, true),
    StructField("Type", StringType, true),
    StructField("Price", StringType, true),
    StructField("Content_Rating", StringType, true),
    StructField("Genres", StringType, true),
    StructField("Last_Updated", StringType, true),
    StructField("Current_Version", StringType, true),
    StructField("Minimum_Android_Version", StringType, true)
    ))

    var dataFrame = spark.read.option("header",true)
            .option("delimiter", ",")
            .schema(customSchema)
            .csv("src/main/scala/com/company/project/google-play-store-apps/googleplaystore.csv")

    // Remove duplicates and replace nan values on column "Rating" with null
    dataFrame = dataFrame.dropDuplicates().withColumn("Rating", when(isnan(col("Rating")), lit(null.asInstanceOf[String])).otherwise(col("Rating")))
    
    return dataFrame

  }

  def dataFrameGoogleStoreReviews(spark : SparkSession) : DataFrame = {

    val customSchema = StructType(Array(
    StructField("App", StringType, false),
    StructField("Translated_Review", StringType, true),
    StructField("Sentiment", StringType, true),
    StructField("Sentiment_Polarity", DoubleType, true),
    StructField("Sentiment_Subjectivity", DoubleType, true),
    ))


    var dataFrame = spark.read.option("header",true)
            .schema(customSchema)
            .csv("src/main/scala/com/company/project/google-play-store-apps/googleplaystore_user_reviews.csv")  

    // Remove duplicates                  
    dataFrame = dataFrame.dropDuplicates()

    return dataFrame
  }


  def dataFrameAverageSentimentPolarity(spark : SparkSession, df : DataFrame) : DataFrame = {

    var dataFrame = df.groupBy("App").avg("Sentiment_Polarity").withColumnRenamed("avg(Sentiment_Polarity)","Average_Sentiment_Polarity")

    //Replace null values with 0(default value for this column)
    dataFrame= dataFrame.withColumn("Average_Sentiment_Polarity", coalesce(col("Average_Sentiment_Polarity"), lit(0)))

    return dataFrame
  }

  def dataFrameBestApps(spark : SparkSession, df : DataFrame) : DataFrame = {

    
    var dataFrame = df.filter(col("Rating") >= 4.0).sort(col("Rating").desc)

    //dataFrame.repartition(1).write.option("header",true).option("delimiter", "§").csv("src/main/scala/com/company/project/best_apps.csv")

    return dataFrame
  }

  def dataFrame3(spark : SparkSession, df : DataFrame) : DataFrame = {

    var dataFrame = df.groupBy("App").agg(collect_list("Category").as("Categories"), max("Reviews").as("Reviews"))

    var df_temp = df.withColumn("Reviews", coalesce(col("Reviews"), lit(0)))                     //Replace null values with 0(default value for this column)

    df_temp = df_temp.withColumn("Size",regexp_replace(col("Size"),"M","").cast(DoubleType))     //Remove "M" and cast(String -> Double)
    df_temp = df_temp.withColumn("Price",substring_index(col("Price"),"$",-1).cast(DoubleType))  //Remove "$" and cast(String -> Double)
    df_temp = df_temp.withColumn("Price",col("Price")*0.9)                                       //Convert from dollars to euros
    df_temp = df_temp.withColumn("Genres",split(col("Genres"),";"))                              //Convert string to array of strings (delimiter: ";")
    df_temp = df_temp.withColumn("Last_Updated",regexp_replace(col("Last_Updated"),",",""))   
    df_temp = df_temp.withColumn("Last_Updated",to_timestamp(col("Last_Updated"),"MMMM dd yyyy")) //Convert string to timestamp


    dataFrame = dataFrame.join(df_temp, Seq("App","Reviews")).drop("Category")
    dataFrame = dataFrame.select("App","Categories","Rating","Reviews","Size","Installs","Type",
    "Price","Content_Rating","Genres","Last_Updated","Current_Version","Minimum_Android_Version")

    return dataFrame
  }


  def dataFrameGoogleStoreCleaned(spark : SparkSession, df_1 : DataFrame, df_3 : DataFrame) : DataFrame = {

    var dataFrame = df_3.join(df_1,Seq("App"))
    //dataFrame.repartition(1).write.parquet("src/main/scala/com/company/project/googleplaystore_cleaned") // default compression is gzip

    return dataFrame
  }


  def dataFrameGoogleStoreMetrics(spark : SparkSession, df : DataFrame) : DataFrame = {

    var dataFrame = df.withColumn("Genre",explode(col("Genres"))).drop("Genres")         

    dataFrame = dataFrame.groupBy("Genre").agg(count("App").as("Count"),avg("Rating").as("Average_Rating"),
        avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity"))
    
    //dataFrame.repartition(1).write.parquet("src/main/scala/com/company/project/googleplaystore_metrics") // default compression is gzip

    return dataFrame
  }





}
