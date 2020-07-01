package com.oujian.recommend.statisticsRecommender


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}


object StatisticsRecommender {
  val MONGODB_RATING_COLLECTION="Rating"
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"


  def main(args: Array[String]): Unit = {
    val config=Map(
      "spark.core"->"local[*]",
      "mongo.uri"->"mongodb://hadoop100/recommender",
      "mongo.db"->"recommender"
    )
    val sparkConf: SparkConf = new SparkConf().setAppName("statisticsRecommender").setMaster(config("spark.core"))
    sparkConf.set("spark.executor.memory","5g")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    import  sparkSession.implicits._
    val ratingDF: DataFrame = sparkSession.read
      .option("uri", config("mongo.uri"))
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Ratings]
      .toDF()
    ratingDF.createOrReplaceTempView("ratings")
    //历史最热门商品
    val hisHotProduct: DataFrame = sparkSession.sql("select productId,count(productId) as count from ratings group by productId ")
    saveDataMongoDB(hisHotProduct,RATE_MORE_PRODUCTS)(mongoConfig)
    val format = new SimpleDateFormat("yyyyMM")
    //获取近期热门商品
    sparkSession.udf.register("changeDate",(x:Long)=> format.format(new Date(x*1000L)).toLong)
    sparkSession.sql("select productId,changeDate(timestamp) as yearMonth from ratings")
      .createOrReplaceTempView("ratingsDateFormat")
    val recentHotProduct: DataFrame = sparkSession.sql("select productId,count(productId) as count from ratingsDateFormat group by yearMonth,productId order by yearMonth desc,count desc")
    saveDataMongoDB(recentHotProduct,RATE_MORE_RECENTLY_PRODUCTS)(mongoConfig)
    val avgScoreProduct: DataFrame = sparkSession.sql("select productId,avg(score) avgScore from ratings group by productId")
    saveDataMongoDB(avgScoreProduct,AVERAGE_PRODUCTS)(mongoConfig)


    sparkSession.stop()
  }
  def saveDataMongoDB(dataFrame: DataFrame,tableName:String)(mongoConfig: MongoConfig)={
    val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
     val collection: MongoCollection = mongoClient(mongoConfig.db)(tableName)
    collection.dropCollection()
    dataFrame.write
      .option("uri",mongoConfig.uri)
      .option("collection",tableName)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }
}
case class MongoConfig(uri: String, db: String)
case class Ratings(userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Long)