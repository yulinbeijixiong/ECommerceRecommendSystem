package com.oujian.recommender

import com.mongodb.casbah.commons.MongoDBObject

import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}



object DataLoader {
  val PRODUCT_DATA_PATH = "C:\\Users\\Ann\\IdeaProjects\\sparkmall\\ECommerceRecommendSystem\\recommend\\DataLoader\\src\\main\\resources\\products.csv"
  val RATINGS_DATA_PATH = "C:\\Users\\Ann\\IdeaProjects\\sparkmall\\ECommerceRecommendSystem\\recommend\\DataLoader\\src\\main\\resources\\ratings.csv"
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    //定义到配置参数
    var config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop100:27017/recommender",
      "mongo.db" -> "recommender"
    )
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val productRDD: RDD[String] = sparkSession.sparkContext.textFile(PRODUCT_DATA_PATH)
    val ratingsRDD: RDD[String] = sparkSession.sparkContext.textFile(RATINGS_DATA_PATH)
    import sparkSession.implicits._
    val productDF: DataFrame = productRDD.map { line =>
      val productInfoArray: Array[String] = line.split("\\^")
      Product(productInfoArray(0).toInt, productInfoArray(1).trim, productInfoArray(2).trim,
        productInfoArray(4).trim, productInfoArray(6).trim)
    }.toDF()
    val ratingsDF: DataFrame = ratingsRDD.map {
      line =>
        val ratings: Array[String] = line.split(",")
        Ratings(ratings(0).toInt, ratings(1).toInt, ratings(2).toDouble, ratings(3).toLong)
    }.toDF()
    implicit val mongoConfig: MongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)
    storeDataInMongoDB(productDF,ratingsDF)(mongoConfig)
    sparkSession.stop()


  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig) = {
    //获取连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //创建表
    val productCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection: MongoCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
    //如果已存在，删除该表
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    //将当前数据写入到MongoDB
    productDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //对数据表建立索引
    productCollection.createIndex(MongoDBObject("productId"->1))
    ratingCollection.createIndex(MongoDBObject("userId"->1))
    ratingCollection.createIndex(MongoDBObject("productId"->1))
    //MongoBD的连接
    mongoClient.close()
  }
}


case class MongoConfig(uri: String, db: String)

case class Product(
                    productId: Int,
                    name: String,
                    categories: String,
                    imageUrl: String,
                    tags: String
                  )

case class Ratings(userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Long)