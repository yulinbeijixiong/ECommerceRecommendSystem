package com.oujian.streaming

import java.util

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{MongoClientURI, casbah}
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("hadoop100")
  lazy val mongoClient = MongoClient(casbah.MongoClientURI("mongodb://hadoop100:27017/recommender"))
}

case class MongoConfig(uri: String, db: String)

//标准推荐
case class Recommendation(productId: Int, score: Double)

//用户推荐
case class UserRecs(userId: Int, recs: Seq[Recommendation])

//商品相识度
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object StreamingRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"

  //从redis 中取出用户的m个评分
  def getUserRecentlyRating(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
    //用户的队列中取出num个评分
    jedis.lrange("userId:" + userId.toString(), 0, num).map { item =>
      val userProductArr: Array[String] = item.split("\\:")
      (userProductArr(0).trim.toInt, userProductArr(1).trim.toDouble)

    }.toArray
  }

  //获取与最近商品列表相似的商品
  def getTopSimProducts(num: Int, productId: Int, userId: Int, simProducts: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig): Array[Int] = {
    //获取与当前商品相似的商品
    val allSimProduct: Array[(Int, Double)] = simProducts.get(productId).get.toArray
    //获取已经评分的商品
    val ratingExist: Array[Int] = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).find(
      MongoDBObject(
        "userId" -> userId
      )
    ).toArray.map { item =>
      item.get("production").toString.toInt
    }
    //从全部相似商品中过滤已经看过的相似商品
    allSimProduct.filter { sim =>
      !ratingExist.contains(sim._1)
    }.sortWith(_._2 > _._2).take(num).map(_._1)


  }
  //获取商品之间的相似度
  def getProductsSimSctore(simProduct: collection.Map[Int, Map[Int, Double]], productId: Int, topsimProduct: Int): Double = {
    simProduct.get(topsimProduct) match {
      case Some(sim) => sim.get(productId) match {
        case Some(score) => score
        case None => 0.0
      }
      case None =>0.0
    }

  }
  //log 替换底的方法
  def log(m:Int):Double={
    math.log(m)/math.log(10)
  }
  def computeProductScores(simProduct: collection.Map[Int, Map[Int, Double]], userRecentlyRatings: Array[(Int, Double)],
                           simTopProducts: Array[Int]): Array[(Int, Double)] = {
    //用于保存每一个待选商品和最近评分的每个商品的得分权重
    val score: ArrayBuffer[(Int, Double)] = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //用于保存每个商品的增强因子
    val increMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()
    //用于保存每个商品的减弱因子
    val decreMap: mutable.HashMap[Int, Int] = scala.collection.mutable.HashMap[Int, Int]()
    for (topsimProduct <- simTopProducts; userRecentlyRating <- userRecentlyRatings) {
      val simScore = getProductsSimSctore(simProduct, userRecentlyRating._1, topsimProduct)
      //筛选相似度大于0.6的商品
      if(simScore>0.6){
        //计算相似度与评分的乘积
        score +=((topsimProduct,simScore*userRecentlyRating._2))
        //加强因子计算
        if(userRecentlyRating._2>3){
          //是否存在评分大于3的商品
          increMap(topsimProduct) =increMap.getOrDefault(topsimProduct,0)+1
        }else{
          decreMap(topsimProduct) =decreMap.getOrDefault(topsimProduct,0)+1
        }
      }
    }
    //计算出推荐列表
    //商品id进行分组
    score.groupBy(_._1)
      .map{
        case(productId,item)=>
          (productId,item.map(_._2).sum/item.length+log(increMap.getOrDefault(productId,1))
          -log(decreMap.getOrDefault(productId,1)))
      }.toArray.sortWith(_._2>_._2)
  }

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop100:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val context: SparkContext = session.sparkContext
    val ssc = new StreamingContext(context, Seconds(2))
    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    import session.implicits._
    //商品相识度矩阵
    val simProductsMatrix: collection.Map[Int, Map[Int, Double]] = session.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_RECS_COLLECTION)
      .format("com.mongo.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      .map { recs =>
        (recs.productId, recs.recs.map(x => (x.productId, x.score)).toMap)

      }.collectAsMap()
    //将相识度矩阵设为广播变量

    val simProductsMatrixBrocast: Broadcast[collection.Map[Int, Map[Int, Double]]] = context.broadcast(simProductsMatrix)
    //设置kafka的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop100:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"

    )
    //连接kafka,并从中获取数据
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara)
    )
    //解析产生的数据  数据类型 UID|ProductID|SCORE|TIMESTAMP
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map { case msg =>
      val attr: Array[String] = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }
    //核心算法
    ratingStream.foreachRDD { rdd =>
      rdd.map { case (userId, productId, score, timestamp) =>
        println(">>>>>>>>>>>")
        //取出最近该用户的M次商品评分
        val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, userId, ConnHelper.jedis)
        //获取与当前商品相似的K个商品
        val simTopProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, productId, userId, simProductsMatrixBrocast.value)(mongoConfig)
        //计算待选商品的推荐优先级
        val stremRecs = computeProductScores(simProductsMatrixBrocast.value, userRecentlyRatings, simTopProducts)
        saveRecsToMongoDB(userId, stremRecs)(mongoConfig)
      }.count()
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def saveRecsToMongoDB(userId: Int, value: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    val streaRecsCollection: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)
    //删除之前的评分
    streaRecsCollection.findAndRemove(MongoDBObject("userId"->userId))

    streaRecsCollection.insert(MongoDBObject("userId"->userId,"rces"->
    value.map(x=>MongoDBObject("productId"->x._1,"score"->x._2))
    ))
  }

  def saveDataMongoDB(dataFrame: DataFrame, tableName: String)(mongoConfig: MongoConfig) = {
    val mongoClient: MongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    val collection: MongoCollection = mongoClient(mongoConfig.db)(tableName)
    collection.dropCollection()
    dataFrame.write
      .option("uri", mongoConfig.uri)
      .option("collection", tableName)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }
}
