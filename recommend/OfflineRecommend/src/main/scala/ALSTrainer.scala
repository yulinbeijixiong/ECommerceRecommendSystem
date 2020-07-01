import OfflineRecommend.MONGODB_RATING_COLLECTION
import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {

  def getRMES(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    val userProducts: RDD[(Int, Int)] = data.map(item => (item.user, item.product))
    //得到预测模型
    val predictRating: RDD[Rating] = model.predict(userProducts)
    //真实数据
    val real: RDD[((Int, Int), Double)] = data.map(item => ((item.user, item.product), item.rating))
    //预测值
    val predict: RDD[((Int, Int), Double)] = predictRating.map(item => ((item.user, item.product), item.rating))
    sqrt(
      real.join(predict).map { case ((userId, productId), (real, pre)) =>
        //真实值和预测值之间的差
        val err = real - pre
        err * err
      }.mean()
    )
  }


  def adjustALSParams(trainRDD: RDD[Rating], testRDD: RDD[Rating]) = {
    val result = for (rank <- Array(2, 5, 10, 20, 50, 100); lambda <- Array(2, 1, 0.5, 0.1, 0.01, 0.001))
      yield {
        val model: MatrixFactorizationModel = ALS.train(trainRDD, rank, 5, lambda)
        val rmse = (model, testRDD)
        (rank, lambda, rmse)
      }
    result.toList.foreach(r => {
      println(r._3, r._2, r._1)
    })
  }

  def main(args: Array[String]): Unit = {
    //定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop100:27017/recommender",
      "mongo.db" -> "recommender"

    )
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommend")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    import sparkSession.implicits._
    //从mongoDB 取出业务数据
    val ratings: RDD[Rating] = sparkSession.read
      .option("collection", MONGODB_RATING_COLLECTION)
      .option("uri", mongoConfig.uri)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => Rating(rating.userId, rating.productId, rating.score))
    val array: Array[RDD[Rating]] = ratings.randomSplit(Array(0.8, 0.2))
    val testRDD: RDD[Rating] = array(1)
    val trainRDD: RDD[Rating] = array(0)
    //输出最优参数
    adjustALSParams(trainRDD, testRDD)
    sparkSession.close()
  }
}
