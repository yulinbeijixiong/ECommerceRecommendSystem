import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

object OfflineRecommend {


  val MONGODB_RATING_COLLECTION="Rating"
  //定义表名
  val USER_RECS="UserRecs"
  val PRODUCT_RECS="ProductRecs"
  //用户推荐列表最大个数
  val USER_RECS_MAX_RECOMMENDATION=20

  def main(args: Array[String]): Unit = {
    //定义配置
    val config=Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop100:27017/recommender",
      "mongo.db" -> "recommender"

    )
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommend")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import sparkSession.implicits._
    //从mongoDB 取出业务数据
    val ratingRDD: RDD[(Int, Int, Double)] = sparkSession.read
      .option("collection", MONGODB_RATING_COLLECTION)
      .option("uri", mongoConfig.uri)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(rating => (rating.userId, rating.productId, rating.score))
    //从业务数据中取出用户集和商品集
    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()
    val productionRDD: RDD[Int] = ratingRDD.map(_._2).distinct()
    //创建训练数据集
    val tarinRDD: RDD[Rating] = ratingRDD.map(rdd=>Rating(rdd._1,rdd._2,rdd._3))
    //rank 隐语义模型个数 ，iterations 迭代次数，正则修正项
    val model: MatrixFactorizationModel = ALS.train(tarinRDD,3,5,0.1)
    println(model)
//    //创建预测矩阵(用户与商品形成笛卡尔集)
    val userProductRDD: RDD[(Int, Int)] = userRDD.cartesian(productionRDD)
//    //预测结果

    val predictResult: RDD[Rating] = model.predict(userProductRDD)
    predictResult.foreach{
      pre=>
        println(pre.user)
    }
    val userRescDF: DataFrame = predictResult
      //过滤评分为0 的预测结果
      .filter { rating =>
      rating.rating > 0
    }
      //转变格式便于对用户进行分组
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map { case (userId, resc) =>
        UserRecs(userId, resc.toList.sortWith(_._2 > _._2).take(USER_RECS_MAX_RECOMMENDATION)
          .map { case (productId, score) =>
            Recommendation(productId, score)
          })
      }.toDF()
    //保存到mongoDB
    println(userRescDF)
    userRescDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
//    计算商品相识度
//    获取商品的特征矩阵
    val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map { case (productId, feature) =>
      (productId, new DoubleMatrix(feature))
    }
    //特征矩阵的笛卡尔积-》这个商品与其他商品的相识度
    val productDF: DataFrame = productFeatures.cartesian(productFeatures)
      //排除与自己本身形成的笛卡尔积
      .filter { case (a, b) => a._1 != b._1 }
      //计算相识度
      .map {
      case (a, b) =>
        val simScore = this.consinSim(a._2, b._2)
        (a._1, (b._1, simScore))
    }
      //只去相识度大于60%
      .filter(_._2._2 > 0.6)
      .groupByKey()
      .map { case (productId, items) =>
        ProductRecs(productId, items.toList.map { item =>
          Recommendation(item._1, item._2)
        })
      }.toDF()
    productDF.write
        .option("uri",mongoConfig.uri)
        .option("collection",PRODUCT_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()
    sparkSession.stop()
  }
  //计算两个向量的余弦值
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix):Double = {
    product1.dot(product2)/(product1.norm2()*product2.norm2())
  }


}
case class ProductRating(userId:Int,productId:Int,score:Double,timestamp:Long)
case class MongoConfig(uri:String,db:String)
//标准推荐对象
case class Recommendation(productId:Int,score:Double)
//用户推荐列表
case class UserRecs(userId:Int,cecs:Seq[Recommendation])
//商品相识度
case class ProductRecs(productId:Int,cece:Seq[Recommendation])