
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


object ContentRecommend {
  //定义表名

  val PRODUCT="Product"
  val PRODUCT_CONTENT_RECS="ProductContentRecs"
  //用户推荐列表最大个数
  val USER_RECS_MAX_RECOMMENDATION=20
  def main(args: Array[String]): Unit = {
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
    val productTagDF: DataFrame = sparkSession.read
      .option("collection", PRODUCT)
      .option("uri", mongoConfig.uri)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]
      .map(x => (x.productId, x.name, x.tags.map(c => if (c == '|') ' ' else c)))
      .toDF("productId", "name", "tags").cache()
    //实例化一个分词器，默认按空格
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    //用分词器做转换
    val wordsData: DataFrame = tokenizer.transform(productTagDF)
    // 定义一个HashingTF 工具
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(500)
    // 用HashingTF 做处理
    val dataFrame: DataFrame = hashingTF.transform(wordsData)
    // 定义一个IDF工具
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 将词频数据传入，得到idf模型（统计文档）
    val iDFModel: IDFModel = idf.fit(dataFrame)
    // 用tf-idf算法得到新的特征矩阵
    val rescaledData: DataFrame = iDFModel.transform(dataFrame)
    // 从计算得到的rescaledData 中提取特征向量
    val unit: RDD[(Int, DoubleMatrix)] = rescaledData.map {
      case row =>
        (row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray)
    }
      .rdd
      .map(x => {
        (x._1, new DoubleMatrix(x._2))
      })
    unit.foreach(u=>
      println(u._2,u._1)
    )






  }

}
case class Product(
                    productId: Int,
                    name: String,
                    categories: String,
                    imageUrl: String,
                    tags: String
                  )

case class ProductContect(
                    productId: Int,
                    name: String,
                    categories: String,
                    imageUrl: String,
                    tags: String
                  )