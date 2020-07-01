package com.oujian.recommender.bean

class DataLoaderCaseBase {

}
case class Product(
                 productId:Int,
                 name:String,
                 categories:String,
                 imageUrl:String,
                 tags:String
                  )
case class Ratings(userId:Int,
                   produtId:Int,
                   score:Double,
                   timestamp:Long)