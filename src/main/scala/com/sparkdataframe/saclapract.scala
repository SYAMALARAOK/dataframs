package com.sparkdataframe

import org.apache.spark.sql.SparkSession

object saclapract {
  def main(args:Array[String]): Unit ={
      val spark = SparkSession.builder().master("local").appName("spa").getOrCreate()
      case class Sport(id:Int,name:String,city:String,sex:String,age:Int)
   // spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter","+")
 // val sc1 = spark.sparkContext.parallelize(Array(1,2,3,4))
 // sc1.foreach(println)
//val sc2 = sc1.map(x=>x+1)
 // sc2.foreach(println)


    /*val sc3 = spark.sparkContext.textFile("syam.txt").map(_.split(",")).map(x => Sport(x(0),x(1).toInt))
    val sc4=sc3.toDF()
   sc4.show()
    sc3.foreach(println)
  val l = (0 to 100).toList
    l.foreach(println)
    val l1 = l.map(_+1)
    l1.foreach(println)*/
    val sc5 = spark.read.format("csv").option("delimiter",",").option("inferSchema","true").load("syam.txt")
     //val l1 = List(List(1,2,3),List(4,5,6))
    //val l2 = l1.flatten
     //l2.foreach(println)
sc5.show()
    sc5.printSchema()
    sc5.groupBy("_c4").count().show()

   }

}
