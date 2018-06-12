import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}


object Assignment26part2 {
   def main(args: Array[String]): Unit = {
   
   println(" Spark SQL Streaming")
   val conf = new SparkConf().setMaster("local[2]").setAppName("Assignment26part2")
   val sc = new SparkContext(conf)
   sc.setLogLevel("WARN")
   
   val offwordList:Set[String] = Set("bad","lazy","obesity") //set of offensive words
   println(s"$offwordList")
   val ssc = new StreamingContext(sc, Seconds(15))
   val lines = ssc.socketTextStream("localhost", 9999)
   
   val words = lines.flatMap(_.split(" ")).map(w => w)
   .filter(item => (offwordList.contains(item))).map(x => (x,1)).reduceByKey(_+_)
   words.print()
   ssc.start()
   ssc.awaitTermination()
   }
}
