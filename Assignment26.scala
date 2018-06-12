
import java.io.{File, FileInputStream}

import scala.io.Source._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.sql.SparkSession


object Assignment26 {
  def main(args: Array[String]): Unit = {
    def filterodd(input: String):Double={ //function to filter the odd numbers
    val words = input.split("")
    var number: Double = 0.0
    for(x <- words)
    {
     try{
       val value = x.toDouble
       number = number + value
        }catch
        {
          case ex: Exception => {}
        }
    }
    return number
    }
    println("Spark SQL Streaming")
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSteamingExample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(15))
    val lines = ssc.socketTextStream("localhost", 9999)
    
    println("Performing local word count")
    val evensum = lines.filter{x => filterodd(x)%2 == 0} //To get the even sum
    val linesum = evensum.map{y => filterodd(y)} //To get the line sum
    println("Lines with even sum:")
    evensum.print()
    println("")
    println("sum of numbers in even line")
    linesum.reduce((a,b) => a+b).print()
    ssc.start()
    ssc.awaitTermination()
}
}

