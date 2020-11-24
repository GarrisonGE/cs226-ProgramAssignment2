package edu.ucr.cs.cs226.jge013
import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.reflect.internal.util.NoFile.file

object App {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("SparkApp").setMaster("local")
    val sc = new SparkContext(conf)

    try{
      val data = sc.textFile(args(0)).cache()
      //task1: Find the average number of bytes for lines of each response code
      //Map: (key,value) = (code,(byte,1))
      val code = data.map(s=>(s.split("\t")(5),(s.split("\t")(6).toDouble,1)))
      //Reduce:(key,value) = (code,(total_byte,total_number))
      val totalByte =code.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
      //Map:(key,value) = (code,avgByte)
      val avgByte = totalByte.map(s=>(s._1,s._2._1/s._2._2)).collect()
      val file1 = new PrintWriter(new File("task1.txt"))
      avgByte.foreach(data => {
        print("Code " + data._1 + ", average number of bytes = " + data._2 + "\n")
        file1.write("Code " + data._1 + ", average number of bytes = " + data._2 + "\n")
      })
      file1.close()
      //task2: Find Pairs of requests that ask for the same URL, same host, and happened within an hour of each other
      // 1. t1.host = t2.host
      // 2. t1.url = t2.url
      // 3. |t1.timestamp - t2.timestamp| <= 3600
      // t1 != t2
      //Map: (key,value) = ((host,url),(record))
      val request = data.map(s=>((s.split("\t")(0),s.split("\t")(4)),s))
      val filterData = request.join(request)
                          .filter(s => (s._2._1 != s._2._2))
                          .filter(s => (s._2._1.split("\t")(0) == s._2._2.split("\t")(0)))
                          .filter(s => (s._2._1.split("\t")(4) == s._2._2.split("\t")(4)))
                          .filter(s => math.abs(s._2._1.split("\t")(2).toLong - s._2._2.split("\t")(2).toLong)<= 3600 )
      val pairData = filterData.map(s => s._2._1 + "\t" + s._2._2).collect()
      val file2 = new PrintWriter(new File("task2.txt"))
      pairData.foreach(data => {
        print(data + "\n")
        file2.write(data + "\n")
      })
      file2.close()

    }finally(sc.stop())
  }
}
