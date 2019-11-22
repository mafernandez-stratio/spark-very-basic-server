package utad

import java.io.PrintStream
import java.net.ServerSocket

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.BufferedSource
import scala.util.Try

object VowelsCounter extends App {

  assert(args.length > 0, "Usage: VowelsCounter <Port> [<Master>]")

  val sparkConf = new SparkConf().setAppName("Utad Vowels counter")//.setMaster("local[*]")
  val sparkConfWithMaster = Try(args(1)).map{ m =>
    sparkConf.setMaster(m).setJars(Seq("target/very-basic-server-0.1-SNAPSHOT-jar-with-dependencies.jar"))
  }.getOrElse(sparkConf)
  val sc = new SparkContext(sparkConfWithMaster)

  val server = new ServerSocket(args(0).toInt)

  while(true){

    val s = server.accept()

    val in = new BufferedSource(s.getInputStream()).getLines()
    val out = new PrintStream(s.getOutputStream())

    Try{
      in.foreach { l =>

        val resultRDD = sc.parallelize(l.split(" ").toSeq).
          flatMap(word => word.toLowerCase.toCharArray).
          filter(Set('a', 'e', 'i', 'o', 'u').contains).
          map((_, 1)).
          reduceByKey(_ + _)
          .map(e => s"${e._1} - ${e._2}").cache()

        val result = resultRDD.collect().mkString("; ")

        out.println(result)
        out.flush()
      }

      s.close()
    }

  }

  sys.addShutdownHook{
    sc.stop()
  }

}