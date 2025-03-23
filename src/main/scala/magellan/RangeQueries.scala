package magellan

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.{Row, SQLContext,DataFrame, SparkSession}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.rdd.RDD
import magellan._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.UUID
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import magellan.index._
import fastparse.all._
import fastparse.core.Parsed.{Failure, Success}

import scala.collection.mutable.ListBuffer
import magellan.{BoundingBox, Point, Polygon, PolygonDeserializer}

object RangeQueries {

  val homePath = "GridMesaData\\"
  val poisPath = homePath + "osm21_pois_WKT_1M.csv"
  val buildPath = homePath+ "building_1M.csv"
  val roadsPath = homePath + "roads_1M.csv"
  val parksPath = homePath + "parks_id_100k.csv"
  val lakesPath = homePath + "lakes_id_100k.csv"

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Magellan Range Queries")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "spatialspark.util.KyroRegistrator")
    val ss = SparkSession.builder()
      .appName("Spatial Range Query Example")
      .master("local[*]")
      .getOrCreate()
    val spark = ss.sparkContext
    import ss.implicits._


    def time[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block // call-by-name
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0) / 1E9 + " sec ")
      result
    }

    spatialRangePoint()
//    spatialRangeLineString()
//    spatialRangeRectangle()
    spatialRangePolygon()

    spark.stop()

    def spatialRangePoint() {

      println("************************ POINT Range Queries **************************************")
      val nQueries = 100
      var count = 0L
      val rangeQueryWindow1 = BoundingBox(-50.3010141441, -53.209588996, -24.9526465797, -30.1096863746)
      val rangeQueryWindow2 = BoundingBox(-54.4270741441, -53.209588996, -24.9526465797, -30.1096863746)
      val rangeQueryWindow3 = BoundingBox(-114.4270741441, -54.509588996, 42.9526465797, -27.0106863746)
      val rangeQueryWindow4 = BoundingBox(-82.7638020000, -54.509588996, 42.9526465797, 38.0106863746)
      val rangeQueryWindow5 = BoundingBox(-140.99778, -52.6480987209, 5.7305630159, 83.23324)
      val rangeQueryWindow6 = BoundingBox(-180.0, -90.0, 180.0, 90.0)

      val rawPoints = spark.textFile(poisPath).map { line =>
        val parts = line.split(" ")
        val longitude = parts(1).substring(1).toDouble
        val latitude = parts(2).substring(0, parts(2).length - 1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point").cache()

      val rawCount = rawPoints.count()

      // Dry run
      var t0 = System.nanoTime()
      for (i <- 1 to 20) {
        count = rawPoints.where($"point" withinRange rangeQueryWindow1).count()
      }
      var t1 = System.nanoTime()

      // Main measurements
      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawPoints.where($"point" withinRange rangeQueryWindow1).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawPoints.where($"point" withinRange rangeQueryWindow2).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawPoints.where($"point" withinRange rangeQueryWindow3).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawPoints.where($"point" withinRange rangeQueryWindow4).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawPoints.where($"point" withinRange rangeQueryWindow5).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawPoints.where($"point" withinRange rangeQueryWindow6).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      rawPoints.unpersist()

      println("***********************************************************************************")
      println("")
    }

    def spatialRangePolygon() {

      println("************************ POLYGON Range Queries **************************************")
      val nQueries = 100
      var count = 0L
      val rangeQueryWindow1 = BoundingBox(-20.204, -53.209588996, 17.9526465797, -30.1096863746)
      val rangeQueryWindow2 = BoundingBox(-20.204, -53.209588996, 20.4376465797, -30.1096863746)
      val rangeQueryWindow3 = BoundingBox(-74.4270741441, -34.609588996, 72.9526465797, -6.5906863746)
      val rangeQueryWindow4 = BoundingBox(-104.0938020000, -54.509588996, 118.9526465797, 40.2406863746)
      val rangeQueryWindow5 = BoundingBox(-174.4270741441, -34.609588996, 72.9526465797, 48.4396863746)
      val rangeQueryWindow6 = BoundingBox(-180.0, -90.0, 180.0, 90.0)

      val readBuildings = spark.textFile(buildPath, 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon")).cache()

      val rawCount = rawBuildings.count()
      var t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow1).count()
      }
      var t1 = System.nanoTime()

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow1).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")


      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow2).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow3).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow4).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow5).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow6).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      rawBuildings.unpersist()

      println("***********************************************************************************")
      println("")
    }

    def spatialRangeLineString() {

      println("************************ LineString Range Queries **************************************")
      val nQueries = 100
      var count = 0L
      val rangeQueryWindow1 = BoundingBox(-50.204, -53.209588996, -24.9526465797, -30.1096863746)
      val rangeQueryWindow2 = BoundingBox(-52.1270741441, -53.209588996, -24.9526465797, -30.1096863746)
      val rangeQueryWindow3 = BoundingBox(-94.4270741441, -34.609588996, 22.9526465797, -27.0106863746)
      val rangeQueryWindow4 = BoundingBox(-74.0938020000, -54.509588996, 42.9526465797, 38.0106863746)
      val rangeQueryWindow5 = BoundingBox(-150.99778, -52.6480987209, 7.2705630159, 83.23324)
      val rangeQueryWindow6 = BoundingBox(-180.0, -90.0, 180.0, 90.0)

      val readRoads = spark.textFile(parksPath, 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polyline", wkt($"text")("polyline")).cache()

      val rawCount = rawRoads.count()
      var t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawRoads.where($"polyline" withinRange rangeQueryWindow1).count()
      }
      var t1 = System.nanoTime()

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawRoads.where($"polyline" withinRange rangeQueryWindow1).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")


      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawRoads.where($"polyline" withinRange rangeQueryWindow2).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawRoads.where($"polyline" withinRange rangeQueryWindow3).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawRoads.where($"polyline" withinRange rangeQueryWindow4).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawRoads.where($"polyline" withinRange rangeQueryWindow5).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawRoads.where($"polyline" withinRange rangeQueryWindow6).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      rawRoads.unpersist()

      println("***********************************************************************************")
      println("")
    }

    def spatialRangeRectangle() {

      println("************************ Rectangle Range Queries **************************************")
      val nQueries = 100
      var count = 0L
      val rangeQueryWindow1 = BoundingBox(-20.204, -53.209588996, 17.9526465797, -30.1096863746)
      val rangeQueryWindow2 = BoundingBox(-20.204, -53.209588996, 20.4376465797, -30.1096863746)
      val rangeQueryWindow3 = BoundingBox(-74.4270741441, -34.609588996, 72.9526465797, -6.5906863746)
      val rangeQueryWindow4 = BoundingBox(-104.0938020000, -54.509588996, 118.9526465797, 40.2406863746)
      val rangeQueryWindow5 = BoundingBox(-174.4270741441, -34.609588996, 72.9526465797, 48.4396863746)
      val rangeQueryWindow6 = BoundingBox(-180.0, -90.0, 180.0, 90.0)

      val readRectangles = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawBuildings = readRectangles.toDF("text").withColumn("polygon", wkt($"text")("polygon")).cache()

      val rawCount = rawBuildings.count()
      var t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow1).count()
      }
      var t1 = System.nanoTime()

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow1).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")


      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow2).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow3).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow4).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow5).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count = rawBuildings.where($"polygon" withinRange rangeQueryWindow6).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count)
      println("Selection Ratio: " + ((count * 100.0) / rawCount) + " %")
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / 1E9) + " queries/min")

      rawBuildings.unpersist()

      println("***********************************************************************************")
      println("")
    }
  }
}
