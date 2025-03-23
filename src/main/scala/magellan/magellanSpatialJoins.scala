package magellan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.{SparkConf, SparkContext}

import java.util.UUID
//import org.apache.spark.sql.catalyst.expressions.JsonPathParser.{Failure, Success}

object
magellanSpatialJoins {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Magellan Spatial Joins")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setMaster("local[*]")

    val spark = new SparkContext(conf)
    val sparkSession = SparkSession.builder().appName("Magellan Spatial Joins").getOrCreate()
    import sparkSession.implicits._
    println(s"Spark version: ${sparkSession.version}")

    def time[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block // call-by-name
      val t1 = System.nanoTime()
      println("Join time: " + (t1 - t0) / 1E9 + " sec ")
      result
    }

    val homePath = "GridMesaData\\"
    val poisPath = homePath + "osm21_pois_WKT_1M.csv"
    val roadsPath = homePath + "roads_1M.csv"
    val parksPath = homePath + "parks_id_100k.csv"
    val lakesPath = homePath + "lakes_id_100k.csv"
    val partNum=50
//    pointPoint()
//    pointLineString()
//    pointRectangle()
//    pointPolygon(poisPath,parksPath)
//    linestringLineString()
//    linestringRectangle()
//    linestringPolygon()
//    rectangleRectangle()
//    polygonRectangle()
    polygonPolygon(parksPath,lakesPath,partNum)

    spark.stop()

    def pointLineString() {

      println("*************************** Point-LineString ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val rawPoints = spark.textFile("/data/points_200M.csv").map { line =>
        val parts = line.split(",")
        val longitude = parts(0).toDouble
        val latitude = parts(1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point")
      val roads = rawPoints.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val buildings = rawBuildings.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      // This is only a filter based on Z-curve value
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")
      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def pointPoint() {

      println("*************************** Point-Point ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val rawPoints = spark.textFile("/data/points_200M.csv").map { line =>
        val parts = line.split(",")
        val longitude = parts(0).toDouble
        val latitude = parts(1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point")
      val roads = rawPoints.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val rawBuildings = spark.textFile("/data/points_200M.csv").map { line =>
        val parts = line.split(",")
        val longitude = parts(0).toDouble
        val latitude = parts(1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point")
      val buildings = rawBuildings.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      // indexedRoads("point") intersects indexedBuildings("point") -> this enforces exact check
      count = indexedRoads.join(indexedBuildings, indexedBuildings("curve") === indexedRoads("curve")).where((indexedRoads("relation") === "Intersects") or (indexedRoads("point") intersects indexedBuildings("point"))).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")
      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def pointPolygon(poisPath:String, parksPath:String) {

      println("*************************** Point-Polygon ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)
      val rawPois = spark.textFile(poisPath,1024)
              .map { line =>line.split("\t")(1)}
              .toDF("text")
              .withColumn("point", wkt($"text")("point"))
      val pois = rawPois.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedPois = pois.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count1 = indexedPois.count()


      val readParks = spark.textFile(parksPath, 1024)
      val rawParks = readParks.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val parks = rawParks.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedParks = parks.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedParks.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      // $"point" intersects $"polygon" enforces exact check
      count = indexedPois.join(indexedParks, indexedParks("curve") === indexedPois("curve")).where((indexedPois("relation") === "Intersects") or ($"point" intersects $"polygon")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedPois.unpersist()
      indexedParks.unpersist()
    }

    def pointRectangle() {

      println("*************************** Point-Rectangle ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val rawPoints = spark.textFile("/data/points_200M.csv").map { line =>
        val parts = line.split(",")
        val longitude = parts(0).toDouble
        val latitude = parts(1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point")
      val roads = rawPoints.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedRoads.join(indexedBuildings, indexedBuildings("curve") === indexedRoads("curve")).where((indexedRoads("relation") === "Intersects") or ($"point" intersects $"polygon")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def linestringLineString() {

      println("*************************** LineString-LineString ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val roads = rawRoads.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val buildings = rawBuildings.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      // This is only a filter based on Z-curve value, also intersects predicate has not been implemented for LineString-LineString yet
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Contains")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def linestringPolygon() {

      println("*************************** LineString-Polygon ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val roads = rawRoads.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/buildings_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedRoads.join(indexedBuildings, indexedBuildings("curve") === indexedRoads("curve")).where((indexedRoads("relation") === "Intersects") or ($"polyline" intersects $"polygon")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def linestringRectangle() {

      println("*************************** LineString-Rectangle ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val roads = rawRoads.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedRoads.join(indexedBuildings, indexedBuildings("curve") === indexedRoads("curve")).where((indexedRoads("relation") === "Intersects") or ($"polyline" intersects $"polygon")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def polygonPolygon(parksPath:String, lakesPath:String,partNum:Int) {

      println("*************************** Polygon-Polygon ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readParks = spark.textFile(parksPath, partNum).map { line =>line.split("\t")(1)}
      val rawParks = readParks.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val parks = rawParks.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedParks = parks.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count1 = indexedParks.count()
      println("parksNum:"+count1)

      val readLakes = spark.textFile(lakesPath, partNum)
      val rawLakes = readLakes.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val lakes = rawLakes.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedLakes = lakes.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedLakes.count()
      println("lakesNum:"+count2)
      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedLakes.join(indexedParks, indexedParks("curve") === indexedLakes("curve")).where((indexedLakes("relation") === "Intersects") or (indexedLakes("polygon") intersects indexedParks("polygon"))).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")
      println("resultsNum:"+count)
      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedLakes.unpersist()
      indexedParks.unpersist()
    }

    def polygonRectangle() {

      println("*************************** Rectangle-Polygon ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val roads = rawRoads.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/buildings_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedRoads.join(indexedBuildings, indexedBuildings("curve") === indexedRoads("curve")).where((indexedRoads("relation") === "Intersects") or (indexedRoads("polygon") intersects indexedBuildings("polygon"))).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def rectangleRectangle() {

      println("*************************** Rectangle-Rectangle ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val roads = rawRoads.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedRoads.join(indexedBuildings, indexedBuildings("curve") === indexedRoads("curve")).where((indexedRoads("relation") === "Intersects") or (indexedRoads("polygon") intersects indexedBuildings("polygon"))).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)

      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }
  }
}
