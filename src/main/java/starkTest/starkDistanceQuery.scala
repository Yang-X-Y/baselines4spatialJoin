package starkTest

import dbis.stark.{Distance, STObject, ScalarDistance}
import dbis.stark.spatial.indexed.RTreeConfig
import dbis.stark.spatial.indexed.live.LiveIndexedSpatialRDDFunctions
import dbis.stark.spatial.partitioner.BSPartitioner
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.spark.SpatialRDD.convertSpatialPlain
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

object starkDistanceQuery {
  var sc: SparkContext = null
  //path
  var homePath: String = _
  var poiPath: String = _
  var parksPath: String = _
  var lakesPath: String = _
  var roadsPath: String = _
  var buildingsPath: String = _

  var resultSize: Long = _
  var partitionNum: Int = _
  var indexDataType: String = _
  var queryDataType: String = _
  var indexRDD: RDD[(STObject, Int)] = _
  var queryRDD: RDD[STObject] = _
  var brodeCast: Broadcast[Array[STObject]] = _

  var loadTime: Long = _
  var partitionTime: Long = _
  var indexTime: Long = _
  var joinTime: Long = _
  var totalTime: Long = _

  var distance: Double = _

  def main(args: Array[String]): Unit = {
    initParameters(args)

    // load data
    val startLoadT = System.currentTimeMillis()
    indexRDD = loadData(indexDataType)
    queryRDD = loadQueryData(queryDataType)
    brodeCast = sc.broadcast(queryRDD.collect())

    val endLoadT = System.currentTimeMillis()
    loadTime = endLoadT - startLoadT

    // partition data
    val startPartitionT = System.currentTimeMillis()
    val indexRDDNoSample = BSPartitioner(indexRDD, sideLength = 1, maxCostPerPartition = 10, pointsOnly = false)
    val indexRDDPartitioned = indexRDD.partitionBy(indexRDDNoSample)
    val endPartitionT = System.currentTimeMillis()
    partitionTime = endPartitionT - startPartitionT

    // index data
    val startIndexT = System.currentTimeMillis()
    val indexRDDPartitionedIndex = indexRDDPartitioned.liveIndex(RTreeConfig(order = 5))
    val endIndexT = System.currentTimeMillis()
    indexTime = endIndexT - startIndexT

    // rangeQuery
    val startJoinT = System.currentTimeMillis()
    val joinResPlain = distanceQuery(indexRDDPartitionedIndex, brodeCast,distance)
    resultSize = joinResPlain
    val endJoinT = System.currentTimeMillis()
    joinTime = endJoinT - startJoinT
    totalTime = endJoinT - startLoadT

    sc.stop()

    val resultStr =
      s"""************************ starkSpatialJoin************************
         |indexDataType: $indexDataType
         |queryDataType: $queryDataType
         |resultNum: $resultSize
         |loadTime: $loadTime ms
         |partitionTime: $partitionTime ms
         |indexTime: $indexTime ms
         |joinTime: $joinTime ms
         |totalTime: $totalTime ms
         |""".stripMargin
    println(resultStr)
  }

  def distanceQuery(indexRDD: LiveIndexedSpatialRDDFunctions[STObject, Int], brodeCast: Broadcast[Array[STObject]],distance: Double): Long = {

    var num: Long = 0
    val queryRDD = brodeCast.value
    println("queryRDD size: " + queryRDD.length)
    for (q <- queryRDD) {
      val res = indexRDD.withinDistance(q,ScalarDistance(0), Distance.seuclid)
      println("res size: " + res.count())
      num += res.count()
    }
    num
  }

  def buildSparkContext(local: Boolean): Unit = {
    if (sc != null) return
    val conf = new SparkConf()
    conf.setAppName("stark spatial join")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    if (local) {
      conf.setMaster("local[*]")
    }
    sc = new SparkContext(conf)
  }

  def initParameters(args: Array[String]): Unit = {
    var isLocal = true
    if (!isLocal) {
      isLocal = false
      homePath = "/home/yxy/data/spatialJoin/"
      poiPath = homePath + "osm21_pois.csv"
      roadsPath = homePath + "roads.csv"
      parksPath = homePath + "parks_polygon.csv"
      lakesPath = homePath + "lakes_polygon.csv"
      buildingsPath = homePath + "buildings.csv"
      indexDataType = args(0).split(";")(0)
      queryDataType = args(0).split(";")(1)
      distance = args(1).toDouble
    } else {
      homePath = "D:\\whu\\baselines4spatialJoin-master\\data\\GridMesaData\\"
      poiPath = homePath + "osm21_pois_WKT_1M.csv"
      roadsPath = homePath + "roads_1M.csv"
      parksPath = homePath + "parks_id_100k.csv"
      lakesPath = homePath + "lakes_id_100k.csv"
      buildingsPath = homePath + "buildings_1M.csv"
      indexDataType = "parks"
      queryDataType = "lakes"
      partitionNum = 20
      distance = 1
    }
    buildSparkContext(isLocal)

  }

  def loadData(dataType: String): RDD[(STObject, Int)] = {
    dataType match {
      case "pois" =>
        sc.textFile(poiPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1)), arr(0).toInt)) // ( STObject, Int)
      case "roads" =>
        sc.textFile(roadsPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1)), arr(0).toInt)) // ( STObject, Int)
      case "parks" =>
        sc.textFile(parksPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1)), arr(0).toInt)) // ( STObject, Int)
      case "lakes" =>
        sc.textFile(lakesPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1)), arr(0).toInt)) // ( STObject, Int)
      case "buildings" =>
        sc.textFile(buildingsPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1)), arr(0).toInt)) // ( STObject, Int)
      case _ =>
        println("unsupported dataType: " + dataType)
        null
    }
  }

  def loadQueryData(dataType: String): RDD[STObject] = {
    dataType match {
      case "pois" =>
        sc.textFile(poiPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1))))
      case "roads" =>
        sc.textFile(roadsPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1))))
      case "parks" =>
        sc.textFile(parksPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1))))
      case "lakes" =>
        sc.textFile(lakesPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1))))
      case "buildings" =>
        sc.textFile(buildingsPath)
          .map(line => line.split("\t"))
          .map(arr => (STObject(arr(1))))
      case _ =>
        println("unsupported dataType: " + dataType)
        null
    }
  }
}
