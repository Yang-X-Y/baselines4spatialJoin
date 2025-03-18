package starkTest
import dbis.stark.STObject

import dbis.stark.spatial.partitioner.{BSPartitioner, SpatialGridPartitioner}
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.spark.SpatialRDD.convertSpatialPlain
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

object starkSpatialJoin {

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
    var indexRDD: RDD[( STObject, Int )] = _
    var queryRDD: RDD[( STObject, Int )]= _

    var loadTime: Long = _
    var partitionTime: Long = _
    var indexTime: Long = _
    var joinTime: Long = _
    var totalTime: Long = _

    def main(args: Array[String]): Unit = {
      initParameters(args)

      // load data
      val startLoadT = System.currentTimeMillis()
      indexRDD = loadData(indexDataType)
      queryRDD = loadData(queryDataType)


      val parti = BSPartitioner(indexRDD, 0.5, 100, pointsOnly = true)
      val indexRDDIndex=indexRDD.liveIndex(parti, order = 5)
      val result = indexRDDIndex.contains(STObject("POINT( 8.474516 53.20708 )"))
      println(result.count())


      sc.stop()
      val resultStr = s"""************************ sedona ************************
                         |indexDataType: $indexDataType
                         |queryDataType: $queryDataType
                         |resultNum: $resultSize
                         |totalTime: $totalTime ms
                         |loadTime: $loadTime ms
                         |partitionTime: $partitionTime ms
                         |indexTime: $indexTime ms
                         |joinTime: $joinTime ms
                         |""".stripMargin
      println(resultStr)
    }

    def buildSparkContext(local: Boolean): Unit = {
      if (sc != null) return
      val conf = new SparkConf()
      conf.setAppName("sedona spatial join")
      conf.set("spark.serializer", classOf[KryoSerializer].getName)
      conf.set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      if (local) {
        conf.setMaster("local[*]")
      }
      sc = new SparkContext(conf)
    }

    def initParameters(args: Array[String]): Unit = {
      var isLocal = true
      if (args.length > 0) {
        isLocal = false
        homePath = "/home/yxy/data/spatialJoin/"
        poiPath = homePath + "osm21_pois.csv"
        roadsPath = homePath + "roads.csv"
        parksPath = homePath + "parks_polygon.csv"
        lakesPath = homePath + "lakes_polygon.csv"
        buildingsPath = homePath + "buildings.csv"
        indexDataType = args(0).split(";")(0)
        queryDataType = args(0).split(";")(1)
        partitionNum = args(1).toInt
      } else {
        homePath = "D:\\whu\\baselines4spatialJoin-master\\data\\GridMesaData\\"
        poiPath = homePath + "osm21_pois_WKT_1M.csv"
        roadsPath = homePath + "roads_1M.csv"
        parksPath = homePath + "parks_id_100k.csv"
        lakesPath = homePath + "lakes_id_100k.csv"
        buildingsPath = homePath + "buildings_1M.csv"
        indexDataType = "parks"
        queryDataType = "parks"
        partitionNum = 20
      }
      buildSparkContext(isLocal)

    }

    def loadData(dataType: String): RDD[( STObject, Int )] = {
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
}
