import com.vividsolutions.jts.geom.{Envelope, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spatialspark.util.MBR
import spatialspark.join.PartitionedSpatialJoin
import spatialspark.operator.SpatialOperator
import spatialspark.partition.bsp.BinarySplitPartitionConf
import spatialspark.partition.fgp.FixedGridPartitionConf
import spatialspark.partition.stp.SortTilePartitionConf

import scala.util.Try

object spatialsparkSpatialJoins {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("SpatialSpark Spatial Joins")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.kryo.registrator", "spatialspark.util.KyroRegistrator")
        // conf.setMaster("local[*]")
        val sc = new SparkContext(conf)

        val homePath = "/home/yxy/data/spatialJoin/"
        val poiPath = homePath + "osm21_pois.csv"
        val roadsPath = homePath + "roads.csv"
        val parksPath = homePath + "parks_polygon.csv"
        val lakesPath = homePath + "lakes_polygon.csv"

        val leftDataType = args(0).split(";").apply(0)
        val rightDataType = args(0).split(";").apply(1)
        val partNum = args(1).toInt
        val isCache = args(2).toBoolean


        query(leftDataType, rightDataType,partNum)

        sc.stop()

        def query(leftDataType: String, rightDataType: String, partNum: Int) {
            val leftPath = leftDataType match  {
                case "pois"=> poiPath
                case "parks"=> parksPath
                case "lakes"=> lakesPath
                case "roads"=> roadsPath
            }
            val rightPath = rightDataType match  {
                case "pois"=> poiPath
                case "parks"=> parksPath
                case "lakes"=> lakesPath
                case "roads"=> roadsPath
            }
            var resultNum = 0L
            val extentString = ""
            val method = "stp"
            val startLoad = System.nanoTime()
            val leftData = sc.textFile(leftPath, partNum).map(x => x.split("\t")).zipWithIndex()
            val leftGeometryById = leftData.map(x => (x._2, Try(new WKTReader().read(x._1.apply(1))))).filter(x=>x._2.isSuccess && x._2.get.isValid).map(x => (x._1, x._2.get)).cache()
            val rightData = sc.textFile(rightPath, partNum).map(x => x.split("\t")).zipWithIndex()
            val rightGeometryById = rightData.map(x => (x._2, Try(new WKTReader().read(x._1.apply(1))))).filter(x=>x._2.isSuccess && x._2.get.isValid).map(x => (x._1, x._2.get)).cache()
            val countLeft = leftGeometryById.count()
            val countRight = rightGeometryById.count()
            val endLoad = System.nanoTime()

            val loadTime=(endLoad - startLoad) / 1E9
            var matchedPairs: RDD[(Long, Long)] = sc.emptyRDD
            val extent = extentString match {
            case "" =>
                val temp = leftGeometryById.map(x => x._2.getEnvelopeInternal).map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY)).reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
                val temp2 = rightGeometryById.map(x => x._2.getEnvelopeInternal).map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY)).reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
                (temp._1 min temp2._1, temp._2 min temp2._2, temp._3 max temp2._3, temp._4 max temp2._4)
            case _ =>
                (extentString.split(":").apply(0).toDouble, extentString.split(":").apply(1).toDouble,
                    extentString.split(":").apply(2).toDouble, extentString.split(":").apply(3).toDouble)
            }
            val endAnalyse = System.nanoTime()
            val analyseTime=(endAnalyse-endLoad) / 1E9

            val partConf = method match {
            case "stp" =>
                val dimX = 32
                val dimY = 32
                val ratio = 0.3
                new SortTilePartitionConf(dimX, dimY, new MBR(extent._1, extent._2, extent._3, extent._4), ratio, true)
            case "bsp" =>
                val level = 32
                val ratio = 0.1
                new BinarySplitPartitionConf(ratio, new MBR(extent._1, extent._2, extent._3, extent._4), level, true)
            case _ =>
                val dimX = 32
                val dimY = 32
                new FixedGridPartitionConf(dimX, dimY, new MBR(extent._1, extent._2, extent._3, extent._4))
            }
            matchedPairs = PartitionedSpatialJoin(sc, leftGeometryById, rightGeometryById, SpatialOperator.Intersects, 0.0, partConf)
            resultNum = matchedPairs.count()
            val endJoin = System.nanoTime()
            val joinTime = (endJoin - endAnalyse) / 1E9
            val totalTime = (endJoin - startLoad)/ 1E9
            val resultStr = "************************ spatialSpark ************************"+
                    "\nleftData: " + leftDataType +
                    "\nrightData: " + rightDataType +
                    "\nresultNum:" + resultNum +
                    "\ntotalTime: " + totalTime + "s"+
                    "\nloadTime: " + loadTime + "s" +
                    "\nanalyseTime: " + analyseTime + "s" +
                    "\njoinTime: " + joinTime + "s"
            println(resultStr)
            leftGeometryById.unpersist()
            rightGeometryById.unpersist()
        }
    }
}
