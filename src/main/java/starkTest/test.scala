package starkTest
import dbis.stark._
import org.apache.spark.SparkContext
import org.apache.spark.SpatialRDD._
import org.apache.spark.api.java.JavaSparkContext
//import org.apache.spark.api.java.JavaSparkContext


object  test {

  import dbis.stark._
  import org.apache.spark.SparkContext
  import org.apache.spark.SpatialRDD._

  def main(args: Array[String]): Unit = {
    println("Hello, World!")
    val sc =new SparkContext("local[*]", "test")
    val parks = sc.textFile("D:\\whu\\baselines4spatialJoin-master\\data\\GridMesaData\\parks_id_100k.csv") // assume Schema ID;Name;WKT String
      .map(line => line.split("\t"))
      .map(arr => (STObject(arr(1)), arr(0).toInt )) // ( STObject, (Int, String) )

    // find all geometries that contain the given point
    val filtered = parks.contains(STObject("POINT( 8.474516 53.20708 )"))
    println(filtered.count())
  }

}
