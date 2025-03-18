package starkTest;

import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;

public class test1 {

    public static void main(String[] args) {
        JavaSparkContext sc = null;
        SparkConf conf = new SparkConf();
        conf.setAppName("sedona spatial join");
        conf.set("spark.serializer", KryoSerializer .class.getName());
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator .class.getName());
        conf.setMaster("local[*]");
        sc = new JavaSparkContext(conf);

//        countries = sc.textFile(".../data/GridMesaData/buildings_1M.csv") // assume Schema ID;Name;WKT String
//                .map(line => line.split(';'))
//      .map(arr => (STObject(arr(2)), (arr(0).toInt, arr(1))) // ( STObject, (Int, String) )
//
//        // find all geometries that contain the given point
//        val filtered = countries.contains(STObject("POINT( 50.681898 10.938838 )"))
    }

}
