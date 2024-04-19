/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.sedona.core.enums.FileDataSplitter;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.serde.SedonaKryoRegistrator;
import org.apache.sedona.core.spatialOperator.JoinQuery;
import org.apache.sedona.core.spatialRDD.LineStringRDD;
import org.apache.sedona.core.spatialRDD.PointRDD;
import org.apache.sedona.core.spatialRDD.PolygonRDD;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Geometry;
import java.io.Serializable;

/**
 * The Class Example.
 */
public class sedonaSpatialJoin
        implements Serializable {

    /**
     * The sc.
     */
    public static JavaSparkContext sc = null;

    /**
     * dataset location.
     */
    static String homePath;
    static String poiPath;
    static String gdeltPath;
    static String taxiPath;
    static String parksPath;
    static String lakesPath;
    static String roadsPath;

    /**
     * partition number.
     */
    static int partitionNum;

    static String indexDataType;

    static String queryDataType;

    static boolean isCache;
    static boolean considerBoundaryIntersection=true;
    static GridType partitionerType;
    static IndexType localIndexType;

    /**
     * dataset splitter.
     */
    static FileDataSplitter splitter = FileDataSplitter.WKT;

    static SpatialRDD indexRDD;
    static SpatialRDD queryRDD;

    /**
     * statistic result.
     */
    static long loadTime;
    static long analysisTime;
    static long partitionTime;
    static long indexTime;
    static long cacheTime;
    static long joinTime;
    static long totalTime;

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws Exception {

        initParameters(args);

        // load data
        long startLoadT = System.currentTimeMillis();
        indexRDD = loadData(indexDataType);
        queryRDD = loadData(queryDataType);
        JavaRDD indexFilterRDD = indexRDD.rawSpatialRDD.filter(i -> ((Geometry) i).isValid());
        JavaRDD queryFilterRDD = queryRDD.rawSpatialRDD.filter(i -> ((Geometry) i).isValid());

        indexRDD.setRawSpatialRDD(indexFilterRDD);
        queryRDD.setRawSpatialRDD(queryFilterRDD);
        System.out.println("[mylog]indexRDDSize:"+indexRDD.rawSpatialRDD.count());
        System.out.println("[mylog]queryRDDSize:"+queryRDD.rawSpatialRDD.count());
        long endLoadT = System.currentTimeMillis();
        loadTime = endLoadT-startLoadT;
        indexRDD.analyze();
        long endAnalysisT = System.currentTimeMillis();
        analysisTime = endAnalysisT-endLoadT;
        indexRDD.spatialPartitioning(partitionerType);
        queryRDD.spatialPartitioning(indexRDD.getPartitioner());
        System.out.println("[mylog]indexPartedRDD:"+indexRDD.spatialPartitionedRDD.first());
        System.out.println("[mylog]queryPartedRDD:"+queryRDD.spatialPartitionedRDD.first());
        long endPartitionT = System.currentTimeMillis();
        partitionTime = endPartitionT-endAnalysisT;
        indexRDD.buildIndex(localIndexType,true);
        System.out.println("[mylog]indexedRDD:"+indexRDD.indexedRDD.first());
        long endIndexT = System.currentTimeMillis();
        indexTime = endIndexT - endPartitionT;
        if (isCache) {
            indexRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
            queryRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());
        }
        long endCacheT = System.currentTimeMillis();
        cacheTime = endCacheT-endIndexT;
        long resultSize = JoinQuery.SpatialJoinQuery(indexRDD, queryRDD, true, considerBoundaryIntersection).count();
        long endJoinT = System.currentTimeMillis();
        joinTime = endJoinT-endCacheT;
        totalTime = endJoinT-startLoadT;
        sc.stop();
        String resultStr = ("************************ sedona ************************"
                + "\nindexDataType: " + indexDataType
                + "\nqueryDataType: " + queryDataType
                + "\npartitionerType: " + partitionerType
                + "\nlocalIndexType: " + localIndexType
                + "\nconsiderBoundaryIntersection: "+considerBoundaryIntersection
                + "\nisCache: " + isCache
                + "\nresultNum:" + resultSize
                + "\ntotalTime: " + totalTime + "ms"
                + "\nloadTime: " + loadTime + "ms"
                + "\nanalysisTime: " + analysisTime + "ms"
                + "\npartitionTime: " + partitionTime + "ms"
                + "\nindexTime: " + indexTime + "ms"
                + "\ncacheTime: " + cacheTime + "ms"
                + "\njoinTime: " + joinTime + "ms");
        System.out.println(resultStr);
    }

    private static void buildSparkContext(boolean local){

        if (sc != null)
            return ;
        SparkConf conf = new SparkConf();
        conf.setAppName("sedona spatial join");
        conf.set("spark.serializer", KryoSerializer.class.getName());
        conf.set("spark.kryo.registrator", SedonaKryoRegistrator.class.getName());
        if (local){
            conf.setMaster("local[*]");
        }
        sc = new JavaSparkContext(conf);

    }

    public static void initParameters(String[] args){
        boolean isLocal = true;
        if (args.length>0) {
            isLocal = false;
            homePath = "/home/yxy/data/spatialJoin/";
            poiPath = homePath+"osm21_pois.csv";
            roadsPath = homePath+"roads.csv";
            parksPath = homePath+"parks_polygon.csv";
            lakesPath = homePath+"lakes_polygon.csv";
            indexDataType=(args[0].split(";"))[0];
            queryDataType=(args[0].split(";"))[1];
            partitionNum=Integer.parseInt(args[1]);
            isCache = Boolean.parseBoolean(args[2]);
        } else {
            homePath = "D:\\data\\spatialJoin\\";
            poiPath = homePath+"osm21_pois_WKT_100k.csv";
            roadsPath = homePath+"lineStrings_100K.csv";
            parksPath = homePath+"parks_id_100k.csv";
            lakesPath = homePath+"lakes_id_100k.csv";
            indexDataType="parks";
            queryDataType="lakes";
            partitionNum=20;
            isCache=true;
        }
        buildSparkContext(isLocal);

        partitionerType = GridType.KDBTREE;
        localIndexType = IndexType.RTREE;
    }

    public static SpatialRDD loadData(String dataType){
        SpatialRDD dataRDD = null;
        switch (dataType) {
            case "pois":
                considerBoundaryIntersection = false;
                dataRDD = new PointRDD(sc, poiPath, 1, splitter, true, partitionNum);
                break;
            case "taxi":
                considerBoundaryIntersection = false;
                dataRDD = new PointRDD(sc, taxiPath, 1, splitter, true, partitionNum);
                break;
            case "gdelt":
                considerBoundaryIntersection = false;
                dataRDD = new PointRDD(sc, gdeltPath, 1, splitter, true, partitionNum);
                break;
            case "roads":
                dataRDD = new LineStringRDD(sc, roadsPath, 1, 1, splitter, true, partitionNum);
                break;
            case "parks":
                dataRDD = new PolygonRDD(sc, parksPath, 1, 1, splitter, true, partitionNum);
                break;
            case "lakes":
                dataRDD = new PolygonRDD(sc, lakesPath, 1, 1, splitter, true, partitionNum);
                break;
            default:
                System.out.println("unsupported dataTpe: "+dataType);
                break;
        }
        return dataRDD;
    }

}