#!/bin/bash

function spatialspark {
  spark-submit \
    --master spark://huawei5:7077 \
    --class spatialsparkSpatialJoins \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --executor-cores 5 \
    --num-executors 24 \
    --conf spark.driver.memory=15g \
    --conf spark.executor.memory=40g \
    --conf spark.driver.maxResultSize=60g \
    --conf spark.kryoserializer.buffer.max=1g \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.driver.extraClassPath="/opt/module/spark-2.4.4/HBaseJar/*:/opt/module/hadoop-2.7.7/etc/hadoop/:/opt/module/hbase-1.4.9/conf/" \
    --conf spark.executor.extraClassPath="/opt/module/spark-2.4.4/HBaseJar/*:/opt/module/hadoop-2.7.7/etc/hadoop/:/opt/module/hbase-1.4.9/conf/" \
    /home/yxy/spatialJoin/SpatialJoinBaselines-1.0-SNAPSHOT.jar ${dataset} ${spatialsparkPartNum} ${isCache}
}

function sedona {
  spark-submit \
    --master spark://huawei5:7077 \
    --class sedonaSpatialJoin \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --executor-cores 5 \
    --num-executors 24 \
    --conf spark.driver.memory=15g \
    --conf spark.executor.memory=40g \
    --conf spark.driver.maxResultSize=60g \
    --conf spark.kryoserializer.buffer.max=1g \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.driver.extraClassPath="/opt/module/spark-2.4.4/HBaseJar/*:/opt/module/hadoop-2.7.7/etc/hadoop/:/opt/module/hbase-1.4.9/conf/" \
    --conf spark.executor.extraClassPath="/opt/module/spark-2.4.4/HBaseJar/*:/opt/module/hadoop-2.7.7/etc/hadoop/:/opt/module/hbase-1.4.9/conf/" \
    /home/yxy/spatialJoin/SpatialJoinBaselines-1.0-SNAPSHOT.jar ${dataset} ${sedonaPartNum} ${isCache}
}

spatialsparkPartNum=1024
sedonaPartNum=750
isCache=true

datasets="parks;lakes parks;roads parks;pois"

for dataset in $datasets
do
  eval spatialspark
  eval sedona
done




