package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf= new SparkConf().setAppName("TP Streaming").setMaster("local[*]");
        JavaStreamingContext sc=new JavaStreamingContext(conf, Durations.seconds(8));
        JavaReceiverInputDStream <String> dStream1=sc.socketTextStream("localhost",8080);
        JavaDStream<String> dStreamWords=dStream1.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String,Integer> dPairStream=dStreamWords.mapToPair(m->new Tuple2<>(m,1));
        JavaPairDStream<String,Integer> dStramWordCount= dPairStream.reduceByKey((a,b)->a+b);
        dStramWordCount.print();
        sc.start();
        sc.awaitTermination();


    }
}