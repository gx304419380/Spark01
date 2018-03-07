package com.fly.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author XXX
 * @since 2018-03-06
 */
public class WordCount {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaPairRDD<String, Integer> resultRDD = jsc.textFile("res/wordcount.txt")
                .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        resultRDD.collect()
                .stream()
                .sorted(Comparator.comparingInt(Tuple2::_2))
                .forEach(System.out::println);

        resultRDD.sample(true, 0.02, System.currentTimeMillis())
                .foreach(x -> System.out.println(x));
    }
}
