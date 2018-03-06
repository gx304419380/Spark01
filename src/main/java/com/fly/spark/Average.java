package com.fly.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.tools.cmd.Spec;

import java.util.*;
import java.util.stream.Collector;

/**
 * @author XXX
 * @since 2018-03-06
 */
public class Average {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("maxmin").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = jsc.textFile("C:\\Users\\guoxiang.HDSC\\Desktop\\average.txt");

        textFile.mapToPair(line -> new Tuple2<>(line.split(" ")[0], Integer.parseInt(line.split(" ")[1])))
                .reduceByKey((a, b) -> a + b)
                .collect()
                .forEach(t -> System.out.println(t._1() + "\t" + t._2()));

        textFile.mapToPair(line -> new Tuple2<>(line.split(" ")[0], Integer.parseInt(line.split(" ")[1])))
                .groupByKey()
                .mapToPair(info -> {
                    List<Integer> list = new ArrayList<>();
                    info._2().forEach(salary -> list.add(salary));
                    list.sort(Comparator.comparingInt(a -> a));
                    return new Tuple2<>(info._1(), list);
                })
                .collect()
                .forEach(info -> {
                    System.out.println(info._1());
                    System.out.println(info._2());
                });
        //求平均 方法一： groupByKey
        textFile.mapToPair(line -> new Tuple2<>(line.split(" ")[0], Integer.parseInt(line.split(" ")[1])))
                .groupByKey()
                .mapToPair(info -> {
                    double sum = 0;
                    double count = 0;
                    Iterator<Integer> it = info._2().iterator();
                    while (it.hasNext()) {
                        sum += it.next();
                        count++;
                    }
                    double ave = sum / count;
                    return new Tuple2<>(info._1(), ave);
                })
                .collect()
                .forEach(System.out::println);

        //求平均 方法二： combineByKey
        textFile.mapToPair(line -> new Tuple2<>(line.split(" ")[0], Integer.parseInt(line.split(" ")[1])))
                .combineByKey(score -> new Tuple2<>(score, 1),  // 将score映射为一个元组
                        (t, score) -> new Tuple2<>(t._1() + score, t._2() + 1), //分区内聚合，
                        (a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))   //分区间聚合
                .mapToPair(info -> new Tuple2<>(info._1(), info._2()._1()/info._2()._2()))
                .collect()
                .forEach(System.out::println);


    }
}
