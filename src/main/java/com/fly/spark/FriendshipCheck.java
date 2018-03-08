package com.fly.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author XXX
 * @since 2018-03-08
 * 求出互粉的好友
 */
public class FriendshipCheck {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("maxmin").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = jsc.textFile("res/friend.txt");

        textFile.map(line -> {
            String[] split = line.split(":");
            String[] fans = split[1].split(",");
            String[] relationships = new String[fans.length];
            for (int i = 0; i < fans.length; i++) {
                relationships[i] = split[0].compareTo(fans[i]) > 0 ? fans[i] + split[0] : split[0] + fans[i];
            }
            return relationships;
        })
                .flatMap(relationships -> Arrays.asList(relationships).iterator())  //将关系映射为类似于AB，BC，EG，要按照字母排序
                .mapToPair(relation -> new Tuple2<>(relation, 1))   //映射为（AB，1）（BC，1）（EG,1）元组
                .reduceByKey((a, b) -> a + b)
                .filter(t -> t._2() > 1)  //求出大于2的，即为互粉的好友
                .sortByKey()
                .foreach(t -> System.out.println(t._1()));

    }
}
