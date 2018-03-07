package com.fly.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author XXX
 * @since 2018-03-06
 */
public class MaxMin {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("maxmin").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = jsc.textFile("res/maxmin.txt");
        Integer max = textFile
                .map(Integer::parseInt)
                .reduce(Math::max);
        Integer min = textFile.map(Integer::parseInt)
                .reduce(Math::min);

        System.out.println(min);
        System.out.println(max);
    }
}
