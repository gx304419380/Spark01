package com.fly.spark;

import com.fly.spark.entity.Log;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * @author XXX
 * @since 2018-03-07
 */
public class LogAnalysis {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("log-analysis").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        JavaRDD<String> logRDD = jsc.textFile("res/log.txt");
        JavaRDD<Log> logs = logRDD
                .map(LogAnalysis::parseLog)
                .filter(Objects::nonNull);
        Dataset<Row> logDataset = spark.createDataFrame(logs, Log.class);

        List<Log> warnLog = logs
                .filter(log -> log.getLogLevel().equals("WARN"))
                .collect();
        List<Log> errorLog = logs
                .filter(log -> log.getLogLevel().equals("ERROR"))
                .collect();

        warnLog.forEach(log -> System.out.println("WARN: " + log.getContent()));
        errorLog.stream()
                .filter(log -> log.getContent().toLowerCase().contains("csv header"))
                .forEach(log -> System.out.println(log.getContent()));
    }

    public static Log parseLog(String logString) {

        Log log = new Log();
        String[] logs = logString.split(" ");
        if (logs.length < 5)
            return null;

        Date date = null;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(logs[0] + " " + logs[1]);
        } catch (ParseException e) {
            System.out.println("日期转换异常" + logs[0] + " " + logs[1]);
            return null;
        }
        StringBuilder content = new StringBuilder();
        for (int i = 5; i < logs.length; i++) {
            content.append(logs[i]).append(" ");
        }
        long number = 0;
        try {
            number = Long.parseLong(logs[2]);
        } catch (Exception e) {
            System.out.println("数字异常" + logs[2]);
            return null;
        }

        log.setDate(date);
        log.setNumber(number);
        log.setThreadName(logs[3]);
        log.setLogLevel(logs[4]);
        log.setContent(content.toString());
        return log;
    }

}
