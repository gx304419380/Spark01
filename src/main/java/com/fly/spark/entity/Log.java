package com.fly.spark.entity;

import java.io.Serializable;
import java.util.Date;

/**
 * @author XXX
 * @since 2018-03-07
 */
public class Log implements Serializable {
    private Date date;
    private Long number;
    private String threadName;
    private String logLevel;
    private String content;

    public Log() {
    }

    public Log(Date date, Long number, String threadName, String logLevel, String content) {

        this.date = date;
        this.number = number;
        this.threadName = threadName;
        this.logLevel = logLevel;
        this.content = content;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Long getNumber() {
        return number;
    }

    public void setNumber(Long number) {
        this.number = number;
    }

    public String getThreadName() {
        return threadName;
    }

    public void setThreadName(String threadName) {
        this.threadName = threadName;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "Log{" +
                "date=" + date +
                ", number=" + number +
                ", threadName='" + threadName + '\'' +
                ", logLevel='" + logLevel + '\'' +
                ", content='" + content + '\'' +
                '}';
    }
}
