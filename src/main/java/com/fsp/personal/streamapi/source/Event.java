package com.fsp.personal.streamapi.source;

import java.sql.Timestamp;

/**
 * @Description 三元组（用户名，用户访问的URL， 用户访问URL的时间戳）
 * @Author ZhongYangyixiong
 * @Date 2022/5/31 9:20 PM
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
