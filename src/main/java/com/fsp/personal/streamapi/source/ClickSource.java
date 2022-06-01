package com.fsp.personal.streamapi.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @Description 自定义数据源
 * @Author ZhongYangyixiong
 * @Date 2022/6/1 9:00 PM
 */
public class ClickSource implements SourceFunction<Event> {
    private Boolean running = true;

    // 申明一个标识位，来控制数据的生成
    @Override
    public void run(SourceContext ctx) throws Exception {
        // 随机生成数据
        Random random = new Random();

        // 定于字段选取的数据
        String[] users = {"Mary", "Henry", "Tom", "Jessoca"};
        String[] urls = {"./home", "./cart", "./fav", "./port?id=100"};
        // 循环生成数据
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user, url, timestamp));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
