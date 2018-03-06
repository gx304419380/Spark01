package com.fly.spark;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @author XXX
 * @since 2018-03-06
 */
public class TestUtils {
    @Test
    public void testGenerateText() throws IOException {
        FileWriter writer = new FileWriter("C:\\Users\\guoxiang.HDSC\\Desktop\\info.txt");
        Random random = new Random();
        String[] names = {"AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG"};
        for (int i = 0; i < 100; i++) {
            String name = names[random.nextInt(names.length)];
            int num = random.nextInt(1000);
            writer.write(name + " " + num + "\r\n");
        }
        writer.close();
    }
}
