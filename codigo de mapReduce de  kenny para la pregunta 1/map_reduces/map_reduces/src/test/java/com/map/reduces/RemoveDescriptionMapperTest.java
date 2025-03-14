package com.map.reduces;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

public class RemoveDescriptionMapperTest {

    @Test
    public void testMap() throws IOException {
        Text value = new Text(
                "\"3C66w5Z0ixs\",\"esto es una prueba ,,, ..., ,, ,.\",\"2020-08-11T19:20:14Z\",\"UCvtRTOMP2TqYqu51xNrqAzg\",\"Brawadis\",\"22\",\"2020-08-12T00:00:00Z\",\"brawadis|prank|basketball|skits|ghost|funny videos|vlog|vlogging|NBA|browadis|challenges|bmw i8|faze rug|faze rug brother|mama rug and papa rug\",\"1514614\",\"156908\",\"5855\",\"35313\",\"https://i.ytimg.com/vi/3C66w5Z0ixs/default.jpg\",\"False\",\"False\",\"SUBSCRIBE to BRAWADIS ▶ http://bit.ly/SubscribeToBrawadis\r\n" + //
                                        "\r\n" + //
                                        "FOLLOW ME ON SOCIAL\r\n" + //
                                        "▶ Twitter: https://twitter.com/Brawadis\r\n" + //
                                        "▶ Instagram: https://www.instagram.com/brawadis/\r\n" + //
                                        "▶ Snapchat: brawadis\r\n" + //
                                        "\r\n" + //
                                        "Hi! I’m Brandon Awadis and I like to make dope vlogs, pranks, reactions, challenges and basketball videos. Don’t forget to subscribe and come be a part of the BrawadSquad!\"\r\n" + //
                                        "");

        new MapDriver<LongWritable, Text, LongWritable, Text>()
                .withMapper(new RemoveDescriptionMapper())
                .withInput(new LongWritable(1), value)
                .runTest();
    }
}