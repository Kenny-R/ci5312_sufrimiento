package com.map.reduces;

import java.io.IOException;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;

public class SortVideosByLikesAndViewsTest {
//     @Test
//     public void testMap() throws IOException {
//         Text value = new Text(
//                 "\"3C66w5Z0ixs\",\"Titulo\",\"2020-08-11T19:20:14Z\",\"UCvtRTOMP2TqYqu51xNrqAzg\",\"prueba1\",\"22\",\"2020-08-12T00:00:00Z\",\"tags|tags\",\"1514614\",\"156908\",\"5855\",\"35313\",\"https://i.ytimg.com/vi/3C66w5Z0ixs/default.jpg\",\"False\",\"False\",\"Una descripción\"");

//         TrendingVideoKey outputKey = new TrendingVideoKey(
//                 "2020-08-12T00:00:00Z",
//                 1514614L,
//                 156908L,
//                 "3C66w5Z0ixs");

//         new MapDriver<LongWritable, Text, TrendingVideoKey, LongWritable>()
//                 .withMapper(new SortVideosByLikesAndViewsMapper())
//                 .withInput(new LongWritable(1), value)
//                 .withOutput(outputKey, new LongWritable(22))
//                 .runTest();
//     }
}
