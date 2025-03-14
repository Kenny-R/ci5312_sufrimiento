package com.map.reduces;

import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RemoveDescriptionMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        try (StringReader reader = new StringReader(line)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
            for (CSVRecord record : records) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < record.size() - 1; i++) {
                    sb.append(record.get(i));
                    if (i < record.size() - 2) {
                        sb.append("͹");
                    }
                }
                System.out.println("Parsed record (without last field): " + sb.toString());
                // context.write(key, new Text(sb.toString()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}