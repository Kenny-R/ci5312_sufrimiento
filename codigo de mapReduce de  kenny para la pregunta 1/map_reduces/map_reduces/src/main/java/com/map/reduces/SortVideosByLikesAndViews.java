package com.map.reduces;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

// la clase principal, aqui se hace toda la "magia"
public class SortVideosByLikesAndViews {
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: SortVideosByLikesAndViews <input path> <sequence file path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf_dfs = new Configuration();
        FileSystem fs = FileSystem.get(conf_dfs);
        Path sequenceFilePath = new Path(args[1]);

        // primero generamos un archivo secuencial
        if (!fs.exists(sequenceFilePath)) {
            Job job1 = Job.getInstance();
            job1.setJarByClass(SortVideosByLikesAndViews.class);
            job1.setJobName("Generate Sequence File");
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));

            
            job1.setMapperClass(GenerateSequenceFileMapper.class);
            job1.setNumReduceTasks(0);
            
            job1.setOutputKeyClass(TrendingVideoKey.class);
            job1.setOutputValueClass(Text.class);

            boolean job1Success = job1.waitForCompletion(true);
            if (!job1Success) {
                System.exit(1);
            }
        }

        // Tomamos el resultado de ese archivo secuencial y lo ordenamos

        Job job2 = Job.getInstance();
        job2.setJarByClass(SortVideosByLikesAndViews.class);
        job2.setJobName("Ordenar videos por likes y vistas");

        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.setMapperClass(SortVideosByLikesAndViewsMapper.class);
        job2.setReducerClass(SortVideosByLikesAndViewsReducer.class);

        job2.setNumReduceTasks(5);

        job2.setOutputKeyClass(TrendingVideoKey.class);
        job2.setOutputValueClass(Text.class);

        // configuramos las particiones, el agrupamiento y la comparación
        job2.setPartitionerClass(DatePartitioner.class);
        job2.setGroupingComparatorClass(DateGroupingComparator.class);
        job2.setSortComparatorClass(SortComparator.class);

        InputSampler.Sampler<TrendingVideoKey, Text> sampler = new InputSampler.RandomSampler<TrendingVideoKey, Text>(
                0.1, 1000,
                10);

        InputSampler.writePartitionFile(job2, sampler);

        Configuration conf = job2.getConfiguration();
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);

        System.out.println("Partition file: " + partitionFile);

        URI partitionUri = new URI(partitionFile);
        job2.addCacheFile(partitionUri);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

// Este mapper parsea los datos de los videos y los mapea a una clave compuesta
// por la fecha en la que se hicieron trending, las vistas, los likes y el id del
// video, el valor es la categoria del video (lo podemos cambiar)
class GenerateSequenceFileMapper extends Mapper<LongWritable, Text, TrendingVideoKey, Text> {

    public static String[] parse(String line) {
        return parse(line,
                "^\"(.{11})\",\"(.*)\",\"(.{20})\",\"(.{24})\",\"(.*?)\",\"(\\d{1,2})\",\"(.{20})\",\"(.*?)\",\"(\\d*)\",\"(\\d*)\",\"(\\d*)\",\"(\\d*)\",\"(https://i.ytimg.com/vi/.{11}/default(?:_live)?.jpg)\",\"(False|True)\",\"(False|True)\",\".*$");
    }

    public static String[] parse(String line, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);

        if (matcher.lookingAt()) {
            int groupCount = matcher.groupCount();
            String[] capturedGroups = new String[groupCount];

            for (int i = 1; i <= groupCount; i++) {
                capturedGroups[i - 1] = matcher.group(i);
            }
            return capturedGroups;
        } else {
            return new String[0];
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] cells = parse(line);

        if (cells.length > 0) {
            String trendingDate = cells[6];
            String videoId = cells[0];
            long views = Long.parseLong(cells[8]);
            long likes = Long.parseLong(cells[9]);
            String category = cells[5];

            TrendingVideoKey trendingVideoKey = new TrendingVideoKey(trendingDate, views, likes, videoId);

            // el valor sera el mismo, pero sin la descripcion
            String otherData = String.join("¶", Arrays.copyOfRange(cells, 0, cells.length - 1));

            context.write(trendingVideoKey, new Text(category));
        }
    }
}

// Este mapper no hace nada, solo pasa la clave y el valor al reducer
class SortVideosByLikesAndViewsMapper
        extends Mapper<TrendingVideoKey, Text, TrendingVideoKey, Text> {

    @Override
    public void map(TrendingVideoKey key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}

// para que todos los videos que se hicieron trending en la misma fecha vayan al
// mismo reducer
class DatePartitioner extends Partitioner<TrendingVideoKey, Text> {
    @Override
    public int getPartition(TrendingVideoKey key, Text value, int numPartitions) {
        // Creo que parte de la culpa esta aquí
        return (key.getTrendingDate().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}

// para que todos los viedo que se hicieron trending en la misma fecha vayan al
// mismo grupo
class DateGroupingComparator extends WritableComparator {
    protected DateGroupingComparator() {
        super(TrendingVideoKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        // System.out.println("Estoy comprando en el DateGroupingComparator!!");
        TrendingVideoKey k1 = (TrendingVideoKey) w1;
        TrendingVideoKey k2 = (TrendingVideoKey) w2;
        return k1.getTrendingDate().compareTo(k2.getTrendingDate());
    }
}

// para comparar las claves
class SortComparator extends WritableComparator {
    protected SortComparator() {
        super(TrendingVideoKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TrendingVideoKey k1 = (TrendingVideoKey) w1;
        TrendingVideoKey k2 = (TrendingVideoKey) w2;
        return k1.compareTo(k2);
    }
}

// Este reducer no hace nada, solo pasa la clave y el valor al output
class SortVideosByLikesAndViewsReducer extends Reducer<TrendingVideoKey, Text, TrendingVideoKey, Text> {

    @Override
    public void reduce(TrendingVideoKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(key, value);
        }
    }
}