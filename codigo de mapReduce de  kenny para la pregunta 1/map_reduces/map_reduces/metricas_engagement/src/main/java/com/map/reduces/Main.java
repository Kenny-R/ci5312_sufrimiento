package com.map.reduces;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// Voy a poner aquí  toda la ejecución de los trabajos de map reduce
public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println(
                    "Usage: Main <input path> <sequence file path> <metrics output path>");
            System.exit(-1);
        }

        CreateSequenceFileJobBuilder csfjb = new CreateSequenceFileJobBuilder(args[0], args[1]);

        Configuration conf_dfs = new Configuration();
        FileSystem fs = FileSystem.get(conf_dfs);
        Path sequenceFilePath = new Path(args[1]);

        // Si los datos no tienen un sequence file lo creamos
        if (!fs.exists(sequenceFilePath)) {
            Job job1 = csfjb.CreateJob();

            boolean job1Success = job1.waitForCompletion(true);
            if (!job1Success) {
                System.exit(1);
            }
        }
        // Si ya se calculo las proporciones no lo volvemos a hacer
        if (!fs.exists(new Path(args[2]))) {
            CalculateTrendingMetricsJobBuilder ctmjb = new CalculateTrendingMetricsJobBuilder(args[1],
                    args[2]);

            Job job2 = ctmjb.CreateJob();

            boolean job2Success = job2.waitForCompletion(true);
            if (!job2Success) {
                System.exit(1);
            }
        }

        // if (!fs.exists(new Path(args[3]))) {
        // SortMetricsJobBuilder smjb = new SortMetricsJobBuilder(args[2], args[3]);

        // Job job3 = smjb.CreateJob();

        // boolean job3Success = job3.waitForCompletion(true);
        // if (!job3Success) {
        // System.exit(1);
        // }
        // }
    }

}

/*******************************************************************************************
 * Zona para la declaración del trabajo para generar un sequence file
 *******************************************************************************************
 */

class GenerateSequenceFileMapper extends Mapper<LongWritable, Text, TrendingDateAndIdVideoKey, Text> {

    public static enum Counters {
        FILAS_PROCESADAS_MAPPER
    }

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
            String category = cells[5];
            String views = cells[8];
            String likes = cells[9];
            String dislikes = cells[10];
            String comments = cells[11];

            TrendingDateAndIdVideoKey trendingDateAndIdVideoKey = new TrendingDateAndIdVideoKey(trendingDate, videoId);

            String final_value = views + "¶" + likes + "¶" + dislikes + "¶" + comments + "¶" + category;

            context.getCounter(Counters.FILAS_PROCESADAS_MAPPER).increment(1);

            context.write(trendingDateAndIdVideoKey, new Text(final_value));
        }
    }
}

class CreateSequenceFileJobBuilder {
    private String inputPath;
    private String outputPath;

    public CreateSequenceFileJobBuilder(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public Job CreateJob() throws IOException {
        Job job = Job.getInstance();
        job.setJarByClass(CreateSequenceFileJobBuilder.class);
        job.setJobName("Generate Sequence File");
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(this.inputPath));
        FileOutputFormat.setOutputPath(job, new Path(this.outputPath));

        job.setMapperClass(GenerateSequenceFileMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(TrendingDateAndIdVideoKey.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}

/*******************************************************************************************
 * Zona para la declaración del trabajo para calcular las metricas de los
 * videos. La salida
 * de este trabajo sera un SequenceFile donde el valor es la categoria del video
 * y la clave sera una clave compuesta por:
 * - La fecha en la que se hizo trending
 * - El ID del video
 * - likes/views
 * - dislikes/views
 * - comments/views
 * - likes/dislikes
 *******************************************************************************************
 */

/*
 * Este mapper no hace nada solo pasa la clave y el valor
 */
class CalculateTrendingMetricsMapper
        extends Mapper<TrendingDateAndIdVideoKey, Text, TrendingDateAndIdVideoKey, Text> {

    public static enum Counters {
        FILAS_PROCESADAS_MAPPER
    }

    @Override
    public void map(TrendingDateAndIdVideoKey key, Text value, Context context)
            throws IOException, InterruptedException {

        context.getCounter(Counters.FILAS_PROCESADAS_MAPPER).increment(1);
        context.write(key, value);
    }
}

class CalculateTrendingMetricsReducer
        extends Reducer<TrendingDateAndIdVideoKey, Text, Text, Text> {

    public static enum Counters {
        FILAS_PROCESADAS_REDUCER,
        FILAS_INGNORADAS_POR_SER_NAN_O_INFINITO
    }

    @Override
    public void reduce(TrendingDateAndIdVideoKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Text lastValue = null;
        String[] currentValues;
        String[] lastValues;

        for (Text value : values) {
            if (lastValue == null) {
                lastValue = value;
            } else {
                lastValues = lastValue.toString().split("¶");
                currentValues = value.toString().split("¶");

                boolean updated = false;

                // Si la cantidad de vistas es mayor a la anterior actulaizamos el valor
                // Si ademas la cantidad de vistas es igual a la anterior, comparamos likes,
                // dislikes y comentarios y si alguno es mayor actualizamos
                if (Integer.parseInt(currentValues[0]) >= Integer.parseInt(lastValues[0])) {
                    for (int i = 1; i < 4; i++) {
                        if (Integer.parseInt(currentValues[i]) > Integer.parseInt(lastValues[i])) {
                            updated = true;
                            break;
                        }
                    }
                }

                updated = updated || Integer.parseInt(currentValues[0]) > Integer.parseInt(lastValues[0]);

                if (updated) {
                    lastValue = value;
                }
            }
        }
        context.getCounter(Counters.FILAS_PROCESADAS_REDUCER).increment(1);

        if (lastValue != null) {
            String[] finalValues = lastValue.toString().split("¶");
            double views = Double.parseDouble(finalValues[0]);
            double likes = Double.parseDouble(finalValues[1]);
            double dislikes = Double.parseDouble(finalValues[2]);
            double comments = Double.parseDouble(finalValues[3]);
            String category = finalValues[4];

            double likesToViewsRatio = likes / views;
            double dislikesToViewsRatio = dislikes / views;
            double commentsToViewsRatio = comments / views;
            double likesToDislikesRatio = likes / dislikes;

            // Si alguno de las propociones son NaN o infinito no lo pasamos por que despues
            // no podremos ordenar bien
            // likesToViewsRatio es infinito si views es 0
            // dislikesToViewsRatio si views es 0
            // likesToDislikesRatio es inifnito si dislikes es 0

            if (Double.isNaN(likesToViewsRatio) || Double.isInfinite(likesToViewsRatio) ||
                    Double.isNaN(dislikesToViewsRatio) || Double.isInfinite(dislikesToViewsRatio) ||
                    Double.isNaN(commentsToViewsRatio) || Double.isInfinite(commentsToViewsRatio) ||
                    Double.isNaN(likesToDislikesRatio) || Double.isInfinite(likesToDislikesRatio)) {
                context.getCounter(Counters.FILAS_INGNORADAS_POR_SER_NAN_O_INFINITO).increment(1);

                return;
            }

            TrendingMetricsKey trendingMetricsKey = new TrendingMetricsKey(key.getVideoId().toString(),
                    key.getTrendingDate().toString(),
                    likesToViewsRatio, dislikesToViewsRatio, commentsToViewsRatio, likesToDislikesRatio);
            String clave_final = trendingMetricsKey.toString();

            clave_final += "\t" + finalValues[0] + "\t" + finalValues[1] + "\t" + finalValues[2] + "\t" + finalValues[3];
            
            String newValue = category;

            lastValue.set(newValue);
            context.write(new Text(clave_final), lastValue);
        }
    }
}

class DateAndVideoIDPartitioner extends Partitioner<TrendingDateAndIdVideoKey, Text> {
    @Override
    public int getPartition(TrendingDateAndIdVideoKey key, Text value, int numPartitions) {
        int hash = key.getTrendingDate().hashCode() * 163 + key.getVideoId().hashCode();
        return (hash & Integer.MAX_VALUE) % numPartitions;
    }
}

class DateAndVideoIdGroupingComparator extends WritableComparator {
    protected DateAndVideoIdGroupingComparator() {
        super(TrendingDateAndIdVideoKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TrendingDateAndIdVideoKey k1 = (TrendingDateAndIdVideoKey) w1;
        TrendingDateAndIdVideoKey k2 = (TrendingDateAndIdVideoKey) w2;
        int result = k1.getTrendingDate().compareTo(k2.getTrendingDate());
        if (result == 0) {
            result = k1.getVideoId().compareTo(k2.getVideoId());
        }
        return result;
    }
}

class SortComparator extends WritableComparator {
    protected SortComparator() {
        super(TrendingDateAndIdVideoKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TrendingDateAndIdVideoKey k1 = (TrendingDateAndIdVideoKey) w1;
        TrendingDateAndIdVideoKey k2 = (TrendingDateAndIdVideoKey) w2;
        return k1.compareTo(k2);
    }
}

class CalculateTrendingMetricsJobBuilder {
    private String inputPath;
    private String outputPath;

    public CalculateTrendingMetricsJobBuilder(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public Job CreateJob() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance();
        job.setJarByClass(CalculateTrendingMetricsJobBuilder.class);
        job.setJobName("Get Last Views, Likes, Dislikes and Comments");

        // Seteamos la entrada y salida del trabajo
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // seteamos las direcciones de entrada y salida del trabajo
        FileInputFormat.addInputPath(job, new Path(this.inputPath));
        FileOutputFormat.setOutputPath(job, new Path(this.outputPath));

        // Seteamos las clases del maper y el reducer
        job.setMapperClass(CalculateTrendingMetricsMapper.class);
        job.setReducerClass(CalculateTrendingMetricsReducer.class);

        job.setNumReduceTasks(5);

        // Seteamos la salida del maper
        job.setMapOutputKeyClass(TrendingDateAndIdVideoKey.class);
        job.setMapOutputValueClass(Text.class);

        // Seteamos la salida del reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Seteamos las particiones y todo lo necesario para ordenar la salida
        // por qué? Por que lo aprendi y lo voy a usar hasta la hartarme
        job.setPartitionerClass(DateAndVideoIDPartitioner.class);
        job.setGroupingComparatorClass(DateAndVideoIdGroupingComparator.class);
        job.setSortComparatorClass(SortComparator.class);

        InputSampler.Sampler<TrendingDateAndIdVideoKey, Text> sampler = new InputSampler.RandomSampler<TrendingDateAndIdVideoKey, Text>(
                0.1, 1000, 10);

        InputSampler.writePartitionFile(job, sampler);

        Configuration conf = job.getConfiguration();
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);

        System.out.println("Partition file: " + partitionFile);

        URI partitionUri = new URI(partitionFile);
        job.addCacheFile(partitionUri);
        return job;
    }
}

/*******************************************************************************************
 * Zona para la declaración del trabajo que ordenara el los videos por las
 * metricas obtenidas (no funciono T.T)
 *******************************************************************************************
 */

class SortMetricsMapper extends Mapper<TrendingMetricsKey, Text, TrendingMetricsKey, Text> {
    public static enum Counters {
        FILAS_PROCESADAS_MAPPER
    }

    @Override
    public void map(TrendingMetricsKey key, Text value, Context context)
            throws IOException, InterruptedException {
        context.getCounter(Counters.FILAS_PROCESADAS_MAPPER).increment(1);

        context.write(key, value);
    }
}

class SortMetricsReducer extends Reducer<TrendingMetricsKey, Text, TrendingMetricsKey, Text> {
    public static enum Counters {
        FILAS_PROCESADAS_REDUCER
    }

    @Override
    public void reduce(TrendingMetricsKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            context.getCounter(Counters.FILAS_PROCESADAS_REDUCER).increment(1);

            context.write(key, value);
        }
    }
}

class DateAndVideoIDMetricsPartitioner extends Partitioner<TrendingMetricsKey, Text> {
    @Override
    public int getPartition(TrendingMetricsKey key, Text value, int numPartitions) {
        int hash = key.getTrendingDate().hashCode() * 163 + key.getVideoId().hashCode();
        return (hash & Integer.MAX_VALUE) % numPartitions;
    }
}

class DateAndVideoIdGroupingMetricsComparator extends WritableComparator {
    protected DateAndVideoIdGroupingMetricsComparator() {
        super(TrendingMetricsKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TrendingMetricsKey k1 = (TrendingMetricsKey) w1;
        TrendingMetricsKey k2 = (TrendingMetricsKey) w2;
        int result = k1.getTrendingDate().compareTo(k2.getTrendingDate());
        if (result == 0) {
            result = k1.getVideoId().compareTo(k2.getVideoId());
        }
        return result;
    }
}

class SortMetricsComparator extends WritableComparator {
    protected SortMetricsComparator() {
        super(TrendingMetricsKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        TrendingMetricsKey k1 = (TrendingMetricsKey) w1;
        TrendingMetricsKey k2 = (TrendingMetricsKey) w2;
        return k1.compareTo(k2);
    }
}

class SortMetricsJobBuilder {
    private String inputPath;
    private String outputPath;

    public SortMetricsJobBuilder(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public Job CreateJob() throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance();
        job.setJarByClass(SortMetricsJobBuilder.class);
        job.setJobName("Sort Metrics");

        job.setInputFormatClass(SequenceFileInputFormat.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(this.inputPath));
        FileOutputFormat.setOutputPath(job, new Path(this.outputPath));

        job.setMapperClass(SortMetricsMapper.class);
        job.setReducerClass(SortMetricsReducer.class);

        job.setNumReduceTasks(10);

        job.setOutputKeyClass(TrendingMetricsKey.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(DateAndVideoIDMetricsPartitioner.class);
        job.setGroupingComparatorClass(DateAndVideoIdGroupingMetricsComparator.class);
        job.setSortComparatorClass(SortMetricsComparator.class);

        InputSampler.Sampler<TrendingMetricsKey, Text> sampler = new InputSampler.RandomSampler<TrendingMetricsKey, Text>(
                0.1, 1000, 10);

        InputSampler.writePartitionFile(job, sampler);

        Configuration conf = job.getConfiguration();
        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);

        System.out.println("Partition file: " + partitionFile);

        URI partitionUri = new URI(partitionFile);
        job.addCacheFile(partitionUri);
        return job;
    }
}