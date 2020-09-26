package bd.homework1;

import lombok.extern.log4j.Log4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import bd.homework1.*;

import java.io.File;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            throw new RuntimeException("You should specify input and output folders!");
        }
        //FileUtils.deleteDirectory(new File("/root/Desktop/homework1/output"));
        Configuration conf = new Configuration();
        // задаём выходной файл, разделенный запятыми - формат CSV в соответствии с заданием
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        int reducers_count = 1;
        Job job = Job.getInstance(conf, "Bytes stats");
        job.setJarByClass(LogWritable.class); // Основной класс-контейнер для данных
        job.setMapperClass(HW1Mapper.class); // Маппер
        job.setCombinerClass(HW1Combiner.class); // Комбайнер
        job.setReducerClass(HW1Reducer.class); // Редуктор
        job.setNumReduceTasks(reducers_count);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LogWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // Задаем входную и выходную директории
        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        long start = System.currentTimeMillis();
        job.waitForCompletion(true);
        long time = System.currentTimeMillis() - start;
        log.info("=====================JOB ENDED=====================");
        log.info("Time: " +String.valueOf(time)); // Считаем время выполнения
    }
}
