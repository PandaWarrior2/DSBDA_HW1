package bd.homework1;

import com.sun.jersey.core.header.reader.HttpHeaderReader;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

/**
 * Маппер: парсит UserAgent из логов, к каждому браузеру присваиваем в соответствие единицу
 */
public class HW1Mapper extends Mapper<Object, Text, Text, LogWritable> {

    private Text word = new Text();
    private IntWritable bytes = new IntWritable(0);
    private IntWritable requests = new IntWritable(1);
    private FloatWritable avg = new FloatWritable(1.0f);
    private LogWritable Log = new LogWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split(" ");
        String IP = arr[0];
        if(!arr[9].equals("-")) {
            bytes.set(Integer.parseInt(arr[8]));
            requests.set(1);
        }
        else{
            bytes.set(0);
        }

        word.set(arr[0]);
        Log.set(bytes, requests, avg);
        context.write(word,Log);
    }
}