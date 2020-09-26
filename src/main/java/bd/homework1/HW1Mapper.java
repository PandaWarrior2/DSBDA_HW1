package bd.homework1;

import com.sun.jersey.core.header.reader.HttpHeaderReader;
import com.sun.org.apache.xpath.internal.operations.Bool;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

/**
 * Маппер: Парсит строку лога, вытаскивает IP-адрес и количество байт.
 * Присваивает bytes это количество байт, остальным значениям по 1 (requests, ибо в рамках одного запроса, FloatWriteable будет рассчитано в редьюсере)
 */
public class HW1Mapper extends Mapper<Object, Text, Text, LogWritable> {

    private Text word = new Text();
    private IntWritable bytes = new IntWritable(0);
    private IntWritable requests = new IntWritable(1);
    private FloatWritable avg = new FloatWritable(0.0f);
    private LogWritable Log = new LogWritable();
    private static boolean isInt(String strNum) {
        try {
            int d = Integer.parseInt(strNum);
        } catch (NumberFormatException | NullPointerException nfe) {
            return false;
        }
        return true;
    }

    public static boolean verifyIP(String IP){
        boolean flag = true;
        String[] arr = IP.split("\\.");
        flag = (arr.length == 4);
        for(String i: arr){
            flag = flag && isInt(i);
        }
        return flag;
    }
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split(" ");
        try {
            String IP = arr[0];
            String bytes_count = arr[8];
            if(verifyIP(IP) && isInt(bytes_count)) {
                bytes.set(Integer.parseInt(bytes_count));
                requests.set(1);

                word.set(arr[0]);
                Log.set(bytes, requests, avg);
                context.write(word, Log);
            }
        }
        catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
    }
}