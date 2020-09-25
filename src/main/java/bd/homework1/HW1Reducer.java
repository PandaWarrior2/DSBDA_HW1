package bd.homework1;

import jdk.nashorn.internal.runtime.Context;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sun.rmi.runtime.Log;

import java.io.IOException;

/**
 * Редьюсер:
 * а) суммирует все байты полученные от HW1Mapper, выдаёт суммарное количество байт
 * б) суммирует все количества запросов, полученные от HW1Mapper, выдает суммарное кол-во запросов, полученное с определенного IP
 * в) считает среднее число байт, пришедшее от определенного IP
 */

public class HW1Reducer extends Reducer<Text, LogWritable, Text, LogWritable> {
    private IntWritable rBytes = new IntWritable();
    private IntWritable rRequests = new IntWritable();
    private FloatWritable avgBytes = new FloatWritable();
    private LogWritable rLog = new LogWritable();

    @Override
    public void reduce(Text key, Iterable<LogWritable> values, Context context) throws IOException, InterruptedException {
        int sum_bytes = 0;
        int requests_cnt = 0;

        for(LogWritable i : values){
            sum_bytes += i.getSumBytes().get();
            requests_cnt += i.getRequestsCount().get();
        }
        float avg_bytes = 0;
        if(requests_cnt != 0) {
            avg_bytes = (float) sum_bytes / requests_cnt;
        }
        else{
            avg_bytes = 0.0f;
        }
        rBytes.set(sum_bytes);
        rRequests.set(requests_cnt);
        avgBytes.set(avg_bytes);

        rLog.set(rBytes, rRequests, avgBytes);
        context.write(key, rLog);
    }
}
