package bd.homework1;

import jdk.nashorn.internal.runtime.Context;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.fasterxml.jackson.annotation.ObjectIdGenerators;
import sun.rmi.runtime.Log;

import java.io.IOException;

/**
 * Комбайнгер:
 * а) суммирует все байты полученные от HW1Mapper, выдаёт суммарное количество байт
 * б) суммирует все количества запросов, полученные от HW1Mapper, выдает суммарное кол-во запросов, полученное с определенного IP
 */

public class HW1Combiner extends Reducer<Text, LogWritable, Text, LogWritable> {
    private IntWritable bytes = new IntWritable();
    private IntWritable requests = new IntWritable();
    private LogWritable log = new LogWritable();

    @Override
    public void reduce(Text key, Iterable<LogWritable> values, Context context) throws IOException, InterruptedException {
        int sum_bytes = 0;
        int requests_cnt = 0;

        for(LogWritable i : values){
            sum_bytes += i.getSumBytes().get();
            requests_cnt += i.getRequestsCount().get();
        }

        bytes.set(sum_bytes);
        requests.set(requests_cnt);

        log.set(bytes, requests, new FloatWritable(0));
        context.write(key, log);
    }
}
