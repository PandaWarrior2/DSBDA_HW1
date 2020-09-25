import bd.homework1.LogWritable;
import eu.bitwalker.useragentutils.UserAgent;
import bd.homework1.HW1Mapper;
import bd.homework1.HW1Reducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapReduceTest {
    private MapDriver<Object, Text, Text, LogWritable> mapDriver;
    private ReduceDriver<Text, LogWritable, Text, LogWritable> reduceDriver;
    private MapReduceDriver<Object, Text, Text, LogWritable, Text, LogWritable> mapReduceDriver;
    private String testIP = "3.167.108.238 - - [12/Jul/2020:14:27:11 +0100] \"GET https://mysite.com\" 202 2701 \"-\"Mozilla/5.0 (Linux; U; Android 6.0; zh-CN; MZ-U10 Build/MRA58K) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/57.0.2987.108 MZBrowser/8.2.210-2020061519 UWS/2.15.0.4 Mobile Safari/537.36";
    @Before
    public void setUp(){
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = mapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(testIP))
                 .withOutput(new Text("3.167.108.238"), new LogWritable(new FloatWritable(1.0f), new IntWritable(2701), new IntWritable(1)))
                 .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<LogWritable> values = new ArrayList<LogWritable>();
        values.add(new LogWritable(new FloatWritable(1.0f), new IntWritable(2701), new IntWritable(1)));
        reduceDriver
                .withInput(new Text("3.167.108.238"), values)
                .withOutput(new Text("3.167.108.238"), new LogWritable(new FloatWritable(2701.0f), new IntWritable(2701), new IntWritable(1)))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testIP))
                .withInput(new LongWritable(), new Text(testIP))
                .withInput(new LongWritable(), new Text(testIP))
                .withOutput(new Text("3.167.108.238"), new LogWritable(new FloatWritable(2701.0f), new IntWritable(2701*3), new IntWritable(3)))
                .runTest();
    }
}
