package bd.homework1;
import java.io.*;
import org.apache.hadoop.io.*;
/*
    Основной класс-контейнер для данных
    Объединяет в себе 3 показателя:
    - Количество байт, присланных с IP адреса
    - Количество запросов
    - Ср.количество байт с IP адреса
*/
public class LogWritable implements Writable {
    private IntWritable bytes;
    private IntWritable requests;
    private FloatWritable average_bytes;
    public LogWritable(){
        this.average_bytes = new FloatWritable(0.0f);
        this.bytes = new IntWritable(0);
        this.requests = new IntWritable(0);
    }
    public LogWritable(FloatWritable avg, IntWritable bytes, IntWritable reqs){
        this.average_bytes = avg;
        this.bytes = bytes;
        this.requests = reqs;
    }
    public FloatWritable getAverageBytes(){
        return this.average_bytes;
    }
    public IntWritable getSumBytes(){
        return this.bytes;
    }
    public IntWritable getRequestsCount(){
        return this.requests;
    }
    // Запись и чтение полей, соответственно.
    @Override
    public void write(DataOutput dataOut) throws IOException {
        bytes.write(dataOut);
        requests.write(dataOut);
        average_bytes.write(dataOut);
    }
    @Override
    public void readFields(DataInput dataIn) throws IOException {
        bytes.readFields(dataIn);
        requests.readFields(dataIn);
        average_bytes.readFields(dataIn);
    }

    @Override
    public int hashCode(){
        return bytes.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LogWritable) {
            LogWritable other = (LogWritable) o;
            return  bytes.equals(other.bytes) &&
                    requests.equals(other.requests) &&
                    average_bytes.equals(other.average_bytes);
        }
        return false;
    }
    // Метод для записи в файл (формат)
    @Override
    public String toString(){
        return bytes.toString() + ", " + requests.toString() + ", " + average_bytes.toString();
    }
    // Метод для подгрузки готовых значений в редьюсере
    public void set(IntWritable bytes, IntWritable requests, FloatWritable avg) {
        this.average_bytes = avg;
        this.bytes = bytes;
        this.requests = requests;
    }
}
