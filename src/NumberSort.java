import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ydubale on 3/29/15.
 */
public class NumberSort {



    public static class NumberSortMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

        private static final IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Long num = Long.parseLong(value.toString());

            context.write(new LongWritable(num), one);
        }
    }

    public static class NumberSortPartitioner extends Partitioner<LongWritable, IntWritable> {

        private final long numberOfValues = 1073741825; //2^30 + 1

        @Override
        public int getPartition(LongWritable key, IntWritable value, int numReducers) {
            long numPartitions = numberOfValues/numReducers;
            int divisor = (int) (key.get()/numPartitions);
            return divisor % numReducers;
        }
    }

    public static class NumberSortReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

        public void reduce(LongWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable count : value){
                sum += count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args){

        //Set the number of reducers to 16


    }

}
