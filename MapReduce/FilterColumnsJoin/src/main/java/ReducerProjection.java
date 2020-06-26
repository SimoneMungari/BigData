import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerProjection extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, Text, NullWritable>
{
    public void reduce(IntWritable key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        for(Text i : values)
        {
            context.write(i, NullWritable.get());
        }
    }
}

