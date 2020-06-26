import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerProjection extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, NullWritable>
{
    public void reduce(Text key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String firstRow = "id,wm_yr_wk,sell_price";
        context.write(new Text(firstRow),NullWritable.get());
        for(Text i : values)
        {
            context.write(i, NullWritable.get());
        }
    }
}

