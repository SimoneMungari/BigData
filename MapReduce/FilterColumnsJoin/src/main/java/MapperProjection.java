import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class MapperProjection extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, Text>
{
    public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(";");
        StringBuilder builder = new StringBuilder("");
        int length = data.length;
        String fixed = data[1]+";"+data[length - 1]+";"+data[length - 2]+";"+data[length - 3]+";"+data[length - 4]+";"+data[length - 5]+";";
        int cont = 0;
        for(int i = 2; i < data.length - 5; i++)
        {
            if(i % 1000 == 0 || i == data.length - 6) {
                context.write(new IntWritable(cont), new Text(fixed + builder.toString()));
                cont++;
                builder = new StringBuilder("");
            }

            if(i < data.length - 6)
                builder.append(data[i]+";");
            else
                builder.append(data[i]);
        }
    }
}

