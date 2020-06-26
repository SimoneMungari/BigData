import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapperProjection extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
{
    private Text dumbKey = new Text("1");

    public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        //colonne da tenere 1,2,6,8,10

        if(!data[0].equals("date")) //se è la prima riga la saltiamo, verrà creata direttamente dal reducer
        {
            StringBuilder builder = new StringBuilder("");
            for(int i = 0; i < data.length; i++)
            {
                if(i == 1 || i == 2 || i == 6 || i == 8) builder.append(data[i]+",");
                else if(i == 10)
                    builder.append(data[i]);
            }


            context.write(dumbKey, new Text(builder.toString()));
        }
    }
}

