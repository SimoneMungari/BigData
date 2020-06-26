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
        //tutte le colonne da tenere ma fare join fra le prime due

        if(!data[0].equals("store_id")) //se è la prima riga la saltiamo, verrà creata direttamente dal reducer
        {
            StringBuilder builder = new StringBuilder("");
            String temp = "";
            for(int i = 0; i < data.length; i++)
            {
                if(i == 0)
                    temp = data[i];
                else if( i == 1)
                    builder.append(data[i]+"_"+temp+"_validation,");
                else if(i == data.length - 1)
                    builder.append(data[i]);
                else
                    builder.append(data[i]+",");

            }


            context.write(dumbKey, new Text(builder.toString()));
        }
    }
}

