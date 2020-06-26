import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class MapperProjection extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>
{
    private Text dumbKey = new Text("1");


    public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        if (data[0].equals("id")) //saltiamo la prima riga, la mettiamo a mano nel reducer
            return;
        StringBuilder builder = new StringBuilder("");
        for(int i = 0; i < data.length; i++)
        {
            if(i == data.length - 1)
                builder.append(data[i]);
            else if(i == 0 || i >= 1805 ||   //prima colonna e ultimi 3 mesi e mezzo
                    (i >= 1553 && i <= 1580) || //Aprile-Maggio 2015
                    (i >= 1188 && i <= 1215) || //Aprile-Maggio 2014
                    (i >= 823 && i <= 850) || //Aprile-Maggio 2013
                    (i >= 458 && i <= 485) || //Aprile-Maggio 2012
                    (i >= 92 && i <= 119)) //Aprile-Maggio 2011
                builder.append(data[i]+",");
        }
        context.write(dumbKey, new Text(builder.toString()));
    }
}

