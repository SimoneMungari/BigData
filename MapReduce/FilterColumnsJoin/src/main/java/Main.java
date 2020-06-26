import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Main
{
    public static void main(String argv[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Column projection of joinFinale");

        job.setJarByClass(Main.class);

        job.setMapperClass(MapperProjection.class);

        job.setReducerClass(ReducerProjection.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);

        job.setNumReduceTasks(31);

        FileInputFormat.addInputPath(job, new Path("../joinFinale.csv")); //d
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}