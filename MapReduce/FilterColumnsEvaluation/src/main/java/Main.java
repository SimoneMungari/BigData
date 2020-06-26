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

        Job job = Job.getInstance(conf, "Column projection of sales_train_evaluation");

        job.setJarByClass(Main.class);

        job.setMapperClass(MapperProjection.class);

        job.setReducerClass(ReducerProjection.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path("/tmp/csvdata/sales_train_evaluation.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/csvdata/outputSalesTrainEvaluationFiltered"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}