package k3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ChainMR
{
    public class SplitMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            IntWritable one = new IntWritable(1);
            while (tokenizer.hasMoreTokens()) {
                String content =  tokenizer.nextToken();
                context.write(new Text(content), one);
            }
        }
    }


    public class LowerCaseMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void map(Text key, IntWritable value, Context context)
                throws IOException, InterruptedException {

            String lowerKey = key.toString().toLowerCase();
            Text newKey = new Text(lowerKey);
            context.write(newKey, value);
        }
    }

    public class ChainMapReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {sum += value.get();}
            context.write(key, new IntWritable(sum));
        }

    }


    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();

        Configuration splitMapConf = new Configuration(false);
        ChainMapper.addMapper(job, SplitMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class, splitMapConf);


        Configuration lowerCaseConf = new Configuration(false);
        ChainMapper.addMapper(job, LowerCaseMapper.class, Text.class, IntWritable.class,Text.class, IntWritable.class, lowerCaseConf);

        job.setJarByClass(ChainMR.class);
        job.setReducerClass(ChainMapReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
