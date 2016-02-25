package k3;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.fs.shell.Display;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ImgMR
{

    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        // Check if all program arguments are passed.
        if(args.length != 2){
            System.err.println("Missing arguments!");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Image Test");

        System.out.println("Setting job classes");
        job.setJarByClass(ImgMR.class);
        job.setMapperClass(ImgMapper.class);
        job.setReducerClass(ImgReducer.class);
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        System.out.println("Job Classes set");

        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.out.println("Input is set to: " + args[0]);


        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("Output path set");

        //Get file names from images folder
        FileUtil futil = new FileUtil();
        File images = new File("images");
        String[] files  = futil.list(images);
//        System.out.println("List of files: " + Arrays.toString(files));

        // Write file names to input/images.txt
        Path inputFile = new Path(args[0]+"/images.txt");
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(inputFile, true)));
        for(String file : files){
            System.out.println("New file found: " + file);
            bw.write("images/"+file);
            bw.newLine();
        }
        bw.flush();
        bw.close();

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
