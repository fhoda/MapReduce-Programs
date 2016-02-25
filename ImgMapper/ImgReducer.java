package k3;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class ImgReducer extends Reducer<BytesWritable, Text, Text, IntWritable> {


    @Override
    protected void reduce(BytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);
        System.out.println("in reducer");

        ArrayList<String> fileNames = new ArrayList<String>();

        IntWritable result = new IntWritable();
        int sum = 0;

        for(Text value : values) {
            sum = sum + 1;
            String tmp = value.toString();
            fileNames.add(tmp);
        }

//        System.out.println(fileNames.toString());
        result.set(sum);
        for(String val : fileNames){
//            System.out.println("Value passed to reducer: " + val);
            context.write(new Text(val), result);
        }
    }
}
