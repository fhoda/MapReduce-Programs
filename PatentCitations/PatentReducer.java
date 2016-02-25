package k3.patentRef;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public  class PatentReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            super.reduce(key, values, context);

//        System.out.println("in reduce");
        ArrayList<String> patentsList = new ArrayList<String>();

        for (Text val : values){
            if(!patentsList.contains(val.toString())){
                patentsList.add(val.toString());
            }
        }

        result.set(patentsList.toString());
        context.write(key, result);
    }
}
