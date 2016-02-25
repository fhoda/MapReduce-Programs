package k3.legal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class App 
{
	public static class LegalMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		
		Text documentId; // Store filename 
        private static Text word = new Text(); // for key on inverted index
		ArrayList<String> bad_words_list = new ArrayList<String>(); // Storing list of bad words

        
		@Override
		public void setup (Context context) throws IOException{	
//			System.out.println("in setup function");

			// Get file name and store it for use as a value
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			
			System.out.println(fileName);
            documentId = new Text(fileName);
        }
		// Test change
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			
//			System.out.println("in Map function");
			
			// If the Array for the bad words listi is empty populate it otherwise skip this step			
			if(bad_words_list.isEmpty()){
				Configuration conf = context.getConfiguration();
	        	String bwlist= conf.get("badWordsList");
	        	
	        	StringTokenizer bwlIterator = new StringTokenizer(bwlist.toLowerCase());
	        	while(bwlIterator.hasMoreTokens()){
	        		String tmp_badword = bwlIterator.nextToken();
	        		bad_words_list.add(tmp_badword);
	//        		System.out.println(tmp_badword);
	        	}
			}
        	
			// Read files and map them.
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("[-+.^:,';’()?!]","")); 
	        String tmp;
	        
	        while (itr.hasMoreTokens()) {
	            tmp = itr.nextToken();
	            if(bad_words_list.contains(tmp)){
		        	word.set(tmp);
//		        	System.out.println("Mapper writing...");
		            context.write(word, documentId);
	            }
	        }
		}
	}
		
		
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{
        
        private Text result = new Text(); // for writing values on the inverted index

        protected void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
    
        	// Store values for a given key in an array
            ArrayList<String> allDocs = new ArrayList<String>(); 
            
            // Collect all values on a key
            for (Text val : values){
            	//Prevent duplicate values
            	if(!allDocs.contains(val.toString())){
                	allDocs.add(val.toString());
                }
            }
            
            result.set(new Text(allDocs.toString()));
            context.write(key, result);
    
        }
    }
	    
	
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
		
    	// Get list of bad words from a file and store as a string to be passed to the mapper
    	String bad_words = "";
        FileReader file = new FileReader("badwords.txt");
		BufferedReader reader = new BufferedReader(file);
		while(reader.ready()){
			String badWord = reader.readLine();
			bad_words = bad_words + " " + badWord;
//			System.out.println(bad_words);
		}
		file.close();
		reader.close();

        Configuration conf = new Configuration();
		conf.set("badWordsList", bad_words);
        Job job = Job.getInstance(conf, "Badwords inverted index");

        job.setJarByClass(App.class);
        job.setMapperClass(LegalMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        FileSystem fs = FileSystem.get(conf);
       
       if (fs.exists(new Path(args[1]))) {
           fs.delete(new Path(args[1]), true);
       }
       
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
       System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
        

