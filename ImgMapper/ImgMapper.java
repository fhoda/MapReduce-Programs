package k3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

public class ImgMapper extends Mapper<Object, Text, BytesWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String uri = value.toString();

        System.out.println("Value of URI is now: " + uri);
        Configuration conf = context.getConfiguration();

        System.out.println("pass map conf");

        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        System.out.println("fs get uri");


        FSDataInputStream in = null;

        try {
            in = fs.open(new Path(uri));
            ByteArrayOutputStream bout = new ByteArrayOutputStream();

            byte buffer[] = new byte[1024 * 1024];


            while (in.read(buffer, 0, buffer.length) >= 0) {
                bout.write(buffer);
            }

            BytesWritable bytes = new BytesWritable(bout.toByteArray());
            context.write(bytes, value);
            System.out.println("Mapped value is: " + value);


        } catch (Throwable e){
            System.out.println("Error in try block: " + e);
        }
        finally {
            IOUtils.closeStream(in);
        }

    }

}
