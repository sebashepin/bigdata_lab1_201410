package uniandes.mapRed;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String noticiaMayor = "";
        int masPalabras = 0;
    	
    	for (IntWritable iw : values) {
    		if (iw.get() > masPalabras)
    		{
    			noticiaMayor = key.toString();
    			masPalabras = iw.get();
    		}
        	context.write(key, new IntWritable(iw.get()));
        }
        
    	context.write(new Text(noticiaMayor), new IntWritable(masPalabras));
    }

}
