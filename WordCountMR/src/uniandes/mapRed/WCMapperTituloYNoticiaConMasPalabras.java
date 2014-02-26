package uniandes.mapRed;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapperTituloYNoticiaConMasPalabras extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> palabrasLinea = new HashMap<String, Integer>();
        String linea = value.toString();
        // Solo corre si la línea tiene un tag 'title'
        if (linea.toLowerCase().contains("<title>")) {
            linea = linea.replace("<title>", "");
            linea = linea.replace("</title>", "");
            String[] palabras = linea.split("([().,!?:;'\"-]|\\s)+");
            palabrasLinea.put(linea, palabras.length);
        }

        for (String k : palabrasLinea.keySet()) {
            context.write(new Text(k), new IntWritable(palabrasLinea.get(k)));
        }

    }
}
