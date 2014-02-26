package uniandes.mapRed;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapperContarNoticias extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> palabrasLinea = new HashMap<String, Integer>();
        String linea = value.toString();

        // TODO Solo corre si la línea tiene un tag '<title'>
        // Si encuentra una lína con ése tag, le suma uno al contador de
        // noticias
        if (linea.toLowerCase().contains("<title>")) {
            palabrasLinea.put("NOTICIAS", palabrasLinea.containsKey("NOTICIAS") ? (palabrasLinea.get("NOTICIAS") + 1) : 1);
        }

        // Se dejó la construcción del iterador ya que ayuda a contemplar el
        // caso de un hashmap vacío
        for (String k : palabrasLinea.keySet()) {
            context.write(new Text(k), new IntWritable(palabrasLinea.get(k)));
        }

    }
}
