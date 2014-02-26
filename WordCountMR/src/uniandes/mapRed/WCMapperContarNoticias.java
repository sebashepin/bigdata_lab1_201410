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
        String[] palabras = value.toString().split("([().,!?:;'\"-]|\\s)+");
        for (String palabra : palabras) {
            String lw = palabra.toLowerCase().trim();
            if (lw.equals("")) {
                continue;
            } // No queremos contar espacios
              // Si la palabra existe en el hashmap incrementa en 1 su valor,
              // en caso contrario la agrega y le asigna 1.

            // Para el conteo de noticias, si no es una noticia, no tenga en
            // cuenta la línea
            if (!lw.startsWith("<title>")) {
                continue;
            }
            palabrasLinea.put("NOTICIAS", palabrasLinea.containsKey("NOTICIAS") ? (palabrasLinea.get(lw) + 1) : 1);
        }
        for (String k : palabrasLinea.keySet()) {
            context.write(new Text(k), new IntWritable(palabrasLinea.get(k)));
        }

    }
}
