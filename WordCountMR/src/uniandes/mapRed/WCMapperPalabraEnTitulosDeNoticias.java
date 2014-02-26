package uniandes.mapRed;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapperPalabraEnTitulosDeNoticias extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> palabrasLinea = new HashMap<String, Integer>();
        String linea = value.toString();

        // TODO Solo corre si la línea tiene un tag '<title>'
        // Ya que se quiere contar palabras en título, el resto del código se
        // deja intacto
        if (linea.toLowerCase().contains("<title>")) {

            // TODO Se "limpia" la línea de tags que pudieran ser contados como
            // palabras
            linea = linea.replace("<title>", "");
            linea = linea.replace("</title>", "");

            String[] palabras = linea.split("([().,!?:'\"-]|\\s)+");
            for (String palabra : palabras) {
                String lw = palabra.toLowerCase().trim();
                if (lw.equals("")) {
                    continue;
                }
                // No queremos contar espacios
                // Si la palabra existe en el hashmap incrementa en 1 su
                // valor,
                // en caso contrario la agrega y le asigna 1.
                palabrasLinea.put(lw, palabrasLinea.containsKey(lw) ? (palabrasLinea.get(lw) + 1) : 1);
            }
        }
        for (String k : palabrasLinea.keySet()) {
            context.write(new Text(k), new IntWritable(palabrasLinea.get(k)));
        }

    }
}
