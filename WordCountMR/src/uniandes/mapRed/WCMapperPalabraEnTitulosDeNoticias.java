package uniandes.mapRed;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapperPalabraEnTitulosDeNoticias extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String noticia = value.toString();
		String[] lineas = noticia.split("\n");
		String tituloNoticia = "";
		int wordCounter = 0;

		for (int i = 0; i < lineas.length; i++) {
			String linea = lineas[i];
			if (lineas[i].toLowerCase().contains("<title>")) {
				tituloNoticia = linea;
			}
			String[] palabras = linea.split("([().,!?:'\"-]|\\s)+");
			wordCounter+=palabras.length;

		}

			context.write(new Text(tituloNoticia), new IntWritable(wordCounter));
		

	}
}
