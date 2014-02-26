package uniandes.reuters.job;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import uniandes.mapRed.WCMapper;
import uniandes.mapRed.WCMapperContarNoticias;
import uniandes.mapRed.WCMapperPalabraEnTitulosDeNoticias;
import uniandes.mapRed.WCReducer;

public class WordCounter {

	public static final int DEFAULT = 0;
	public static final int CONTAR_NOTICIAS = 1;
	public static final int PALABRA_EN_TITULOS_DE_NOTICIAS = 2;
	public static final int TITULO_Y_NOTICIA_CON_MAS_PALABRAS = 3;
	
	public static void main(String[] args)  {
		if(args.length<3){
			System.out.println("Se necesitan las carpetas de entrada y salida y el modo de ejecución");
			System.exit(-1);
		}
		String entrada = args[0]; //carpeta de entrada
		String salida = args[1];//La carpeta de salida no puede existir
		int modo = Integer.parseInt(args[2]);
		
		try {
			ejecutarJob(entrada, salida, modo);
		} catch (Exception e) { //Puede ser IOException, ClassNotFoundException o InterruptedException
			e.printStackTrace();
		} 
		
	}
	
	
	/**
	 * @modo Para señalizar cuál de los puntos del taller se quiere ejecutar
	 */
	public static void ejecutarJob(String entrada, String salida, int modo) throws IOException,ClassNotFoundException, InterruptedException
	{
		/**
		 * Objeto de configuraci�n, dependiendo de la versi�n de Hadoop 
		 * uno u otro es requerido. 
		 * */
		Configuration conf = new Configuration();
		JobConf conf2=new JobConf();
		conf2.setJarByClass(WordCounter.class);
		
		Job wcJob=new Job(conf, "WordCounter Job");
		wcJob.setJarByClass(WordCounter.class);
		
		switch (modo) {
		case CONTAR_NOTICIAS:
			System.out.println("Modo contar noticias");
			wcJob.setMapperClass(WCMapperContarNoticias.class);
			wcJob.setMapOutputKeyClass(Text.class);
			wcJob.setMapOutputValueClass(IntWritable.class);
			break;
		case PALABRA_EN_TITULOS_DE_NOTICIAS:
			System.out.println("Modo palabra en titulos de noticias");
			wcJob.setMapperClass(WCMapperPalabraEnTitulosDeNoticias.class);
			wcJob.setMapOutputKeyClass(Text.class);
			wcJob.setMapOutputValueClass(IntWritable.class);
			break;
		case TITULO_Y_NOTICIA_CON_MAS_PALABRAS:
			System.out.println("Modo titulo y noticia con más palabras");
		case DEFAULT:
			wcJob.setMapperClass(WCMapper.class);
			wcJob.setMapOutputKeyClass(Text.class);
			wcJob.setMapOutputValueClass(IntWritable.class);
			break;
		}
		
		///////////////////////////
		//Reducer
		///////////////////////////
		wcJob.setReducerClass(WCReducer.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(IntWritable.class);
		
		///////////////////////////
		//Input Format
		///////////////////////////
		//Advertencia: Hay dos clases con el mismo nombre, 
		//pero no son equivalentes. 
		//Se usa, en este caso, org.apache.hadoop.mapreduce.lib.input.TextInputFormat
		TextInputFormat.setInputPaths(wcJob, new Path(entrada));
		wcJob.setInputFormatClass(TextInputFormat.class); 
		
		////////////////////
		///Output Format
		//////////////////////
		TextOutputFormat.setOutputPath(wcJob, new Path(salida));
		wcJob.setOutputFormatClass(TextOutputFormat.class);
		System.out.println(wcJob.toString());
		wcJob.waitForCompletion(true);
	}
}
