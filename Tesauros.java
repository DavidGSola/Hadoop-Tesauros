
import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Collections;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Tesauros 
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        /*
            Patrones para eliminar los símbolos del texto
        */
        static final Pattern ptrnSimbolos = Pattern.compile("[\\.\\s]?[\\s+,\\*\\+\\:\\;\\?\\!><]+");
        
        /*
            Set con las parejas calculadas
        */
        HashSet<String> parejas = new HashSet<>();
        
        /*
            ArrayList con todas las palabras que se van obteniendo del texto
        */
        ArrayList<String> palabrasTotales = new ArrayList<String>();
        
        /*
            Lista con las palabras sin significado
        */
        static ArrayList<String> stopWords;
        static 
        {
            String[] arr_stopWords = new String[]{"a","about","above","across","after","afterwards","again","against","all","almost","alone","along","already","also","although","always","am","among","amongst","amoungst","amount","an","and","another","any","anyhow","anyone","anything","anyway","anywhere","are","around","as","at","back","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","below","beside","besides","between","beyond","bill","both","bottom","but","by","call","can","cannot","cant","co","computer","con","could","couldnt","cry","de","describe","detail","do","done","down","due","during","each","eg","eight","either","eleven","else","elsewhere","empty","enough","etc","even","ever","every","everyone","everything","everywhere","except","few","fifteen","fify","fill","find","fire","first","five","for","former","formerly","forty","found","four","from","front","full","further","get","give","go","had","has","hasnt","have","he","hence","her","here","hereafter","hereby","herein","hereupon","hers","herse","him","himse","his","how","however","hundred","i","ie","if","in","inc","indeed","interest","into","is","it","its","itse","keep","last","latter","latterly","least","less","ltd","made","many","may","me","meanwhile","might","mill","mine","more","moreover","most","mostly","move","much","must","my","myse","name","namely","neither","never","nevertheless","next","nine","no","nobody","none","noone","nor","not","nothing","now","nowhere","of","off","often","on","once","one","only","onto","or","other","others","otherwise","our","ours","ourselves","out","over","own","part","per","perhaps","please","put","rather","re","same","see","seem","seemed","seeming","seems","serious","several","she","should","show","side","since","sincere","six","sixty","so","some","somehow","someone","something","sometime","sometimes","somewhere","still","such","system","take","ten","than","that","the","their","them","themselves","then","thence","there","thereafter","thereby","therefore","therein","thereupon","these","they","thick","thin","third","this","those","though","three","through","throughout","thru","thus","to","together","too","top","toward","towards","twelve","twenty","two","un","under","until","up","upon","us","very","via","was","we","well","were","what","whatever","when","whence","whenever","where","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","whoever","whole","whom","whose","why","will","with","within","without","would","yet","you","your","yours","yourself","yourselves"};
            stopWords = new ArrayList<>(Arrays.asList(arr_stopWords));
        }
        
        /*
            Lista con las palabras clave de las cabeceras
        */
        static ArrayList<String> cabeceraWords;
        static 
        {
            String[] arr_cabeceraWords = new String[]{"Message-ID:", "Date:", "From:", "To:", "Subject:", "Cc:", "Mime-Version:", "Content-Type:", "Content-Transfer-Encoding:", "Bcc:", "X-From:", "X-To:", "X-cc:", "X-bcc:", "X-Folder:", "X-Origin:", "X-FileName:"};
            cabeceraWords = new ArrayList<>(Arrays.asList(arr_cabeceraWords));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String text = value.toString();
            ArrayList<String> palabrasLinea = new ArrayList<>();

            // Comprobamos si la línea actual es parte de la cabecera
            boolean esCabecera = false;
            for(int i=0; i < cabeceraWords.size() && !esCabecera; i++)
                esCabecera = text.contains(cabeceraWords.get(i));

            if(!esCabecera)
            {
                // Limpiamos los símbolos del texto
                text = ptrnSimbolos.matcher(text).replaceAll("~|~");
                
                // Recorremos los Tokens y los vamos añadiendo a una lista con todas las
                // palabras de esta línea
                StringTokenizer itr = new StringTokenizer(text,"~|~");
                while (itr.hasMoreTokens())
                    palabrasLinea.add(itr.nextToken());
                
                // Borramos las palabras sin significado a partir del Array
                palabrasLinea.removeAll(stopWords);
                
                // Obtenemos el índice de la última palabra de las anteriores ejecuciones
                int lastIndex = palabrasTotales.size() == 0 ? 0 : palabrasTotales.size()-1;

                // Añadimos las palabras de esta línea a las palabras totales
                palabrasTotales.addAll(palabrasLinea);
                
                // Reccorremos el conjunto de palabras totales evitando repetir parejas que ya hemos
                // realizado en iteraciones anteriores (para ello utilizamos el lastIndex calculado)
                for(int i=0; i<palabrasTotales.size(); i++)
                    for(int j=lastIndex; j<palabrasTotales.size(); j++)
                        if(i != j)
                        {
                            String concat;
                            if(palabrasTotales.get(i).compareTo(palabrasTotales.get(j)) <= 0)
                                concat = palabrasTotales.get(i) + " | " + palabrasTotales.get(j);
                            else
                                concat = palabrasTotales.get(j) + " | " + palabrasTotales.get(i);

                            if(!parejas.contains(concat))
                            {
                                word.set(concat);
                                context.write(word, one);
                                parejas.add(concat);
                            }
                        }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
    {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();

            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tesauros");
        job.setJarByClass(Tesauros.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


 
