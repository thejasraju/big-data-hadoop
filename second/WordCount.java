import java . io . IOException ;
import java . util . StringTokenizer ;
import org . apache . hadoop . conf . Configuration ;
import org . apache . hadoop . fs . Path ;
import org . apache . hadoop . io . IntWritable ;
import org . apache . hadoop . io . Text ;
import org . apache . hadoop . mapreduce . Job ;
import org . apache . hadoop . mapreduce . Mapper ;
import org . apache . hadoop . mapreduce . Reducer ;
import org . apache . hadoop . mapreduce . lib . input . FileInputFormat ;
import org . apache . hadoop . mapreduce . lib . output . FileOutputFormat ;

public class WordCount {
public static class TokenizerMapper
extends Mapper < Object , Text , Text , IntWritable >{
private final static IntWritable one = new IntWritable (1);
private Text word = new Text ();
public void map ( Object key , Text value , Context context
) throws IOException , InterruptedException {

String line = value.toString();
StringTokenizer itr = new StringTokenizer (line);
while ( itr . hasMoreTokens ()) {
String itr1= new String(itr.nextToken());

//Removing punctuations, tab-spaces, line breaks, symbols, and all other alpha numeric characters
itr1=itr1.replaceAll("[.!?\\-]","");
itr1=itr1.replaceAll("\t","");
itr1=itr1.replaceAll("[\\r\\n]","");
itr1=itr1.replaceAll("[^a-zA-Z0-9\\s]","").toLowerCase();
word . set ( itr1);
context . write ( word , one );
}
}
}
public static class IntSumReducer
extends Reducer < Text , IntWritable , Text , IntWritable > {
private IntWritable result = new IntWritable ();
private static int wordCount=0;

public void reduce ( Text key , Iterable < IntWritable > values ,

Context context
) throws IOException , InterruptedException {

wordCount++;
result.set(wordCount);
}

public void cleanup(Context context) throws IOException, InterruptedException{
context . write ( new Text("WordCount") , result );
}

}
public static void main ( String [] args ) throws Exception {
Configuration conf = new Configuration ();
Job job = Job . getInstance ( conf , " word count " );
job . setJarByClass ( WordCount . class );
job . setMapperClass ( TokenizerMapper . class );
job . setCombinerClass ( IntSumReducer . class );
job . setReducerClass ( IntSumReducer . class );
job . setOutputKeyClass ( Text . class );
job . setOutputValueClass ( IntWritable . class );
FileInputFormat . addInputPath ( job , new Path ( args [0]));
FileOutputFormat . setOutputPath ( job , new Path ( args [1]));
System . exit ( job . waitForCompletion ( true ) ? 0 : 1);
}
}