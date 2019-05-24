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

//Import statements for reading file containing the stop-words and to store them in a HashSet.
import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.Set;
import java.util.Scanner;

public class WordCount {
public static class TokenizerMapper
extends Mapper < Object , Text , Text , IntWritable >{
private final static IntWritable one = new IntWritable (1);
private Text word = new Text ();

private Set<String> stopWord;

//Function to read file containing stop-words and store them in a string HashSet for look-up.
public void setup(Context context)throws IOException{
stopWord=new HashSet<>();
Scanner file= new Scanner(new File("stopwords.txt"));
while(file.hasNext())
{
stopWord.add(file.next().trim().toLowerCase());
}
}


public void map ( Object key , Text value , Context context
) throws IOException , InterruptedException {
StringTokenizer itr = new StringTokenizer ( value . toString ());
while ( itr . hasMoreTokens ()) {
String itr1= new String(itr.nextToken());

//Checking if token being analysed is present in the HashSet variable containing stop-words. If present, key-value pair is not passed as output.
if(!stopWord.contains(itr1))
{
word.set(itr1);
context.write(word,one);
}
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