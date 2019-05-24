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
StringTokenizer itr = new StringTokenizer ( value . toString ());
while ( itr . hasMoreTokens ()) {
String itr1= new String(itr.nextToken());

//Checking if token belongs to the me/my/mine/I set of words
if(itr1.equals("me")||itr1.equals("my")||itr1.equals("mine")||itr1.equals("I"))
{
word.set("me/my/mine/I");
context . write ( word , one );
}

//Checking if token belongs to the us/our/ours/we set of words
if(itr1.equals("us")||itr1.equals("our")||itr1.equals("ours")||itr1.equals("we"))
{
word.set("us/our/ours/we");
context . write ( word , one );
}
}
}
}

public static class IntSumReducer
extends Reducer < Text , IntWritable , Text , IntWritable > {
private IntWritable result = new IntWritable ();

public void reduce ( Text key , Iterable < IntWritable > values ,

Context context
) throws IOException , InterruptedException {

int sum = 0;
for ( IntWritable val : values ) {
sum += val . get ();
}
result . set ( sum );
context . write ( key , result );
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