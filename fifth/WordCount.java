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

import org . apache . hadoop . io . LongWritable ;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordCount{
public static class TokenizerMapper extends Mapper<LongWritable,Text,Text,Text> {
public void map(LongWritable key, Text value, Context context)
throws IOException,InterruptedException
{
//Names of documents in input path
String files = ((FileSplit) context.getInputSplit()).getPath().getName();
String lines=value.toString();
String words[]=lines.split(" ");
for(String a:words){
//key-value pair where key is the word and value is the name of the file in which it is present
context.write(new Text(a), new Text(files));
}
}
}
public static class IntSumReducer extends
Reducer<Text, Text, Text, Text> {

private static int wordCount=0;
public void reduce(Text key, Iterable<Text> values, Context context)
throws IOException, InterruptedException {
int counter=0;
HashMap hMap=new HashMap();
int flag=0;
for(Text t:values){
String str=t.toString();
//Filename presence is checked else added to hashmap if not present
if(hMap!=null &&hMap.get(str)!=null){
counter=(int)hMap.get(str);
hMap.put(str, ++counter);
}else{
//Count of the word is increased if filename is already added to HashMap
hMap.put(str, 1);
}
}
//Key-value pair is output if key present in a single file only. Key is the word and value is the file in which it occurs
if(hMap.size()==1)
{
wordCount++;
}
}

public void cleanup(Context context) throws IOException, InterruptedException{
String str=""+wordCount;
context . write ( new Text("WordCount") , new Text(str) );
}

}

public static void main(String[] args) throws Exception {
Configuration conf= new Configuration();
Job job = new Job(conf,"word count");
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(Text.class);
job.setJarByClass(WordCount.class);
job.setMapperClass(TokenizerMapper.class);
job.setReducerClass(IntSumReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}


}