package p1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, LongWritable>{
	public MyMapper()
	{
		System.out.println("MyMapper():"+hashCode());
	}
	private final static LongWritable one= new LongWritable(1);
	private Text word=new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		System.out.println("map(-,-,-)");
		System.out.println("key="+key+", Value="+value+", Context="+context);
		
		StringTokenizer itr=new StringTokenizer(value.toString());
		
		while(itr.hasMoreTokens())
		{
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}
