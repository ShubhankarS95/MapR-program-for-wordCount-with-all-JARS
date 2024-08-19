package p1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	public MyReducer()
	{
		System.out.println("MyReducer()="+hashCode());
	}
	public void reduce(Text key,Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		System.out.println("reduce(-,-,-)");
		System.out.println("KEY="+key+", VALUES="+values+", Context="+context);
		
		int s=0;
		for(LongWritable v:values)
		{
			System.out.println(" "+v.get());
			s+=v.get();
			
		}
		System.out.println();
		context.write(key, new LongWritable(s));
	}
}
