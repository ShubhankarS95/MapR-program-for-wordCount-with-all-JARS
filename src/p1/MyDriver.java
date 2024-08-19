package p1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path input_dir=new Path("hdfs://localhost:9000/input_data/");
		Path output_dir=new Path("hdfs://localhost:9000/output_data/");
		
		Configuration cnf=new Configuration();
		Job job =Job.getInstance(cnf,"MyWordCountALL_JARS");
		
		
		job.setJarByClass(MyDriver.class);
		job.setMapperClass(MyMapper.class); 
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, input_dir);
		FileOutputFormat.setOutputPath(job, output_dir);
		
		output_dir.getFileSystem(cnf).delete(output_dir, true);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
