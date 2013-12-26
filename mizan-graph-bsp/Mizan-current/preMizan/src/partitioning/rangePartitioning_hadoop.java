package partitioning;

import java.io.IOException;
import java.util.StringTokenizer;

import modClasses.extraKeyTextWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import prePartition.findMax;
import prePartition.findMax.findMaxCombiner;
import prePartition.findMax.findMaxMapper;
import prePartition.findMax.findMaxReducer;
import prePartition.findMax.mapRedMessage;

//input graph in format (src [weight] dst)
//output (vertex+1 partID)

public class rangePartitioning_hadoop {

	//input graph in format (src [weight] dst)
	//output ((src+disp) partID) && ((dst+disp) partID) ==> partID = vertexID % totalParts
	
	public static class rangeMapper extends
			Mapper<Object, Text, VLongWritable, IntWritable> {
		
		long disp;  
		long totalVertex;
		int totalParts;
		int edgeWeight;
		long sizeEach;
		

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.totalVertex = Long.parseLong(context.getConfiguration().get(
					"MAX_VERTEX"));
			this.totalParts = Integer.parseInt(context.getConfiguration().get(
					"TOTAL_PARTS"));
			this.disp = Long.parseLong(context.getConfiguration().get(
			"VERTEX_DISPLACEMENT"));
			this.edgeWeight = Integer.parseInt(context.getConfiguration().get(
			"EDGE_WEIGHT"));
			sizeEach = (long) Math.ceil((double)this.totalVertex / (double)totalParts);
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String input = value.toString();
			if (!input.startsWith("#") && input.trim().length() != 0) {
				StringTokenizer st = new StringTokenizer(input, " \t");
				long srcV = Long.parseLong(st.nextToken());
				//Ignores edge weights
				for (int i=0;i<edgeWeight;i++){
					st.nextToken();
				}
				long dstV = Long.parseLong(st.nextToken());

				int srcSubG = (int) (srcV / sizeEach);
				int dstSubG = (int) (dstV / sizeEach);

				VLongWritable key1 = new VLongWritable(srcV+disp);
				VLongWritable key2 = new VLongWritable(dstV+disp);

				IntWritable val1 = new IntWritable(srcSubG);
				IntWritable val2 = new IntWritable(dstSubG);

				context.write(key1, val1);
				context.write(key2, val2);
			}
		}
	}
	
	//input (vertex partID)
	//output (vertex partID) ==> single copy
	public static class rangeReducer extends
			Reducer<VLongWritable, IntWritable, VLongWritable, IntWritable> {
		
		public void reduce(VLongWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			IntWritable outValue = values.iterator().next();
			VLongWritable key2 = new VLongWritable(key.get());
			context.write(key2, outValue);
		}
	}

	public static void main(String[] args) throws Exception {
		// ############## Stage1: findMax ####################
		Configuration conf = new Configuration();
		conf.setQuietMode(true);
		conf.set("EDGE_WEIGHT", 0 + "");

		Job job = new Job(conf, "FindMaxVertex");

		job.setJarByClass(findMax.class);
		job.setMapperClass(findMaxMapper.class);
		job.setCombinerClass(findMaxCombiner.class);
		job.setReducerClass(findMaxReducer.class);

		job.setOutputKeyClass(VLongWritable.class);
		job.setOutputValueClass(VLongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("mizan_output1"));
		job.setNumReduceTasks(1);

		boolean stage1 = job.waitForCompletion(false);

		long maxVertex = job.getCounters()
				.findCounter(mapRedMessage.MAX_VERTEX).getValue();

		System.out.println("MaxVertex = " + maxVertex);
		
		// ############## Stage2: rangePartition ####################
		
		Configuration conf2 = new Configuration();
		conf2.setQuietMode(true);

		conf2.set("TOTAL_PARTS", args[0] + "");
		conf2.set("VERTEX_DISPLACEMENT", 1+"");
		conf2.set("EDGE_WEIGHT", 0+"");
		conf2.set("MAX_VERTEX", maxVertex+"");

		Job job2 = new Job(conf2, "rangeMapper");
		job2.setOutputFormatClass(extraKeyTextWriter.class);
		job2.setJarByClass(rangePartitioning_hadoop.class);
		job2.setMapperClass(rangeMapper.class);
		job2.setCombinerClass(rangeReducer.class);
		job2.setReducerClass(rangeReducer.class);

		job2.setOutputKeyClass(VLongWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		// job.setNumReduceTasks(Integer.parseInt(args[0]));
		// submit and wait
		System.exit((job2.waitForCompletion(false) & stage1) ? 0 : 1);
	}
}
