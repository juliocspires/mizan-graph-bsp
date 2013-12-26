package prePartition;

import java.io.IOException;
import java.util.StringTokenizer;

import modClasses.VLongArrayWritable;
import modClasses.LongArrayWriter;
import modClasses.noKeyLongWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import prePartition.preMetisCountAppend_hadoop.mapRedMessage;

public class findMax {

	// ############## listCounterJob ##############
	public static enum mapRedMessage {
		MAX_VERTEX
	}

	// Input graph as edge list (src dst)
	// Output (max)
	public static class findMaxMapper extends
			Mapper<Object, Text, VLongWritable, VLongWritable> {
		// input: (src dst)
		// output: (max(src,dst))
		int edgeWeight;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.edgeWeight = Integer.parseInt(context.getConfiguration().get(
					"EDGE_WEIGHT"));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String input = value.toString();
			if (!input.startsWith("#") && input.trim().length() != 0) {
				StringTokenizer st = new StringTokenizer(input, " \t");

				long max = Long.parseLong(st.nextToken());

				for (int i = 0; i < this.edgeWeight; i++) {
					st.nextToken();
				}
				long dst = Long.parseLong(st.nextToken());
				max = Math.max(max, dst);

				context.write(new VLongWritable(0), new VLongWritable(max));
			}
		}
	}

	public static class findMaxCombiner extends
			Reducer<VLongWritable, VLongWritable, VLongWritable, VLongWritable> {
		// input (0 max1) (0 max2)
		// output (0 max)

		public void reduce(VLongWritable key, Iterable<VLongWritable> values,
				Context context) throws IOException, InterruptedException {

			long max = 0;
			for (VLongWritable value : values) {
				max = Math.max(((VLongWritable) value).get(), max);
			}
			context.write(key, new VLongWritable(max));
		}
	}

	public static class findMaxReducer extends
			Reducer<VLongWritable, VLongWritable, VLongWritable, VLongWritable> {

		public void reduce(VLongWritable key, Iterable<VLongWritable> values,
				Context context) throws IOException, InterruptedException {

			long max = 0;
			for (VLongWritable value : values) {
				max = Math.max(((VLongWritable) value).get(), max);
			}
			context.getCounter(mapRedMessage.MAX_VERTEX).increment(max);
			context.write(key, new VLongWritable(max));

		}
	}

	public static void main(String[] args) throws Exception {

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

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1);

		boolean stage1 = job.waitForCompletion(false);

		long maxVertex = job.getCounters()
				.findCounter(mapRedMessage.MAX_VERTEX).getValue();

		System.out.println("MaxVertex = " + maxVertex);

		System.exit((stage1) ? 0 : 1);
	}
}
