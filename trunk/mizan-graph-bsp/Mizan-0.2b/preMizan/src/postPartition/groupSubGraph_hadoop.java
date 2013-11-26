package postPartition;

import java.io.IOException;
import java.util.StringTokenizer;

import modClasses.VLongArrayWritable;
import modClasses.LongArrayWriter;
import modClasses.noKeyLongWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class groupSubGraph_hadoop {

	public static class groupSubGMapper extends
			Mapper<Object, Text, IntWritable, VLongArrayWritable> {
		// input
		// output

		boolean duplicate;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.duplicate = Boolean.parseBoolean((context.getConfiguration().get(
					"DUPLICATE_OUTEDGES")));
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String input = value.toString();
			if (!input.startsWith("#") && input.trim().length() != 0) {
				StringTokenizer st = new StringTokenizer(input, " \t");
				long srcV = Long.parseLong(st.nextToken());
				long srcSubG = Long.parseLong(st.nextToken());
				long dstV = Long.parseLong(st.nextToken());
				long dstSubG = Long.parseLong(st.nextToken());

				VLongWritable[] outArray1 = new VLongWritable[4];
				outArray1[0] = new VLongWritable(srcV);
				outArray1[1] = new VLongWritable(srcSubG);
				outArray1[2] = new VLongWritable(dstV);
				outArray1[3] = new VLongWritable(dstSubG);

				VLongWritable[] outArray2 = new VLongWritable[4];
				outArray2[0] = new VLongWritable(srcV);
				outArray2[1] = new VLongWritable(srcSubG);
				outArray2[2] = new VLongWritable(dstV);
				outArray2[3] = new VLongWritable(dstSubG);

				VLongArrayWritable outVal1 = new VLongArrayWritable();
				outVal1.set(outArray1);
				VLongArrayWritable outVal2 = new VLongArrayWritable();
				outVal2.set(outArray2);

				IntWritable key1 = new IntWritable((int) srcSubG);
				IntWritable key2 = new IntWritable((int) dstSubG);

				context.write(key1, outVal1);
				if (duplicate && srcSubG != dstSubG) {
					context.write(key2, outVal2);
				}
			}
		}
	}

	public static class groupSubGReducer
			extends
			Reducer<IntWritable, VLongArrayWritable, IntWritable, VLongArrayWritable> {
		// input
		// output
		public void reduce(IntWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {
			for (VLongArrayWritable outVal : values) {
				context.write(key, outVal);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setQuietMode(true);
		
		if(args.length==4){
			conf.set("DUPLICATE_OUTEDGES", args[3]+"");
		}
		else{
			conf.set("DUPLICATE_OUTEDGES", "true");
		}
		
		
		Job job = new Job(conf, "6- groupSubGraph");

		job.setOutputFormatClass(noKeyLongWriter.class);

		job.setJarByClass(groupSubGraph_hadoop.class);
		job.setMapperClass(groupSubGMapper.class);
		// job.setReducerClass(groupSubGReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VLongArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setNumReduceTasks(Integer.parseInt(args[0]));
		// submit and wait
		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
}
