package statistics;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.StringTokenizer;

import modClasses.LongArrayWriter;
import modClasses.VLongArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Input graph as edge list (src dst)
//output graph as adjacency list (src count_0 count_1 total)

public class degreeStat {

	public static class degreeStatMapper extends
			Mapper<Object, Text, VLongWritable, VLongArrayWritable> {
		// input: (src dst)
		// output: (src dst 0) & (dst src 1)
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String input = value.toString();

			if (!input.startsWith("#") && input.trim().length() != 0) {
				StringTokenizer st = new StringTokenizer(input, " \t<>.");
				// Assume input format: (src dst)

				// Set original (src dst)
				VLongWritable src = new VLongWritable();

				VLongArrayWritable dst_array = new VLongArrayWritable();
				VLongWritable[] dst = new VLongWritable[2];
				dst[0] = new VLongWritable();
				dst[1] = new VLongWritable(0);

				src.set((Long.parseLong(st.nextToken())));
				dst[0].set((Long.parseLong(st.nextToken())));
				dst_array.set(dst);

				context.write(src, dst_array);

				// Set copy (dst src) for undirected graphs
				VLongWritable src2 = new VLongWritable(dst[0].get());

				VLongWritable[] dst2 = new VLongWritable[2];
				dst2[0] = new VLongWritable(src.get());
				dst2[1] = new VLongWritable(1);
				VLongArrayWritable dst_array2 = new VLongArrayWritable();
				dst_array2.set(dst2);

				context.write(src2, dst_array2);

			}
		}
	}

	public static class degreeStatCombiner
			extends
			Reducer<VLongWritable, VLongArrayWritable, VLongWritable, VLongArrayWritable> {
		// input (src dst 0) & (src dst 1)
		// output (src dst1 dst2 .. dstx 0) & (src dst1 dst2 .. dstx 1)
		public void reduce(VLongWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			HashSet<Long> edge_set_0 = new HashSet<Long>();
			HashSet<Long> edge_set_1 = new HashSet<Long>();

			VLongArrayWritable edge_list_0 = new VLongArrayWritable();
			VLongArrayWritable edge_list_1 = new VLongArrayWritable();

			for (VLongArrayWritable dst : values) {
				for (int i = 0; i < dst.get().length - 1; i++) {
					long abc = ((VLongWritable) (dst.get())[i]).get();
					long type = ((VLongWritable) (dst.get())[dst.get().length - 1])
							.get();
					if (type == 0) {
						edge_set_0.add(abc);
					}
					else{
						edge_set_1.add(abc);
					}
				}
			}
			// output value format (dst1 dst2 .. dstx)
			VLongWritable[] edges_0 = new VLongWritable[edge_set_0.size()+1];
			VLongWritable[] edges_1 = new VLongWritable[edge_set_1.size()+1];
			
			int i = 0;
			edges_0[edge_set_0.size()] =  new VLongWritable(0);
			for (Long dst : edge_set_0) {
				edges_0[i] = new VLongWritable(dst.longValue());
				i++;
			}
			edge_list_0.set(edges_0);
			
			i = 0;
			edges_1[edge_set_1.size()] =  new VLongWritable(1);
			for (Long dst : edge_set_1) {
				edges_1[i] = new VLongWritable(dst.longValue());
				i++;
			}
			edge_list_1.set(edges_1);
			
			
			context.write(key, edge_list_0);
			context.write(key, edge_list_1);
		}
	}

	public static class degreeStatReducer
			extends
			Reducer<VLongWritable, VLongArrayWritable, VLongWritable, VLongArrayWritable> {
		// output (src dst1 dst2 .. dstx 0) & (src dst1 dst2 .. dstx 1)
		// output (src dst 0) & (dst src 1)
		public void reduce(VLongWritable key,
				Iterable<VLongArrayWritable> values, Context context)
				throws IOException, InterruptedException {

			HashSet<Long> edge_set_0 = new HashSet<Long>();
			HashSet<Long> edge_set_1 = new HashSet<Long>();

			VLongArrayWritable result_list = new VLongArrayWritable();

			for (VLongArrayWritable dst : values) {
				for (int i = 0; i < dst.get().length - 1; i++) {
					long abc = ((VLongWritable) (dst.get())[i]).get();
					long type = ((VLongWritable) (dst.get())[dst.get().length - 1])
							.get();
					if (type == 0) {
						edge_set_0.add(abc);
					}
					else{
						edge_set_1.add(abc);
					}
				}
			}
			// output value format (dst1 dst2 .. dstx)
			VLongWritable[] result = new VLongWritable[3];
			
			result[0] = new VLongWritable(edge_set_0.size());
			result[1] = new VLongWritable(edge_set_1.size());
			result[2] = new VLongWritable(edge_set_0.size()+edge_set_1.size());
			
			result_list.set(result);
			
			context.write(key, result_list);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setQuietMode(true);

		Job job = new Job(conf, "1- Degree Stat");

		job.setJarByClass(degreeStat.class);
		job.setMapperClass(degreeStatMapper.class);
		job.setCombinerClass(degreeStatCombiner.class);
		job.setReducerClass(degreeStatReducer.class);
		job.setOutputFormatClass(LongArrayWriter.class);

		job.setOutputKeyClass(VLongWritable.class);
		job.setOutputValueClass(VLongArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setNumReduceTasks(1);
		// submit and wait
		System.exit(job.waitForCompletion(false) ? 0 : 1);
	}
}
