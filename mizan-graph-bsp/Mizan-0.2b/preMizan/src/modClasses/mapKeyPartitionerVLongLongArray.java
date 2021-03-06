package modClasses;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class mapKeyPartitionerVLongLongArray extends Partitioner<VLongWritable,VLongArrayWritable>{

	@Override
	public int getPartition(VLongWritable key, VLongArrayWritable value,
			int numPartitions) {
		return (((int) (key.get()/10)) % numPartitions);
	}
}
