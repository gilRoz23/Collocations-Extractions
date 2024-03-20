import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step2 {
	
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		int b = 0;
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
																													
			try {
				String[] separatedTabs = value.toString().split("\t");
				String[] prevKey = separatedTabs[0].split(",");		// w1 w2 year
				String cw1w2 = separatedTabs[1].split("_")[0];
				String cw1 = separatedTabs[1].split("_")[1];
				String w1 = prevKey[0];
				String w2 = prevKey[1];
				String year = prevKey[2];
				context.write(new Text(w2 + "," +  "*" + "," + year), new Text(cw1w2));
				context.write(new Text(w2 + "," + w1 + "," + year + "," + cw1), new Text(cw1w2));
				//we reverse the order ex: <w1,w2> -> <w2, w1> so now we want to count(w2) so in reduce() we get: <w2, *> before <w2, w1> <w2,w3> ...
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			try {
				for (Text txt : values) {
					count += Integer.parseInt(txt.toString());
				}
				context.write(key, new Text(String.valueOf(count)));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		int cw2;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			try {
				if (key.toString().contains("*")) {
					cw2 = 0;
					for (Text val : values)
						cw2 = cw2 + Integer.parseInt(val.toString());
				} else {
					for (Text val : values)
						sum = sum + Integer.parseInt(val.toString());
					String[] separatedCommas = key.toString().split(",");
					String w1 = separatedCommas[1];
					String w2 = separatedCommas[0];
					String year = separatedCommas[2];
					String cw1 = separatedCommas[3];
					context.write(new Text(w1 + "," + w2 + "," + year + "," + cw1 + "," + cw2), new Text("" + sum));
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class PartitionerClass extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int partitionsNum) {
			String[] partitions = {"1530", "1540", "1560", "1620","1650", "1660", "1670", "1680","1690", "1700","1710","1720","1730","1740", "1750", "1760","1770",
					"1780", "1790", "1800", "1810", "1820", "1830", "1840", "1850", "1860", "1870", "1880",
					"1890", "1900", "1910", "1920", "1930", "1940", "1950", "1960", "1970", "1980", "1990", "2000","2010"};
			String year = key.toString().split(",")[2];
			for(int i=0; i<partitions.length; i++)
				if(partitions[i].equalsIgnoreCase(year))
					return (i % partitionsNum);
			return 0;
		}
	}
}
