import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class Step4 {
	public static double minimalPmi ;
	public static  double relativeMinmalPmi ;

	public static class MapperClass extends Mapper<LongWritable, Text, ComparableKey, Text> {

		protected void setup(Context context) throws IOException, InterruptedException {

			minimalPmi = Double.parseDouble(context.getConfiguration().get("minimalPmi"));
			relativeMinmalPmi = Double.parseDouble(context.getConfiguration().get("relativeMinmalPmi"));
		}
		


		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] separatedTabs = value.toString().split("\t");
				String[] previousKey = separatedTabs[0].split(",");
				int year = Integer.parseInt(previousKey[0]);
				String w1 = previousKey[1];
				String w2 = previousKey[2];
				double npmi = Double.parseDouble(previousKey[3]);
				double sumValue = Double.parseDouble(separatedTabs[1]);

				double relNpmi = npmi / sumValue ; 
				ComparableKey newKey = new ComparableKey(w1,w2,year,npmi);

				if(npmi>= minimalPmi || relNpmi >= relativeMinmalPmi) {
					context.write(newKey, new Text(""));
				}



			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReducerClass extends Reducer<ComparableKey, Text, Text, Text> {
		int count = 0;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}

		@Override
		public void reduce(ComparableKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			count++;
			int year = key.getDecade();
			String w1 = key.getFirstWord();
			String w2 =key.getSecondWord();
			Double npmi = key.getnpmi();
			context.write(new Text(year + " " + w1 + " " + w2 + " " + npmi), new Text(""));
					
		}
	}

	public static class PartitionerClass extends Partitioner<ComparableKey, Text> {
		@Override
		public int getPartition(ComparableKey key, Text value, int numPartitions) {
			String[] partitions = {"1530", "1540", "1560", "1620","1650", "1660", "1670", "1680","1690", "1700","1710","1720","1730","1740", "1750", "1760","1770",
					"1780", "1790", "1800", "1810", "1820", "1830", "1840", "1850", "1860", "1870", "1880",
					"1890", "1900", "1910", "1920", "1930", "1940", "1950", "1960", "1970", "1980", "1990", "2000","2010"};
			String year = key.getDecade() + "";
			for(int i=0; i<partitions.length; i++)
				if(partitions[i].equalsIgnoreCase(year))
					return (i % numPartitions);
			return 0;
		}
	}


}
