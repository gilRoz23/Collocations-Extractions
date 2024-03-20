import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Step3 {
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

		public static double calculateNPMI(double c1, double c2, double c12, double N) {
			double pmi = Math.log(c12) + Math.log(N) - Math.log(c1) - Math.log(c2);
			double npmi = pmi / ((-1) * (Math.log(c12) - Math.log(N)));
			return npmi;
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String[] separatedTabs = value.toString().split("\t");
				String[] prevKey = separatedTabs[0].split(",");
				String w1 = prevKey[0];
				String w2 = prevKey[1];
				String year = prevKey[2];
				Double c1 = Double.valueOf(prevKey[3]);
				Double c2 = Double.valueOf(prevKey[4]);
				Double c12 = Double.valueOf(separatedTabs[1]);
				String NcounterName = "N_Year_" + year;
				Double currentCounter = Double.valueOf(context.getConfiguration().get(NcounterName));
				Double npmi = calculateNPMI(c1, c2, c12, currentCounter);
				context.write(new Text(year + "," + "*"), new Text("" + npmi)); // year,*	npmi
				context.write(new Text(year + "," + w1 + "," + w2 + "," + npmi), new Text("")); // year,w1,w2,npmi	placeholder
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		double npmisSum;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(key.toString().contains("*")) { // year,*	npmi
				npmisSum = 0;
				for (Text val : values) {
					npmisSum += Double.parseDouble(val.toString());
				}
			}
			else {	// year,w1,w2,npmi	placeholder
				String[] separatedCommas = key.toString().split(",");
				String year = separatedCommas[0];
				String w1 = separatedCommas[1];
				String w2 = separatedCommas[2];
				String npmi = separatedCommas[3];
				context.write(new Text(year + "," + w1 + "," + w2 + "," + npmi), new Text(String.valueOf(npmisSum)));
				// year,w1,w2,npmi	npmiSum	
			}
		}
	}

	public static class PartitionerClass extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String[] partitions = {"1530", "1540", "1560", "1620","1650", "1660", "1670", "1680","1690", "1700","1710","1720","1730","1740", "1750", "1760","1770",
					"1780", "1790", "1800", "1810", "1820", "1830", "1840", "1850", "1860", "1870", "1880",
					"1890", "1900", "1910", "1920", "1930", "1940", "1950", "1960", "1970", "1980", "1990", "2000","2010"};
			String year = key.toString().split(",")[0];
			for(int i=0; i<partitions.length; i++)
				if(partitions[i].equalsIgnoreCase(year))
					return (i % numPartitions);
			return 0;
		}
	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// try and catch maybe
			try {
				if(key.toString().contains("*")) { // year,*	npmi
					int npmiSum = 0;
					for (Text val : values) {
						npmiSum += Double.parseDouble(val.toString());
					}
					context.write(key, new Text(String.valueOf(npmiSum)));
				}
				else {
					context.write(key, new Text(""));
				}
			}
			catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}


