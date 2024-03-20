import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


public class Step1 {

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
		String[] symbols = {"+", "'", ",", "`", "/", "-", "@", "\"", "!", "#", "*",  "%", "^", "(", ")", "$", "[", "]", ">", "<", "•", "~", "\"", "&", "_", "{", "}", "=", "|", "?", ";", ":", "."};
		String[] heb_stopWords = {
				"״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד",
				"מן", "מכל", "מי", "מהם", "מה", "מ", "למה", "לכל", "לי", "לו",
				"להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים",
				"יותר", "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל",
				"והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה", "היא",
				"הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא",
				"את", "אשר", "אם", "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר",
				"אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*",
				"\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם",
				"מקום", "לעולם", "לנו", "להם", "ישראל", "יודע", "זאת", "השמים",
				"הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום", "בהם",
				"אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני", "שכל",
				"שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם",
				"לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ואין", "הן",
				"היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או",
				"אבל", "א"
        };
		String[] eng_stopWords = {"a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone",
        "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and",
        "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around", "as", "at", "back",
        "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being",
        "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can",
        "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
        "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough",
        "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify",
        "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front",
        "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter",
        "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "i",
        "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter",
        "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more",
        "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never",
        "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of",
        "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours",
        "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same", "see",
        "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere",
        "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still",
        "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there",
        "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thick", "thin", "third", "this",
        "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward",
        "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we",
        "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby",
        "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom",
        "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself",
        "yourselves" };

		public HashSet<String> stopWordsMap;

		protected void setup(Context context) throws IOException, InterruptedException {
			if (context.getConfiguration().get("lang").equalsIgnoreCase("heb"))
				stopWordsMap = new HashSet<>(Arrays.asList(heb_stopWords));
			else
				stopWordsMap = new HashSet<>(Arrays.asList(eng_stopWords));

		}

		@Override
		public void map(LongWritable key, Text value, Context context) {
			boolean flag = true;
			try {
				String[] line = value.toString().split("\t");
				String[] bigram = line[0].split(" ");
				if (bigram.length == 2 && bigram[0].length()>1 && bigram[1].length()>1) {
					String word1 = line[0].split(" ")[0];
					String word2 = line[0].split(" ")[1];
					String year = String.valueOf(Integer.parseInt(line[1]) - (Integer.parseInt(line[1]) % 10));	//ex: 1963 -> 1960
					String count = line[2];
					String NcounterName = "N_Year_" + year;
					for (String symbolCheck : symbols) {
						if ( word2.contains(symbolCheck) || word1.contains(symbolCheck) ) {
							flag = false;
							break;
						}
					}
					// for (String ban : banned) {
					// 	if (word1.equalsIgnoreCase(ban) || word2.equalsIgnoreCase(ban)) {
					// 		flag = false;
					// 		break;
					// 	}
					// }
					if (stopWordsMap.contains(word1.toLowerCase()) || stopWordsMap.contains(word2.toLowerCase()))
						flag = false;
					if (flag) {
						context.write(new Text(word1 + "," + "*" + "," + year), new Text(count)); // (w1,*,year	count)
						context.write(new Text(word1 + "," + word2 + "," + year), new Text(count)); // (w1,w2,year	count)
						context.getCounter("NCounter", NcounterName).increment(Integer.parseInt(count));	//inc the proper decade counter
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		int cw1 = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("Setupping reduce() step1");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			try {
				if (key.toString().contains("*")) {	//<w,*>
					cw1 = 0;
					for (Text val : values)
						cw1 = cw1 + Integer.parseInt(val.toString());
				} else {							//<W1, W2>
					for (Text val : values)
						sum = sum + Integer.parseInt(val.toString());
					context.write(key, new Text(sum + "_" + cw1));
				}
			}
			catch (Exception e) {
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
				System.out.println("Error in Step1 Combiner --> key: " + key.toString() );
			}
		}
	}

	public static class PartitionerClass extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String[] partitions = {"1530", "1540", "1560", "1620","1650", "1660", "1670", "1680","1690", "1700","1710","1720","1730","1740", "1750", "1760","1770",
					"1780", "1790", "1800", "1810", "1820", "1830", "1840", "1850", "1860", "1870", "1880",
					"1890", "1900", "1910", "1920", "1930", "1940", "1950", "1960", "1970", "1980", "1990", "2000","2010"};
			String year = key.toString().split(",")[2];
			for(int i=0; i<partitions.length; i++)
				if(partitions[i].equalsIgnoreCase(year))
					return (i % numPartitions);
			return 0;
		}
	}
}
