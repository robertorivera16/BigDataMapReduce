import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONObject;

public class Ex1 {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {

				String tweet = value.toString();
				String[] tuple = tweet.split("\\n");
				

				for (int i = 0; i < tuple.length; i++) {
					JSONObject jsonObj = new JSONObject(tuple[i]);
					String tw = (String) ((JSONObject) jsonObj.get("extended_tweet")).get("full_text");
					StringTokenizer itr = new StringTokenizer(tw, " ?!.1234567890#()@'$%^&*-_");

					while (itr.hasMoreTokens()) {
						String w = itr.nextToken();
						w = w.toLowerCase();
						
						if (w.equalsIgnoreCase("trump") || 
								w.equalsIgnoreCase("flu") ||
								w.equalsIgnoreCase("zika") ||
								w.equalsIgnoreCase("diarrhea") ||
								w.equalsIgnoreCase("ebola") ||
								w.equalsIgnoreCase("headache") ||
								w.equalsIgnoreCase("measles")) {
							
							word.set(w.toLowerCase());
							context.write(word, one);
						}

					}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Ex1.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}