import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Ex6 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text sN = new Text();
		private Text tT = new Text();


		public Boolean isOnlyLetters(String s) {
			char[] chars = s.toCharArray();

			for (char c : chars) {
				if (!Character.isLetter(c)) {
					return false;
				}
			}

			return true;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			try {
				String tweet = value.toString();
				String[] tuple = tweet.split("\\n");

				for (int i = 0; i < tuple.length; i++) {
					JSONObject jsonObj = new JSONObject(tuple[i]);
					String screenName = (String) ((JSONObject) jsonObj.get("user")).get("screen_name");
					String tweetText = (String) ((JSONObject) jsonObj.get("extended_tweet")).get("full_text");
					sN.set(screenName.toLowerCase());
					tT.set(tweetText);
					context.write(sN, tT);
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

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			try{
                JSONObject obj = new JSONObject();
                JSONArray ja = new JSONArray();
                Integer count = 0;
                for(Text val : values){
                    JSONObject jo = new JSONObject().put("message" + count.toString(), val.toString());
                    ja.put(jo);
                    count++;
                }
                obj.put("messages", ja);
                context.write(key, new Text(obj.toString()));
            }catch(JSONException e){
                e.printStackTrace();
            }
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Ex6.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}