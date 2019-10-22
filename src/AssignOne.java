import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class AssignOne {
	// Mapper 1: From input to key:user_id, value:(movie_id,rating)
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,MovieAndRatingWritable>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieAndRatingWritable>.Context context)
				throws IOException, InterruptedException {
			// Process input (0:user_id,1:movie_id,2:rating,3:timestamp)
			String[] inputs = value.toString().split("::");
			// Key: user_id
			Text user_id = new Text(inputs[0]);
			// Value: Create movie and rating writable as the value
			Text movie_id = new Text(inputs[1]);
			IntWritable rating = new IntWritable(Integer.parseInt(inputs[2]));
			MovieAndRatingWritable movieAndRatingWritable = new MovieAndRatingWritable(movie_id,rating);
			
			context.write(user_id, movieAndRatingWritable);
			//Debug
			System.out.println("Mapper added key: " + user_id.toString() + " value: " + movieAndRatingWritable.toString());
		}
		
	}
	// Reducer 1
	public static class Reducer1 extends Reducer<Text, MovieAndRatingWritable, Text, MovieAndRatingArrayWritable>{

		@Override
		protected void reduce(Text key, Iterable<MovieAndRatingWritable> values,
				Reducer<Text, MovieAndRatingWritable, Text, MovieAndRatingArrayWritable>.Context context)
				throws IOException, InterruptedException {
			// Put all values into an array
			int size = 0;
			int cnt = 0;

			ArrayList<MovieAndRatingWritable> tempArrayList = new ArrayList<MovieAndRatingWritable>();
			for(MovieAndRatingWritable value : values) {
				Text movie_id = new Text(value.getMovieId());
				IntWritable rating = new IntWritable(value.getRating().get());
				tempArrayList.add(new MovieAndRatingWritable(movie_id,rating));
			}

			//Debug
			for(MovieAndRatingWritable e:tempArrayList) {
				//Debug
				System.out.println("In arryList: movie " + e.getMovieId().toString() + " and rating " + e.getRating().toString());
			}
			
			Object[] tempObjectArray = tempArrayList.toArray();
			MovieAndRatingWritable[] tempMovieAndRatingArray 
			= Arrays.copyOf(tempObjectArray, tempObjectArray.length, MovieAndRatingWritable[].class);

			MovieAndRatingArrayWritable movieAndRatingArrayWritable 
			= new MovieAndRatingArrayWritable(tempMovieAndRatingArray);
			
			context.write(key, movieAndRatingArrayWritable);
			
			//Debug
			System.out.println("ArrayWritable is: " + movieAndRatingArrayWritable.toString());
			//System.out.println("Reducer find key: " + key.toString() + " watched " + Integer.toString(tempMovieAndRatingArray.length) + " movies");
		}
	}
	// Custom Value
	public static class MovieAndRatingWritable implements Writable{
		
		private Text _movie_id;
		private IntWritable _rating;
		
		// Constructor
		public MovieAndRatingWritable() {
			_movie_id = new Text();
			_rating = new IntWritable();
		}
		public MovieAndRatingWritable(Text movie_id, IntWritable rating) {
			_movie_id = movie_id;
			_rating = rating;
		}
		// Getter
		public Text getMovieId() {
			return _movie_id;
		}
		public IntWritable getRating() {
			return _rating;
		}
		// Setter
		public void setMovieId(Text movie_id) {
			this._movie_id = movie_id;
		}
		public void setRating(IntWritable rating) {
			this._rating = rating;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			_movie_id.readFields(in);
			_rating.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			_movie_id.write(out);
			_rating.write(out);
		}
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return _movie_id.toString() + "," + _rating.toString();
		}
		
		
	}
	// Custom Array Value
	public static class MovieAndRatingArrayWritable extends ArrayWritable{

		// Constructor
		public MovieAndRatingArrayWritable() {
			super(MovieAndRatingWritable.class);
		}
		public MovieAndRatingArrayWritable(MovieAndRatingWritable[] values) {
			super(MovieAndRatingWritable.class, values);
		}

		@Override
		public MovieAndRatingWritable[] get() {
			return (MovieAndRatingWritable[])super.get();
		}
		
		@Override
		public String toString() {
			MovieAndRatingWritable[] values = get();
			String string = "";
			for(int i = 0;i < values.length; i++) {
				string += " " + values[i].toString();
			}
			return string;
		}
	}
	// Mapper 2
	public static class Mapper2 extends Mapper<Text, MovieAndRatingArrayWritable, MoviePair, UserAndRatingArrayWritable>{
		
	}
	// Custom Key
	@SuppressWarnings("rawtypes")
	public static class MoviePair implements WritableComparable{
		
		private Text _movie_id_1;
		private Text _movie_id_2;
		
		// Constructor
		public MoviePair() {
			_movie_id_1 = new Text();
			_movie_id_2 = new Text();
		}
		
		public MoviePair(Text movie_id_1, Text movie_id_2) {
			_movie_id_1 = movie_id_1;
			_movie_id_2 = movie_id_2;
		}
		
		// Getter
		public Text getMovieId1() {
			return _movie_id_1;
		}
		
		public Text getMovieId2() {
			return _movie_id_2;
		}
		
		// Setter
		public void setMovieId1(Text movie_id_1) {
			this._movie_id_1 = movie_id_1;
		}
		
		public void setMovieId2(Text movie_id_2) {
			this._movie_id_2 = movie_id_2;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			_movie_id_1.readFields(in);
			_movie_id_2.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			_movie_id_1.write(out);
			_movie_id_2.write(out);
		}
		
		@Override
		public int compareTo(Object moviePair) {
			Text movieId1 = ((MoviePair)moviePair)._movie_id_1;
			Text movieId2 = ((MoviePair)moviePair)._movie_id_2;
			return (this._movie_id_1 == movieId1) && (this._movie_id_2 == movieId2)?1:0;
		}

		@Override
		public String toString() {
			return "(" + _movie_id_1.toString() + "," + _movie_id_2.toString() + ")";
		}
	}
	// Custom Value
	public static class UserAndRatingWritable implements Writable{
		private Text _user_id;
		private IntWritable _rating_1;
		private IntWritable _rating_2;
		
		// Constructor
		public UserAndRatingWritable() {
			_user_id = new Text();
			_rating_1 = new IntWritable();
			_rating_2 = new IntWritable();
		}
		
		public UserAndRatingWritable(Text user_id, IntWritable rating_1, IntWritable rating_2) {
			_user_id = user_id;
			_rating_1 = rating_1;
			_rating_2 = rating_2;
		}
		
		// Getter
		public Text getUserId() {
			return _user_id;
		}
		
		public IntWritable getRating1() {
			return _rating_1;
		}
		
		public IntWritable getRating2() {
			return _rating_2;
		}
		
		// Setter
		public void setUserId(Text user_id) {
			this._user_id = user_id;
		}
		
		public void setRating1(IntWritable rating_1) {
			this._rating_1 = rating_1;
		}
		
		public void setRating2(IntWritable rating_2) {
			this._rating_2 = rating_2;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			_user_id.readFields(in);
			_rating_1.readFields(in);
			_rating_2.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			_user_id.write(out);
			_rating_1.write(out);
			_rating_2.write(out);
		}
		
		@Override
		public String toString() {
			return "(" + _user_id.toString() + "," + _rating_1.toString() + "," + _rating_2.toString() + ")";
		}
	}
	// Custom Array Value
	public static class UserAndRatingArrayWritable extends ArrayWritable{
		// Constructor
		public UserAndRatingArrayWritable() {
			super(UserAndRatingWritable.class);
		}
		
		public UserAndRatingArrayWritable(UserAndRatingWritable[] values) {
			super(UserAndRatingWritable.class, values);
		}

		@Override
		public UserAndRatingWritable[] get() {
			return (UserAndRatingWritable[])super.get();
		}

		@Override
		public String toString() {
			UserAndRatingWritable[] values = get();
			String string = "";
			for(int i = 0;i < values.length;i++) {
				if (i == 0) string += "[";
				string += values[i].toString();
				if (i == values.length) {
					string += "]";
				}else {
					string += ",";
				}
			}
			return super.toString();
		}
		
		
	}
	// Main
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path out = new Path(args[1]);
		
		// Map Reduce 1
		Job job1 = Job.getInstance(conf,"MapReduce1");
		job1.setJarByClass(AssignOne.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MovieAndRatingWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(MovieAndRatingArrayWritable.class);
		//job1.setOutputFormatClass(SequenceFileOutputFormat.class); //Enable when need to pass object to the 2nd MapReduce, else print to file
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(out,"out1"));
		
		FileSystem hdfs = FileSystem.get(conf);
		if(hdfs.exists(out)) {
			hdfs.delete(out,true);
		}
		if(!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		// Map Reduce 2
	}
}
