import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
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
			// DEBUG
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

			// DEBUG
			for(MovieAndRatingWritable e:tempArrayList) {
				// DEBUG
				System.out.println("In arryList: movie " + e.getMovieId().toString() + " and rating " + e.getRating().toString());
			}
			
			Object[] tempObjectArray = tempArrayList.toArray();
			MovieAndRatingWritable[] tempMovieAndRatingArray 
			= Arrays.copyOf(tempObjectArray, tempObjectArray.length, MovieAndRatingWritable[].class);

			MovieAndRatingArrayWritable movieAndRatingArrayWritable 
			= new MovieAndRatingArrayWritable(tempMovieAndRatingArray);
			
			context.write(key, movieAndRatingArrayWritable);
			
			// DEGUB
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
	public static class Mapper2 extends Mapper<Text, MovieAndRatingArrayWritable, MoviePair, UserAndRatingWritable>{

		@Override
		protected void map(Text key, MovieAndRatingArrayWritable values,
				Mapper<Text, MovieAndRatingArrayWritable, MoviePair, UserAndRatingWritable>.Context context)
				throws IOException, InterruptedException {
			// Add to unsorted ArrayList
			ArrayList<MovieAndRatingWritable> movieAndRatingArrayList = new ArrayList<MovieAndRatingWritable>();
			for(MovieAndRatingWritable value:values.get()) {
				Text movie_id = new Text(value.getMovieId());
				IntWritable rating = new IntWritable(value.getRating().get());
				MovieAndRatingWritable movieAndRatingWritable = new MovieAndRatingWritable(movie_id,rating);
				movieAndRatingArrayList.add(movieAndRatingWritable);
				// DEGUB
				System.out.println("Mapper2 - movie/rating unsoreted arraylist: added " + movie_id.toString() + " and " + rating.toString());
			}
			// Sort
			MoviePairComparator moviePairComparator = new MoviePairComparator();
			Collections.sort(movieAndRatingArrayList, moviePairComparator);
			System.out.println("Mapper2 - movie/rating sorted result: " + movieAndRatingArrayList.toString());

			// Find movie pairs
			int size = movieAndRatingArrayList.size();
			for (int i=0;i<size-1;i++) {
				for(int j=1;j<size;j++) {
					// Key
					Text movie_id_1 = new Text(movieAndRatingArrayList.get(i).getMovieId());
					Text movie_id_2 = new Text(movieAndRatingArrayList.get(j).getMovieId());
					
					MoviePair moviePair = new MoviePair(movie_id_1, movie_id_2);
					// Value
					Text user_id = new Text(key);
					IntWritable rating_1 = new IntWritable(movieAndRatingArrayList.get(i).getRating().get());
					IntWritable rating_2 = new IntWritable(movieAndRatingArrayList.get(j).getRating().get());
					
					UserAndRatingWritable userAndRatignWritable = new UserAndRatingWritable(user_id,rating_1,rating_2);
					context.write(moviePair, userAndRatignWritable);
				}
			}
		}
		
	}
	// Custom Comparator
	public static class MoviePairComparator implements Comparator<MovieAndRatingWritable>{

		@Override
		public int compare(MovieAndRatingWritable in1, MovieAndRatingWritable in2) {
			Text movie_id_1 = new Text(in1.getMovieId());
			Text movie_id_2 = new Text(in2.getMovieId());
			
			return movie_id_1.compareTo(movie_id_2);
		}
		
	}
	// Reducer 2
	public static class Reducer2 extends Reducer<MoviePair, UserAndRatingWritable, MoviePair, UserAndRatingArrayWritable>{

		@Override
		protected void reduce(MoviePair key, Iterable<UserAndRatingWritable> values,
				Reducer<MoviePair, UserAndRatingWritable, MoviePair, UserAndRatingArrayWritable>.Context context)
				throws IOException, InterruptedException {
			// Put all values into an array
			int size = 0;
			int cnt = 0;
			
			ArrayList<UserAndRatingWritable> tempArrayList = new ArrayList<UserAndRatingWritable>();
			for(UserAndRatingWritable value:values) {
				Text user_id = new Text(value.getUserId());
				IntWritable rating_1 = new IntWritable(value.getRating1().get());
				IntWritable rating_2 = new IntWritable(value.getRating2().get());
				tempArrayList.add(new UserAndRatingWritable(user_id, rating_1, rating_2));
			}
			
			// DEGUB
			for(UserAndRatingWritable e:tempArrayList) {
				System.out.println("Reducer 2 in tempArrayList: user" + e.getUserId().toString() + " rating1: " + e.getRating1().toString() + " rating2: " + e.getRating2());
			}
			
			Object[] tempObjectArray = tempArrayList.toArray();
			UserAndRatingWritable[] tempUserAndRatingArray
			= Arrays.copyOf(tempObjectArray, tempObjectArray.length, UserAndRatingWritable[].class);
			
			UserAndRatingArrayWritable userAndRatingArrayWritable
			= new UserAndRatingArrayWritable(tempUserAndRatingArray);
			
			context.write(key, userAndRatingArrayWritable);
			
			// DEGUB
			System.out.println("Reducer2 ArrayWritable is: " + userAndRatingArrayWritable.toString());
		}
		
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
		// Delete old folders
		File temp_folder_to_delete = new File("TEMP");
		if(temp_folder_to_delete.exists()) {
			FileUtils.cleanDirectory(temp_folder_to_delete);
			FileUtils.deleteDirectory(temp_folder_to_delete);
//			temp_folder_to_delete.delete();
			// DEBUG
			if(!temp_folder_to_delete.exists())
				System.out.println("Folder " + temp_folder_to_delete.getName()+ " deleted.");
		}
		
		File output_folder_to_delete = new File("output");
		if(output_folder_to_delete.exists()) {
			FileUtils.cleanDirectory(output_folder_to_delete);
			FileUtils.deleteDirectory(output_folder_to_delete);
//			output_folder_to_delete.delete();
			// DEBUG
			if (!output_folder_to_delete.exists())
				System.out.println("Folder " + output_folder_to_delete.getName()+ " deleted.");
		}
		// Init
		Configuration conf = new Configuration();
		Path temp_folder = new Path("TEMP");
		
		// Map Reduce 1
		Job job1 = Job.getInstance(conf,"MapReduce1");
		job1.setJarByClass(AssignOne.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MovieAndRatingWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(MovieAndRatingArrayWritable.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class); //Enable when need to pass object to the 2nd MapReduce, else print to file
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(temp_folder.toString()));
		
//		FileSystem hdfs = FileSystem.get(conf);
//		if(hdfs.exists(out)) {
//			hdfs.delete(out,true);
//		}
		if(!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		// Map Reduce 2
		Job job2 = Job.getInstance(conf,"MapReduce2");
		job2.setJarByClass(AssignOne.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setMapOutputKeyClass(MoviePair.class);
		job2.setMapOutputValueClass(UserAndRatingWritable.class);
		job2.setOutputKeyClass(MoviePair.class);
		job2.setOutputValueClass(UserAndRatingArrayWritable.class);
		FileInputFormat.addInputPath(job2, new Path(temp_folder.toString()));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		if(!job2.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
