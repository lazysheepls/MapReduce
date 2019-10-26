import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class AssignOnez3451444 {
	/**
	 * UserMapper: Maps the input file to key-value pairs
	 * 1. Extract info from the file, only interested in user_id, movie_id and rating
	 * 2. Create key from user_id
	 * 3. Create value of type MovieAndRatingWritable (custom)
	 * 4. Add to the output
	 * e.g. Input U2::M3::1::11111111 | Output key - U2, value - (M3,1)
	 */
	public static class UserMapper extends Mapper<LongWritable,Text,Text,MovieAndRatingWritable>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovieAndRatingWritable>.Context context)
				throws IOException, InterruptedException {
			// Process input (0:user_id,1:movie_id,2:rating,3:timestamp)
			String[] inputs = value.toString().split("::");
			
			// Key: user_id
			Text user_id = new Text(inputs[0]);
			
			// Value: movie and rating (store into custom writable)
			Text movie_id = new Text(inputs[1]);
			IntWritable rating = new IntWritable(Integer.parseInt(inputs[2]));
			MovieAndRatingWritable movieAndRatingWritable = new MovieAndRatingWritable(movie_id,rating);
			
			context.write(user_id, movieAndRatingWritable);
		}	
	}
	
	/**
	 * UserReducer: Find movies watched by the same user
	 * 1. Make a deep copy of the iterable values input to an intermediate array list
	 * 2. Convert the intermediate array list to an array
	 * 3. Use the generated array to create the MovieAndRatingArrayWritable(custom)
	 * 4. Add to the output
	 * e.g. Input - U2,(M3,1) and U2,(M4,3), output key - U2, value - [(M3,1),(M4,3)]
	 */
	public static class UserReducer extends Reducer<Text, MovieAndRatingWritable, Text, MovieAndRatingArrayWritable>{

		@Override
		protected void reduce(Text key, Iterable<MovieAndRatingWritable> values,
				Reducer<Text, MovieAndRatingWritable, Text, MovieAndRatingArrayWritable>.Context context)
				throws IOException, InterruptedException {
			// Deep copy of the iterable values to an intermediate array list
			ArrayList<MovieAndRatingWritable> tempArrayList = new ArrayList<MovieAndRatingWritable>();
			for(MovieAndRatingWritable value : values) {
				Text movie_id = new Text(value.getMovieId());
				IntWritable rating = new IntWritable(value.getRating().get());
				tempArrayList.add(new MovieAndRatingWritable(movie_id,rating));
			}
			
			// Convert array list to array
			Object[] tempObjectArray = tempArrayList.toArray();
			MovieAndRatingWritable[] movieAndRatingArray 
			= Arrays.copyOf(tempObjectArray, tempObjectArray.length, MovieAndRatingWritable[].class);
			
			// Create MovieAndRatingArrayWritable(custom) from the array generated before
			MovieAndRatingArrayWritable movieAndRatingArrayWritable 
			= new MovieAndRatingArrayWritable(movieAndRatingArray);
			
			context.write(key, movieAndRatingArrayWritable);
		}
	}
	
	/**
	 * MoviePairMapper: Find movie pairs and their ratings
	 * 1. Deep copy movieAndRatingArrayWritable into intermediate array list
	 * 2. Sort the array list by movie id to make sure movie pairs are created in the order
	 * 3. Iterate through the array list to find valid movie pairs
	 * 4. Create key - MoviePair(custom), value - UserAndRatingWritable(custom)
	 * 5. Add to the output
	 * e.g. Input U2,(M1,3),(M3,4) Output key - (M1,M3), value - (U2,3,4)
	 */
	public static class MoviePairMapper extends Mapper<Text, MovieAndRatingArrayWritable, MoviePair, UserAndRatingWritable>{
		@Override
		protected void map(Text key, MovieAndRatingArrayWritable values,
				Mapper<Text, MovieAndRatingArrayWritable, MoviePair, UserAndRatingWritable>.Context context)
				throws IOException, InterruptedException {
			
			// Copy movie and rating array writable to an intermediate arrayList
			ArrayList<MovieAndRatingWritable> movieAndRatingArrayList = new ArrayList<MovieAndRatingWritable>();
			MovieAndRatingWritable[] tempArray = values.get();
			
			for(int i=0; i<tempArray.length;i++) {
				Text movie_id = new Text(tempArray[i].getMovieId());
				IntWritable rating = new IntWritable(tempArray[i].getRating().get());
				MovieAndRatingWritable movieAndRatingWritable = new MovieAndRatingWritable(movie_id,rating);
				movieAndRatingArrayList.add(movieAndRatingWritable);
			}

			// Sort movie_id to avoid redundant movie pairs (e.g (M1,M2) and (M2,M1) should be the same)
			MovieIdComparator movieIdComparator = new MovieIdComparator();
			Collections.sort(movieAndRatingArrayList, movieIdComparator);

			// Find movie pairs
			int size = movieAndRatingArrayList.size();
			for (int i=0;i<size-1;i++) {
				for(int j=i+1;j<size;j++) {
					// Create key - MoviePair
					Text movie_id_1 = new Text(movieAndRatingArrayList.get(i).getMovieId());
					Text movie_id_2 = new Text(movieAndRatingArrayList.get(j).getMovieId());
					if (movie_id_1.equals(movie_id_2)) break; // ignore movie pairs that has the same id
					MoviePair moviePair = new MoviePair(movie_id_1, movie_id_2);
					
					// Create value - UserAndRatingWritable(custom)
					Text user_id = new Text(key);
					IntWritable rating_1 = new IntWritable(movieAndRatingArrayList.get(i).getRating().get());
					IntWritable rating_2 = new IntWritable(movieAndRatingArrayList.get(j).getRating().get());
					UserAndRatingWritable userAndRatingWritable = new UserAndRatingWritable(user_id,rating_1,rating_2);
					
					context.write(moviePair, userAndRatingWritable);
				}
			}
		}
	}
	
	/**
	 * MoviePairReducer: Find movies watched by the same user
	 * 1. Make a deep copy of the iterable values input to an intermediate array list
	 * 2. Convert the intermediate array list to an array
	 * 3. Use the generated array to create the UserAndRatingArrayWritable(custom)
	 * 4. Add to the output
	 * e.g. Input - (M2,M3),(U1,1,2) and (M2,M3),(U2,3,4), output key - (M2,M3), value - [(U1,1,2),(U2,3,4)]
	 */
	public static class MoviePairReducer extends Reducer<MoviePair, UserAndRatingWritable, MoviePair, UserAndRatingArrayWritable>{

		@Override
		protected void reduce(MoviePair key, Iterable<UserAndRatingWritable> values,
				Reducer<MoviePair, UserAndRatingWritable, MoviePair, UserAndRatingArrayWritable>.Context context)
				throws IOException, InterruptedException {
			
			// Deep copy the iteratable values into an intermediate array list
			ArrayList<UserAndRatingWritable> tempArrayList = new ArrayList<UserAndRatingWritable>();
			for(UserAndRatingWritable value:values) {
				Text user_id = new Text(value.getUserId());
				IntWritable rating_1 = new IntWritable(value.getRating1().get());
				IntWritable rating_2 = new IntWritable(value.getRating2().get());
				tempArrayList.add(new UserAndRatingWritable(user_id, rating_1, rating_2));
			}
			
			// Convert array list to array
			Object[] tempObjectArray = tempArrayList.toArray();
			UserAndRatingWritable[] tempUserAndRatingArray
			= Arrays.copyOf(tempObjectArray, tempObjectArray.length, UserAndRatingWritable[].class);
			
			// Create UserAndRatingArrayWritable(custom) from the array generated before
			UserAndRatingArrayWritable userAndRatingArrayWritable
			= new UserAndRatingArrayWritable(tempUserAndRatingArray);
			
			context.write(key, userAndRatingArrayWritable);
		}
	}
	
	/**
	 * MovieAndRatingWritable - used by UserMapper/Reducer
	 * A tuple contains movie id and rating
	 */
	public static class MovieAndRatingWritable implements Writable{
		
		private Text _movie_id;
		private IntWritable _rating;
		
		// Constructor
		public MovieAndRatingWritable() {
			_movie_id = new Text();
			_rating = new IntWritable();
		}
		
		// Explicit Constructor
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
			return _movie_id.toString() + "," + _rating.toString();
		}
	}
	
	/**
	 * MovieAndRatingArrayWritable - used by UserReducer and MoviePairMapper
	 * An arrayWritable of the movie and rating tuples
	 */
	public static class MovieAndRatingArrayWritable extends ArrayWritable{

		// Constructor
		public MovieAndRatingArrayWritable() {
			super(MovieAndRatingWritable.class);
		}
		
		// Explicit Constructor
		public MovieAndRatingArrayWritable(MovieAndRatingWritable[] values) {
			super(MovieAndRatingWritable.class, values);
		}
		
		// Getter
		@Override
		public MovieAndRatingWritable[] get() {
			Writable[] writableArray = super.get();
			MovieAndRatingWritable[] movieAndRatingWritableArray = new MovieAndRatingWritable[writableArray.length];
			for(int i=0;i<writableArray.length;i++) {
				movieAndRatingWritableArray[i] = (MovieAndRatingWritable)writableArray[i];
			}
			return movieAndRatingWritableArray;
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
	
	/**
	 * MovieIdComparator - used by MoviePairMapper
	 * Compare two movieAndRatingWritable by comparing their movie ids
	 */
	public static class MovieIdComparator implements Comparator<MovieAndRatingWritable>{
		@Override
		public int compare(MovieAndRatingWritable in1, MovieAndRatingWritable in2) {
			Text movie_id_1 = new Text(in1.getMovieId());
			Text movie_id_2 = new Text(in2.getMovieId());
			
			return movie_id_1.compareTo(movie_id_2);
		}
	}
	
	/**
	 * MoviePair - used by MoviePairMapper/Reducer
	 * A tuple contains two movie ids, used as the key
	 */
	@SuppressWarnings("rawtypes")
	public static class MoviePair implements WritableComparable{
		
		private Text _movie_id_1;
		private Text _movie_id_2;
		
		// Constructor
		public MoviePair() {
			_movie_id_1 = new Text();
			_movie_id_2 = new Text();
		}
		
		// Explicit Constructor
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
			int result = 0;
			
			// movie1 is different, use movie1 to compare
			if (!movieId1.equals(_movie_id_1)) result = movieId1.compareTo(_movie_id_1);
			// movie1 is the same, movie2 is different
			else if (!movieId2.equals(_movie_id_2)) result = movieId2.compareTo(_movie_id_2);
			// Both pair are identical, still use movie1 to compare
			else result = movieId1.compareTo(_movie_id_1);
			
			return result;
		}

		@Override
		public String toString() {
			return "(" + _movie_id_1.toString() + "," + _movie_id_2.toString() + ")";
		}
	}
	
	/**
	 * UserAndRatingWritable - used by UserMapper/Reducer
	 * A triplet consists of user id, rating of movie 1 and rating of movie 2
	 */
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
		
		// Explicit Constructor
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
	
	/**
	 * UserAndRatingArrayWritable - used by UserReducer
	 * An arrayWritable formed by user_id, rating1, rating2 triplet
	 */
	public static class UserAndRatingArrayWritable extends ArrayWritable{
		
		// Constructor
		public UserAndRatingArrayWritable() {
			super(UserAndRatingWritable.class);
		}
		
		// Explicit Constructor
		public UserAndRatingArrayWritable(UserAndRatingWritable[] values) {
			super(UserAndRatingWritable.class, values);
		}
		
		// Getter
		@Override
		public UserAndRatingWritable[] get() {
			Writable[] writableArray = super.get();
			UserAndRatingWritable[] userAndRatingWritableArray = new UserAndRatingWritable[writableArray.length];
			for(int i=0; i<writableArray.length;i++) {
				userAndRatingWritableArray[i] = (UserAndRatingWritable)writableArray[i];
			}
			return userAndRatingWritableArray;
		}

		@Override
		public String toString() {
			UserAndRatingWritable[] values = get();
			String string = "";
			for(int i = 0;i < values.length;i++) {
				if (i == 0) string += "[";
				string += values[i].toString();
				if (i == values.length - 1) {
					string += "]";
				}else {
					string += ",";
				}
			}
			return string;
		}
	}
	
	public static void main(String[] args) throws Exception {
		// Delete old folders if exist
		File temp_folder_to_delete = new File("TEMP");
		if(temp_folder_to_delete.exists()) {
			FileUtils.cleanDirectory(temp_folder_to_delete);
			FileUtils.deleteDirectory(temp_folder_to_delete);
		}
		
		File output_folder_to_delete = new File("output");
		if(output_folder_to_delete.exists()) {
			FileUtils.cleanDirectory(output_folder_to_delete);
			FileUtils.deleteDirectory(output_folder_to_delete);
		}
		
		// Initialise
		Configuration conf = new Configuration();
		Path temp_folder = new Path("TEMP"); // Store intermediate files to the "TEMP" folder
		
		// MapReduce - File to User Info
		Job job1 = Job.getInstance(conf,"UserMapReduce");
		job1.setJarByClass(AssignOnez3451444.class);
		job1.setMapperClass(UserMapper.class);
		job1.setReducerClass(UserReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(MovieAndRatingWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(MovieAndRatingArrayWritable.class);
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(temp_folder.toString()));
		
		if(!job1.waitForCompletion(true)) {
			System.exit(1);
		}
		
		// Map Reduce - User Info to Movie Pairs
		Job job2 = Job.getInstance(conf,"MapReduceMoviePair");
		job2.setJarByClass(AssignOnez3451444.class);
		job2.setMapperClass(MoviePairMapper.class);
		job2.setReducerClass(MoviePairReducer.class);
		job2.setMapOutputKeyClass(MoviePair.class);
		job2.setMapOutputValueClass(UserAndRatingWritable.class);
		job2.setOutputKeyClass(MoviePair.class);
		job2.setOutputValueClass(UserAndRatingArrayWritable.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(temp_folder.toString()));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		if(!job2.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}
