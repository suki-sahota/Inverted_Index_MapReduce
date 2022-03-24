import java.io.IOException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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


public class InvertedIndexJob {

    // Mapper Class
    public static class InvertedIndexJobMapper 
        extends Mapper<LongWritable, Text, Text, Text> 
    {
        private String docID = null;
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            if (tokenizer.hasMoreTokens()) {
                docID = tokenizer.nextToken();
            }

            String prev_word = null;

            while (tokenizer.hasMoreTokens()) {
                String tempWord = tokenizer.nextToken();

                // Make lower case
                tempWord = tempWord.toLowerCase();

                int aChar = 97;
                int zChar = 122;
                Character spaceChar = ' ';

                // Remove leading special characters
                while (tempWord.length() > 0
                       && (tempWord.charAt(0) < aChar
                       || tempWord.charAt(0) > zChar))
                {
                    tempWord = tempWord.substring(1);
                }

                // Remove trailing special characters
                while (tempWord.length() > 0
                       && (tempWord.charAt(tempWord.length() - 1) < aChar
                       || tempWord.charAt(tempWord.length() - 1) > zChar))
                {
                    tempWord = tempWord.substring(0, tempWord.length() - 1);
                }

                Text docIDAsWord = new Text();
                docIDAsWord.set(docID);

                int start = 0;
                for (int end = 0; end < tempWord.length(); ++end) {
                    Character myChar = tempWord.charAt(end);

                    if (myChar < aChar || myChar > zChar) {
                        // Replace middle special characters with spaces
                        tempWord = tempWord.substring(0, end)
                            + spaceChar
                            + tempWord.substring(end + 1);

                        int lengthOfWord = end - start;
                        if (end != start) {
                            // Find substring that becomes it's own word
                            String substr = tempWord.substring(start, end);
                            word.set(substr);

                            if (prev_word != null) {
                                Text bigram = new Text(prev_word 
                                    + " " 
                                    + word.toString());
                                // Write new word to context
                                context.write(bigram, docIDAsWord);
                            }
                            prev_word = word.toString();
                        }

                        start = end + 1;
                    }
                }

                if (tempWord.length() > 0
                    && tempWord.charAt(tempWord.length() - 1) != spaceChar)
                {
                    String substr = tempWord.substring(start);
                    word.set(substr);
                    Text bigram = new Text(prev_word + " " + word.toString());

                    // Write new word to context
                    context.write(bigram, docIDAsWord);
                    prev_word = word.toString();
                }
            }
        }
    } // End of Mapper class

    // Reducer class
    public static class InvertedIndexJobReducer 
        extends Reducer<Text, Text, Text, Text> 
    {
        public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException 
        {
            Map<String, Integer> countOfDocID
                = new HashMap<String, Integer>();

            for (Text value : values) {
                // Aggregates the word counts by Doc ID
                if (value.toString().length() > 0) {
                    countOfDocID.put(
                        value.toString(), 
                        countOfDocID.getOrDefault(value.toString(), 0) + 1
                    );
                }
            }

            String inverted_index = "";
            for (Map.Entry<String, Integer> entry : countOfDocID.entrySet()) {
                // Creates Inverted Index
                inverted_index += entry.getKey();
                inverted_index += ":";
                inverted_index += entry.getValue().toString();
                inverted_index += " ";
            }

            context.write(key, new Text(inverted_index));
        }
    } // End of Reducer class

    // Main method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println(
                "Usage: inverted index <input path> <output path>"
            );
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndexJob.class);

        job.setMapperClass(InvertedIndexJobMapper.class);
        job.setReducerClass(InvertedIndexJobReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}


