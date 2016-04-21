import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Mapper;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SortMoviesByUser {
    
    private static final String DEBUG_PATH = "/fslhome/misaie/Logs";
    private static final Boolean DEBUG_MODE = false;
    
    public static void main(String[] args) {
        try {
            // initializing paths
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);
            
            // clearing out output files because hdfs cannot re-write
            FileSystem fileSystem = FileSystem.get(new Configuration());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            
            if (DEBUG_MODE) {
                Path debugPath = new Path(DEBUG_PATH);
                if (fileSystem.exists(debugPath)) {
                    fileSystem.delete(debugPath, true);
                }
                fileSystem.mkdirs(debugPath);
            }
            
            // starting map reduce
            JobConf jobConfiguration = new JobConf(SortMoviesByUser.class);
            JobClient job = new JobClient();
            job.setConf(jobConfiguration);
            
            // specify input and output dirs
            FileInputFormat.addInputPath(jobConfiguration, inputPath);
            FileOutputFormat.setOutputPath(jobConfiguration, outputPath);

            // specify a mapper, combiner and reducer
            jobConfiguration.setMapperClass(SortMoviesByUser.Map.class);
            jobConfiguration.setCombinerClass(SortMoviesByUser.Combine.class);
            jobConfiguration.setReducerClass(SortMoviesByUser.Reduce.class);

            // specify output types
            jobConfiguration.setOutputKeyClass(Text.class);
            jobConfiguration.setOutputValueClass(Text.class);
        
        
            JobClient.runJob(jobConfiguration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static BufferedWriter getFileToAppend(){
        BufferedWriter bufferedWritter = null;
        try {
            
                
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return bufferedWritter;
    }

    // map(inKey, inValue) -> list(intermediateKey, intermediateValue)
    // inKey = fileIndex, inValue = lineOfText
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        
        private BufferedWriter writer;
        
        @Override
        public void configure(JobConf job) {
            super.configure(job);
            try {
                if (DEBUG_MODE) {
                    FileSystem fileSystem = FileSystem.get(new Configuration());
                    Random random = new Random();
                    Path debugFile = new Path(DEBUG_PATH + "/mapper_" + random.nextInt() + ".txt");
                    writer = new BufferedWriter( new OutputStreamWriter(fileSystem.create(debugFile)));
                    writer.write("MAPPERS\n\n");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            if (DEBUG_MODE) { 
                writer.write("MapperStart\n");
                writer.write("Input key " + key + "\n");
                writer.write("Input value " + value + "\n");
            }
            Text userText = new Text();
            Text movieAndStarsText = new Text();
            
            String line = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(line.toLowerCase());
            
           
            
            while(stringTokenizer.hasMoreTokens()) {
                String user = stringTokenizer.nextToken();
                String movie = stringTokenizer.nextToken();
                String stars = stringTokenizer.nextToken();
                String movieAndStars = movie + " " + stars;
                userText.set(user);
                movieAndStarsText.set(movieAndStars);
                
                output.collect(movieAndStarsText, userText);
                
                if (DEBUG_MODE) {
                    writer.write("Output key " + movieAndStarsText + "\n");
                    writer.write("Output value " + userText + "\n");
                }
            }
            
            if (DEBUG_MODE) {
                writer.write("MapperEnd\n\n");
            }

        }
        
        @Override
        public void close() throws IOException {
            if (DEBUG_MODE && writer != null) {
                writer.close();
            }
            super.close();
        }
    }
    
    
    // reduce(intermediateKey, list(intermediateValue)) -> list(outKey, outValue)
    public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        
        private BufferedWriter writer;
        
        public String alphabetOrder(String word1, String word2) throws IOException {
            String output = new String();
            int compare = word1.compareTo(word2);
            if (compare == -1) {
                output = word1 + " " + word2;
            } else if (compare == 1) {
                output = word2 + " " + word1;
            } else {
                output = null;
            }
            return output;
        }
        
        @Override
        public void configure(JobConf job) {
            super.configure(job);
            try {
                if (DEBUG_MODE) {
                    FileSystem fileSystem = FileSystem.get(new Configuration());
                    Random random = new Random();
                    Path debugFile = new Path(DEBUG_PATH + "/combiner_" + random.nextInt() + ".txt");
                    writer = new BufferedWriter( new OutputStreamWriter(fileSystem.create(debugFile)));
                    writer.write("COMBINERS\n\n");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        
        @Override
        public void reduce(Text key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
            if (DEBUG_MODE) {
                writer.write("CombinerStart\n");
                writer.write("Input key " + key +"\n");
            }
            
            ArrayList<Text> list = new ArrayList<Text>();
            while (values.hasNext()) {
                Text value = new Text((Text)values.next());
                list.add(value);
                if (DEBUG_MODE) {
                    writer.write("Input value " + value +"\n");
                }
            }
            
            for (int i = 0; i < list.size(); i++) {
                Text userText = list.get(i);
                for (int j = i+1; j < list.size(); j++) {
                    Text valueUserText = list.get(j);
                    String keyUser = alphabetOrder(userText.toString(), valueUserText.toString());
                    if (keyUser != null) {
                        Text keyText = new Text(keyUser);
                        Text valueText = new Text("1");
                        output.collect(keyText, valueText);
                        if (DEBUG_MODE) {
                            writer.write("Output key " + keyText + "\n");
                            writer.write("Output value " + valueText + "\n");
                        }
                    }
                }
            }
  
            if (DEBUG_MODE) {
                writer.write("CombinerEnd" + "\n\n");
            }
        }
        
        @Override
        public void close() throws IOException {
            if (DEBUG_MODE && writer != null) {
                writer.close();
            }
            super.close();
        }
        
    }
    
    
    // reduce(intermediateKey, list(intermediateValue)) -> list(outKey, outValue)
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        
        private BufferedWriter writer;
        
        @Override
        public void configure(JobConf job) {
            super.configure(job);
            try {
                if (DEBUG_MODE) {
                    FileSystem fileSystem = FileSystem.get(new Configuration());
                    Random random = new Random();
                    Path debugFile = new Path(DEBUG_PATH + "/reducer_" + random.nextInt() + ".txt");
                    writer = new BufferedWriter( new OutputStreamWriter(fileSystem.create(debugFile)));
                    writer.write("REDUCERS\n\n");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        
        @Override
        public void reduce(Text key, Iterator values, OutputCollector output, Reporter reporter) throws IOException {
            if (DEBUG_MODE) {
                writer.write("ReducerStart\n");
                writer.write("Input key " + key +"\n");
            }
            int counter = 0;
            while (values.hasNext()) {
                counter++;
                Text value = (Text)values.next();
                if (DEBUG_MODE) {
                    writer.write("Input value " + value + "\n");
                }
            }
            
            Text textCounter = new Text(Integer.toString(counter));
            output.collect(key, textCounter);
            
            if (DEBUG_MODE) {
                writer.write("Output key " + key + "\n");
                writer.write("Output value " + textCounter + "\n");
                writer.write("ReducerEnd" + "\n\n");
            }
        }
        
        @Override
        public void close() throws IOException {
            if (DEBUG_MODE && writer != null) {
                writer.close();
            }
            super.close();
        }
        
    }
    
    
}