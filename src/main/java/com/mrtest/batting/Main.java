/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.mrtest.batting;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 *
 * @author bolivar
 */
public class Main {
    public static class BattingMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        public void map(LongWritable k1, Text v1, OutputCollector oc, Reporter rprtr) throws IOException {
            String line = v1.toString();
            String [] pieces = line.split(",");
            //Define objects to be used
            //Validate input. If it doesn't exist, ignore record.
            if(pieces[0] == null)
                return;
            if("".equals(pieces[0]))
                return;
            String playerID = pieces[0];
            
            if(pieces[1] == null)
                return;
            if("".equals(pieces[1]))
                return;
            String year = pieces[1];
            
            if(pieces[8]==null)
                return;
            if("".equals(pieces[8]))
                return;
            LongWritable runs = new LongWritable(Long.parseLong(pieces[8]));
            
            String key = playerID + "|" + year;
            oc.collect(new Text(key), runs);
        }
    }
    
    public static class BattingReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text k2, Iterator<LongWritable> itrtr, OutputCollector<Text, LongWritable> oc, Reporter rprtr) throws IOException {
            Long totalRuns = (long) 0;
            while(itrtr.hasNext()){
                totalRuns+=itrtr.next().get();
            }
            oc.collect(k2, new LongWritable(totalRuns));
        }

    }
    
    public static void main(String[] args) throws Exception{
        JobConf conf = new JobConf();
        conf.setJobName("Runs count per Player.");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);
        
        conf.setMapperClass(BattingMapper.class);
        conf.setCombinerClass(BattingReducer.class);
        conf.setReducerClass(BattingReducer.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setJarByClass(Main.class);
        JobClient.runJob(conf);
    }
}
