/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.hw1q1;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author akash
 */
public class Q1 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String nextLine;
            String[] lineArray;
            String[] friendList;

            nextLine = value.toString();
            //System.out.println(nextLine);
            lineArray = nextLine.split("\t");
            //System.out.println(lineArray[0]);
            if (lineArray.length == 1) {
            } else {
                //System.out.println(lineArray[0]);
                friendList = lineArray[1].split(",");
                //System.out.println("FRIENDS: "+lineArray[1]);
                int[] mutualFriend = new int[2];
                for (int len = 0; len < friendList.length; len++) {
                    mutualFriend[0] = Integer.parseInt(lineArray[0]);
                    mutualFriend[1] = Integer.parseInt(friendList[len]);
                    Arrays.sort(mutualFriend);
                    context.write(new Text(mutualFriend[0] + "," + mutualFriend[1]), new Text(lineArray[1]));
                }
            }
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        /**
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         * (0,4), (20, 22939), (1, 29826), (6222, 19272), (28041, 28056)
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text[] texts = new Text[2];
            String[] Key = key.toString().split(",");
            if((Key[0].equals("0")&&Key[1].equals("4")) || (Key[0].equals("20")&&Key[1].equals("22939")) || (Key[0].equals("1")&&Key[1].equals("29826"))   ||  (Key[0].equals("6222")&&Key[1].equals("19272")) || (Key[0].equals("28041")&&Key[1].equals("28056")) ){
                
            
            int index = 0;
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                texts[index++] = new Text(iterator.next());
            }
            String[] friendListA = texts[0].toString().split(",");
            String[] friendListB = texts[1].toString().split(",");
            List<String> list = new LinkedList<>();
            for (String friend1 : friendListA) {
                for (String friend2 : friendListB) {
                    if (friend1.equals(friend2)) {
                        list.add(friend1);
                    }
                }
            }

            String commonList = "";
            for (int i = 0; i < list.size(); i++) {
                if (i == list.size() - 1) {
                    commonList += list.get(i);
                } else {
                    commonList +=list.get(i)+ ",";
                }
            }
            context.write(key, new Text(commonList));
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        //String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args

        // create a job with name "mutualFriends"
        Job job = Job.getInstance(conf, "mutualFriends");
        job.setJarByClass(Q1.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
