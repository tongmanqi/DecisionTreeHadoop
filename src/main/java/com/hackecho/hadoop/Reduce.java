package com.hackecho.hadoop;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    static int cnt = 0;
    ArrayList<String> ar = new ArrayList<String>();
    String data = null;
    private static int currentIndex;

    public void configure(JobConf conf) {
        currentIndex = Integer.valueOf(conf.get("currentIndex"));
    }

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
            Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));

        String data = key + " " + sum;
        ar.add(data);
        writeToFile(ar);
        ar.add("\n");
    }

    public static void writeToFile(ArrayList<String> text) {
        try {
            cnt++;
            System.out.println("count " + cnt);
            Path input = new Path("C45/intermediate" + currentIndex + ".txt");
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(input, true)));

            for (String str : text) {
                bw.write(str);
            }

            bw.newLine();
            bw.close();
        } catch (Exception e) {
            System.out.println("File is not creating in reduce");
        }
    }

}