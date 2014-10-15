package com.hackecho.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text attValue = new Text();
    private int i;
    private String token;
    public static int no_Attr;

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        Split split = null;
        int size_split = 0;
        split = Main.currentsplit;

        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);
        int index = 0;
        String attr_value = null;
        no_Attr = itr.countTokens() - 1;
        String attr[] = new String[no_Attr];
        boolean match = true;
        for (i = 0; i < no_Attr; i++) {
            attr[i] = itr.nextToken();
        }
        String classLabel = itr.nextToken();
        size_split = split.attr_index.size();
        for (int count = 0; count < size_split; count++) {
            index = (Integer) split.attr_index.get(count);
            attr_value = (String) split.attr_value.get(count);
            if (!attr[index].equals(attr_value)) {
                match = false;
                break;
            }
        }

        // id.attr_count=new int[no_Attr];
        if (match) {
            for (int l = 0; l < no_Attr; l++) {
                if (!split.attr_index.contains(l)) {
                    token = l + " " + attr[l] + " " + classLabel;
                    attValue.set(token);
                    output.collect(attValue, one);
                }
            }
            if (size_split == no_Attr) {
                token = no_Attr + " " + "null" + " " + classLabel;
                attValue.set(token);
                output.collect(attValue, one);
            }
        }
    }
}
