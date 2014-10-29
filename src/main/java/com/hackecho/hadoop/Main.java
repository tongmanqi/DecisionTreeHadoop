package com.hackecho.hadoop;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    public static Split currentsplit = new Split();
    public static List<Split> splitted = new ArrayList<Split>();;
    public static int current_index = 0;
    public static ArrayList<String> ar = new ArrayList<String>();

    public static void main(String[] args) throws Exception {
        splitted.add(currentsplit);
        Path c45 = new Path("C45");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(c45)) {
            fs.delete(c45, true);
        }
        fs.mkdirs(c45);

        int res = 0;
        int split_index = 0;
        double gainratio = 0;
        double best_gainratio = 0;
        double entropy = 0;
        String classLabel = null;
        int total_attributes = MapClass.no_Attr;
        total_attributes = 4;
        int split_size = splitted.size();
        GainRatio gainObj;
        Split newnode;

        while (split_size > current_index) {
            currentsplit = (Split) splitted.get(current_index);
            gainObj = new GainRatio();
            res = ToolRunner.run(new Configuration(), new Main(), args);
            System.out.println("Current  NODE INDEX . ::" + current_index);
            int j = 0;
            int temp_size;
            gainObj.getcount();
            entropy = gainObj.currNodeEntophy();
            classLabel = gainObj.majorityLabel();
            currentsplit.classLabel = classLabel;

            if (entropy != 0.0 && currentsplit.attr_index.size() != total_attributes) {
                System.out.println("");
                System.out.println("Entropy  NOTT zero   SPLIT INDEX::    " + entropy);
                best_gainratio = 0;
                for (j = 0; j < total_attributes; j++) // Finding the gain of
                                                       // each attribute
                {
                    if (!currentsplit.attr_index.contains(j)) {
                        gainratio = gainObj.gainratio(j, entropy);
                        if (gainratio >= best_gainratio) {
                            split_index = j;
                            best_gainratio = gainratio;
                        }
                    }
                }

                String attr_values_split = gainObj.getvalues(split_index);
                StringTokenizer attrs = new StringTokenizer(attr_values_split);
                int number_splits = attrs.countTokens(); // number of splits
                                                         // possible with
                                                         // attribute selected
                String red = "";
                System.out.println(" INDEX ::  " + split_index);
                System.out.println(" SPLITTING VALUES  " + attr_values_split);

                for (int splitnumber = 1; splitnumber <= number_splits; splitnumber++) {
                    temp_size = currentsplit.attr_index.size();
                    newnode = new Split();
                    for (int y = 0; y < temp_size; y++) {
                        newnode.attr_index.add(currentsplit.attr_index.get(y));
                        newnode.attr_value.add(currentsplit.attr_value.get(y));
                    }
                    red = attrs.nextToken();

                    newnode.attr_index.add(split_index);
                    newnode.attr_value.add(red);
                    splitted.add(newnode);
                }
            } else {
                String rule = "";
                temp_size = currentsplit.attr_index.size();
                for (int val = 0; val < temp_size; val++) {
                    rule = rule + " " + currentsplit.attr_index.get(val) + " " + currentsplit.attr_value.get(val);
                }
                rule = rule + " " + currentsplit.classLabel;
                ar.add(rule);
                writeRuleToFile(ar);
                ar.add("\n");
                if (entropy != 0.0) {
                    System.out.println("Enter rule in file:: " + rule);
                } else {
                    System.out.println("Enter rule in file Entropy zero ::   " + rule);
                }
            }
            split_size = splitted.size();
            System.out.println("TOTAL NODES::    " + split_size);
            current_index++;
        }
        System.out.println("Done!");
        System.exit(res);
    }

    public static void writeRuleToFile(ArrayList<String> text) throws IOException {
        Path rule = new Path("C45/rule.txt");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(rule, true)));
            for (String str : text) {
                bw.write(str);
            }
            bw.newLine();
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int run(String[] args) throws Exception {
        System.out.println("In main ---- run");
        JobConf conf = new JobConf(getConf(), Main.class);
        conf.setJobName("C45");
        conf.set("currentIndex", String.valueOf(current_index));

        // the keys are words (strings)
        conf.setOutputKeyClass(Text.class);
        // the values are counts (ints)
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(MapClass.class);
        conf.setReducerClass(Reduce.class);
        System.out.println("back to run");

        FileSystem fs = FileSystem.get(conf);

        Path out = new Path(args[1] + current_index);
        if (fs.exists(out)) {
            fs.delete(out, true);
        }

        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, out);

        JobClient.runJob(conf);
        return 0;
    }

}