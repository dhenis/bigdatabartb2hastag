// deni setiawan msc software engineering
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.util.*;
import org.apache.hadoop.io.NullWritable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    //1469453965000;757570957502394369;Over 30 million women footballers in the world. Most of us would trade places with this lot for #Rio2016  https://t.co/Mu5miVJAWx;<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      try {


      //          String clean =  value.toString().replaceAll("; ", ""); // cleaning string(data) from "; " ==> because semicolon is used for split

                String[] itr = value.toString().split(";"); // exploed and parsed to array and data type is string fix


                if(itr.length >= 4){ // manage if failed to split


                //long epoch = 1469453965;
                long epoch = Long.parseLong(itr[0]);

                Date date = new Date(epoch);

                SimpleDateFormat time = new SimpleDateFormat("HH");

                time.setTimeZone(TimeZone.getTimeZone("GMT-3")); // rio de janeiro time summer time

                String timeFix = time.format(date);


                if(Integer.parseInt(timeFix) == 22 ){

                         boolean p =itr[2].contains("#"); // filter if there is a hastag or not

                         if(p){ // check availability of hastag

                            String[] hastag = itr[2].toString().split("#"); // count how many hastag available


                            for(int i =1; i<=hastag.length-1;i++){ // -1 because iwill remove [0]

                               String[] getHastag = hastag[i].toString().split(" "); // split again to separate 1st string

                                if(getHastag.length >= 1){ // make sure that the sting is not empty
                                  data.set(getHastag[0].toString()); // only take 1st string

                                  context.write(data, one);


                                }


                              }

                            }

                   }

                }

      } catch (NumberFormatException e) {
          System.err.println("NumberFormatException: " + e.getMessage());
      }//end catch

    }
}
