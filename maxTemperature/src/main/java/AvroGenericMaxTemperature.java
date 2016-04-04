import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.*;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/4.
 */
public class AvroGenericMaxTemperature extends Configured implements Tool {

     private static final Schema SCHEMA = new Schema.Parser().parse(
        "{"+
             "\"type\":\"record\"," +
             "\"name\":\"WeatherRecord\","+
             "\"doc\":\"A weather reading.\","+
             "\"fields\":["+
                "{\"name\":\"year\",\"type\":\"int\"},"+
                "{\"name\":\"temperature\",\"type\":\"int\"},"+
                "{\"name\":\"stationId\",\"type\":\"string\"},"+
               "]"+
        "}"
     );


    public static class MaxTemperatureMapper
            extends AvroMapper<Utf8,Pair<Integer,GenericRecord>>{

        private NcdcRecordParser parser = new NcdcRecordParser();

        private GenericRecord record = new GenericData.Record(SCHEMA);

        @Override
        public void map(Utf8 datum, AvroCollector<Pair<Integer, GenericRecord>> collector, Reporter reporter) throws IOException {

            parser.parse(datum.toString());
            if (parser.isValidTemperature()){
                record.put("year",parser.getYear());
                record.put("temperature",parser.getAirTemperature());
                record.put("stationId","");
                collector.collect(new Pair<Integer, GenericRecord>(parser.getYear(),record));
            }

        }
    }

    public static class MaxTemperatureReducer extends AvroReducer<Integer,GenericRecord,GenericRecord>{


        @Override
        public void reduce(Integer key, Iterable<GenericRecord> values, AvroCollector<GenericRecord> collector, Reporter reporter) throws IOException {
            GenericRecord max = null;
            for(GenericRecord value:values){
                if (max == null ||
                        (Integer)value.get("temperature")>(Integer) max.get("temperature")){
                    max = newWeatherRecord(value);
                }
            }
            collector.collect(max);
        }

        private GenericRecord newWeatherRecord(GenericRecord value){
            GenericRecord record = new GenericData.Record(SCHEMA);
            record.put("year",value.get("year"));
            record.put("temperature",value.get("temperature"));
            record.put("stationId",value.get("stationId"));
            return record;
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2){
            System.err.printf("Uage:%s [options] <input> <output>\n",getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        JobConf conf = new JobConf(getConf(),getClass());
        conf.setJobName("max temperature");

        FileInputFormat.addInputPath(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        AvroJob.setInputSchema(conf,Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputSchema(conf,Pair.getPairSchema(Schema.create(Schema.Type.INT),SCHEMA));
        AvroJob.setOutputSchema(conf,SCHEMA);

        conf.setInputFormat(AvroUtf8InputFormat.class);
        AvroJob.setMapperClass(conf,MaxTemperatureMapper.class);
        AvroJob.setReducerClass(conf,MaxTemperatureReducer.class);
        JobClient.runJob(conf);

        return 0;
    }

    public  static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(),args);
        System.exit(exitCode);
    }
}
