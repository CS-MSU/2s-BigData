package cs.msu;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Map;

import static cs.msu.SequenceFileUtils.toSequenceString;

/**
 * Фильтрует входные данные (пользователи): выбирает пользователей из России.
 */

public class RussianUsers extends Configured implements Tool {
    private static class FilterMapper extends Mapper<Object, Text, NullWritable, Text> {

        private final Text outValue = new Text();

        private final String[] fields = new String[] {
                "Id", "DisplayName", "Location"
        };

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            if (IsRussian(row)) {
                outValue.set(toSequenceString(row, fields));
                context.write(NullWritable.get(), outValue);
            }
        }

        private boolean IsRussian(Map<String, String> row) {
            if (StringUtils.isBlank(row.get("Location"))) return false;

            String location = row.get("Location").toLowerCase();

            if(location.contains("russia")) return true;
            if(location.contains("moscow")) return true;
            if(location.contains("novosibirsk")) return true;
            if(location.contains("saint_petersburg")) return true;
            if(location.contains("yekaterinburg")) return true;
            if(location.contains("kazan")) return true;
            if(location.contains("nizhny_novgorod")) return true;
            if(location.contains("chelyabinsk")) return true;
            if(location.contains("samara")) return true;
            if(location.contains("omsk")) return true;
            if(location.contains("rostov_on_don")) return true;
            if(location.contains("ufa")) return true;
            if(location.contains("krasnoyarsk")) return true;
            if(location.contains("voronezh")) return true;
            if(location.contains("perm")) return true;
            if(location.contains("volgograd")) return true;

            return false;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(getConf(), "Filter users by location Russia");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(RussianUsers.class);
        // Указываем класс Маппера
        job.setMapperClass(FilterMapper.class);
        // Тип ключа на выходе
        job.setOutputKeyClass(NullWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(Text.class);
        // Путь к файлу на вход
        FileInputFormat.addInputPath(job, new Path(inputPath));
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Включаем компрессию
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        job.setNumReduceTasks(0);


        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса
        return success ? 0 : 2;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int result = ToolRunner.run(new Configuration(), new RussianUsers(), args);

        System.exit(result);
    }

}