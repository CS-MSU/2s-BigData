package cs.msu;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import java.util.Arrays;
import java.util.Map;

/**
 * Conversion from XML to Sequence file.
 */
public class ConvertToSequenceFile extends Configured implements Tool {

    private static class XmlSourceMapper extends Mapper<Object, Text, LongWritable, Text> {

        private LongWritable outKey = new LongWritable();
        private Text outValue = new Text();
        private StringBuilder buffer = new StringBuilder(250);
        private String[] fields;
        private String keyField = "Id";

        @Override
        protected void setup(Context context) {
            String newKeyField = context.getConfiguration().get("key");
            if (StringUtils.isNotBlank(newKeyField)) {
                keyField = newKeyField.trim();
            }
            System.out.println("Using key field: " + keyField);

            String commaSeparatedFields = context.getConfiguration().get("fields");
            fields = commaSeparatedFields.split(",");
            System.out.println("Loading fields: " + Arrays.toString(fields));
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            String keyValue = row.get(keyField);
            outKey.set(keyValue == null ? -1 : Long.parseLong(keyValue));

            buffer.setLength(0);
            for (String field : fields) {
                String fieldValue = row.get(field);
                if (fieldValue != null) {
                    buffer.append(fieldValue);
                }
                buffer.append('\01');
            }
            outValue.set(buffer.toString());
            context.write(outKey, outValue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Utility to convert StackOverflow XML to sequence file.\n" +
                    "Usage: hadoop job <jar name> " + ConvertToSequenceFile.class.getName() +
                    " <input path> <output path> <comma separated fields>");
            return 1;
        }
        String inputPath = args[0];
        String outputPath = args[1];
        String fields = args[2];

        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(getConf(), "Convert " + inputPath);
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(ConvertToSequenceFile.class);
        // Указываем класс Маппера
        job.setMapperClass(XmlSourceMapper.class);
        // Тип ключа на выходе
        job.setOutputKeyClass(LongWritable.class);
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

        // Передаем параметры в конфигурацию задачи
        job.getConfiguration().set("fields", fields);

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса
        return success ? 0 : 2;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int result = ToolRunner.run(new Configuration(), new ConvertToSequenceFile(), args);

        System.exit(result);
    }
}
