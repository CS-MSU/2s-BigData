package cs.msu;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import static cs.msu.SequenceFileUtils.toSequenceString;
import static cs.msu.SequenceFileUtils.fromSequenceString;


/**
 * Соединяет вопросы и ответы по полю Id и ParentId
 */

public class QuestionsAnswers extends Configured implements Tool {
    private static class QuestionMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();

        private final String[] input_fields = new String[] {
                "Id", "Tags"
        };
        private final String[] output_fields = new String[] {
                "Id", "Type", "OwnerUserId", "Score"
        };

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = fromSequenceString(value, input_fields);

            row.put("Type", "question");
            outValue.set(toSequenceString(row, output_fields));
            outKey.set(Long.parseLong(row.get("Id")));
            context.write(outKey, outValue);
        }
    }
    private static class AnswerMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final LongWritable outKey = new LongWritable();
        private final Text outValue = new Text();

        private final String[] output_fields = new String[] {
                "Id", "Type", "OwnerUserId", "Score"
        };

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            row.put("Type", "answer");
            outValue.set(toSequenceString(row, output_fields));

            if (StringUtils.isBlank(row.get("ParentId"))) return;
            outKey.set(Long.parseLong(row.get("ParentId")));
            context.write(outKey, outValue);
        }
    }

    static class JoinReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        private final String[] input_fields = new String[] {
                "Id", "Type", "OwnerUserId", "Score"
        };
        private final String[] output_fields = new String[] {
                "OwnerUserId", "Score"
        };
        private final Text outValue = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean hasQuestion = false;
            List<String> answers = new ArrayList<>();

            // Распределим значения по типам строк в соотв. списки
            for (Text value : values) {
                Map<String, String> row = fromSequenceString(value, input_fields);
                if(row.get("Type").equals("question")) {
                    hasQuestion = true;
                } else {
                    answers.add(toSequenceString(row, output_fields));
                }
            }

            if (hasQuestion) {
                // Выполним Join
                for (String ans : answers) {
                    outValue.set(ans);
                    context.write(NullWritable.get(), outValue);
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Path questionPath = new Path(args[0]);
        Path postsPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        // Создаем новую задачу (Job), указывая ее название
        Job job = Job.getInstance(getConf(), "Questions and Answers join");
        // Указываем архив с задачей по имени класса в этом архиве
        job.setJarByClass(QuestionsAnswers.class);
        // Указываем класс Редьюсера
        job.setReducerClass(JoinReducer.class);
        // Кол-во тасков
        job.setNumReduceTasks(10);
        // Тип ключа на выходе
        job.setOutputKeyClass(NullWritable.class);
        // Тип значения на выходе
        job.setOutputValueClass(Text.class);
        // Пути к входным файлам, формат файла и мэппер
        MultipleInputs.addInputPath(job, questionPath, SequenceFileInputFormat.class, QuestionMapper.class);
        MultipleInputs.addInputPath(job, postsPath, TextInputFormat.class, AnswerMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        // Путь к файлу на выход (куда запишутся результаты)
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // Включаем компрессию
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        // Запускаем джобу и ждем окончания ее выполнения
        boolean success = job.waitForCompletion(true);
        // Возвращаем ее статус в виде exit-кода процесса
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int result = ToolRunner.run(new Configuration(), new QuestionsAnswers(), args);

        System.exit(result);
    }

}