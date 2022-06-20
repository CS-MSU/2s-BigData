package cs.msu;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import java.io.Serializable;
import java.util.Map;

/**
 spark-submit
 --class ru.mai.dep806.bigdata.spark.RussiaHadoop
 --master yarn --deploy-mode cluster
 --driver-memory 512M
 --executor-memory 4096M
 spark.jar
 /user/stud/stackoverflow/landing/Posts
 /user/stud/stackoverflow/landing/Users
 /user/mirpulatov/spark
 */

public class HadoopRussia {
    public static void main(String[] args) {

        String postsPath = args[0];
        String usersPath = args[1];
        String outPath = args[2];


        // Создание спарк-конфигурации и контекста
        SparkConf sparkConf = new SparkConf()
                .setAppName("Top Russian Hadoop Users")
                .set("spark.sql.shuffle.partitions", "16");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);


        // Создаем RDD для постов
        JavaRDD<Post> posts = sc.textFile(postsPath)
                .map(line -> new Post(XmlUtils.parseXmlToMap(line)));


        // Создаем Dataset для юзеров
        JavaRDD<User> users =  sc.textFile(usersPath)
                .map(line -> new User(XmlUtils.parseXmlToMap(line)));


        // Создаем Dataset для вопросов
        Dataset<Row> questions = sqlContext.createDataFrame(
                posts.filter(q -> isQuestion(
                        q.getPostTypeId(),
                        q.getTags())
                ),
                Post.class
        );

        // Создаем Dataset для вопросов
        Dataset<Row> answers = sqlContext.createDataFrame(
                posts.filter(a -> isAnswer(
                        a.getOwnerUserId(),
                        a.getScore(),
                        a.getParentId())
                ),
                Post.class
        );

        // 3 Query
        Dataset<Row> russian_users = sqlContext.createDataFrame(
                users.filter(u -> isRussian(
                        u.getLocation())
                ),
                User.class
        );

        // 2 Query
        Dataset<Row> questions_answers = questions
                .join(answers, questions.col("Id")
                        .equalTo(answers.col("ParentId")))
                .select(
                        answers.col("OwnerUserId").alias("user_id"),
                        answers.col("Score").alias("score"),
                        questions.col("Tags").alias("tags")
                );

        // 4 Query
        questions_answers
                .join(
                        russian_users,
                        questions_answers.col("user_id")
                        .equalTo(russian_users.col("Id"))
                )
                .groupBy(
                        questions_answers.col("user_id"),
                        russian_users.col("DisplayName").alias("name"),
                        russian_users.col("Location").alias("location")
                )
                .agg(
                        sum(questions_answers.col("score")).as("sum_score")
                )
                .orderBy(
                        col("sum_score").desc()
                )
                .select(
                        "user_id",
                        "name",
                        "location",
                        "sum_score"
                )
                .limit(50)
                .coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .mode(SaveMode.Overwrite)
                .save(outPath);
    }

    public static class Post implements Serializable {
        private final String parentId;
        private final String postTypeId;
        private final String ownerUserId;
        private final String tags;
        private final String score;


        public Post(Map<String, String> row) {
            this.parentId = row.get("ParentId");
            this.postTypeId = row.get("PostTypeId");
            this.ownerUserId = row.get("OwnerUserId");
            this.tags = row.get("Tags");
            this.score = row.get("Score");
        }

        public String getParentId() {
            return parentId;
        }
        public String getTags() { return tags; }
        public String getPostTypeId() {
            return postTypeId;
        }
        public String getOwnerUserId() {
            return ownerUserId;
        }
        public String getScore() { return score; }
    }

    public static class User implements Serializable {
        private final String location;

        public User(Map<String, String> row) {
            this.location = row.get("Location");
        }

        public String getLocation() { return location; }
    }

    private static Boolean isAnswer(String owner_user_id, String score, String parent_id) {
        if (StringUtils.isBlank(parent_id)) return false;
        if (StringUtils.isBlank(owner_user_id)) return false;
        if (StringUtils.isBlank(score)) return false;

        return true;
    }

    private static Boolean isQuestion(String type, String tags) {
        if (StringUtils.isBlank(type)) return false;
        if (StringUtils.isBlank(tags)) return false;

        return tags.toLowerCase().contains("hadoop") && "1".equals(type);
    }

    private static Boolean isRussian(String location) {
        if (StringUtils.isBlank(location)) return false;

        if(StringUtils.containsIgnoreCase(location, "russia")) return true;
        if(StringUtils.containsIgnoreCase(location, "moscow")) return true;
        if(StringUtils.containsIgnoreCase(location, "novosibirsk")) return true;
        if(StringUtils.containsIgnoreCase(location, "saint_petersburg")) return true;
        if(StringUtils.containsIgnoreCase(location, "yekaterinburg")) return true;
        if(StringUtils.containsIgnoreCase(location, "kazan")) return true;
        if(StringUtils.containsIgnoreCase(location, "nizhny_novgorod")) return true;
        if(StringUtils.containsIgnoreCase(location, "chelyabinsk")) return true;
        if(StringUtils.containsIgnoreCase(location, "samara")) return true;
        if(StringUtils.containsIgnoreCase(location, "omsk")) return true;
        if(StringUtils.containsIgnoreCase(location, "rostov_on_don")) return true;
        if(StringUtils.containsIgnoreCase(location, "ufa")) return true;
        if(StringUtils.containsIgnoreCase(location, "krasnoyarsk")) return true;
        if(StringUtils.containsIgnoreCase(location, "voronezh")) return true;
        if(StringUtils.containsIgnoreCase(location, "perm")) return true;
        if(StringUtils.containsIgnoreCase(location, "volgograd")) return true;

        return false;
    }

}
