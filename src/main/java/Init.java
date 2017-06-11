import io.*;
import kmeans.EvaluatingMethodEnum;
import models.ClusteringResult;
import models.StartAppConfiguration;
import models.enums.StartModeEnum;
import models.enums.TwitsSourceEnum;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.joda.time.DateTime;
import preprocessor.coreNlp.ClusterInfoWriter;
import preprocessor.coreNlp.KMeansProcessor;
import preprocessor.coreNlp.TwitDataPreprocessor;
import preprocessor.coreNlp.evaluating.ClusterEvaluator;
import preprocessor.coreNlp.evaluating.ClusterEvaluatorFactory;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import utils.SparkSingleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Init {
    /**
     * Основной работающий метод
     */
   // static SparkSession spark;

    public static void StartWork(){
        //Получаем конфиги старта приложеньки
        StartAppConfiguration config = StartAppConfiguration.Instance;
        //Устанавливаем режим файловой системы, в зависимости от флага локалки
        FSConfiguration.Instance.FileSystemType = config.localStart ? FileSystemTypeEnum.WindowsFS : FileSystemTypeEnum.HDFS;

        JavaSparkContext context = SparkSingleton.getContext(); //Получаем контекст спарка
        SparkSession session = SparkSingleton.getSparkSession(); //Задаём сессию
        JavaStreamingContext jssc = SparkSingleton.getStreamingContext(); //Получаем контекст стриминга

        //Получаем dstream, из файла или твиттера, в зависимости от настроек
        JavaDStream<String> lines = config.twitsSource == TwitsSourceEnum.FromFiles
                ? jssc.textFileStream(config.pathIn)
                : getTwitterJavaDStream(jssc);

        lines.persist(StorageLevel.DISK_ONLY());

        //Устанавливаем окно
        JavaDStream<String> window = lines.window(Durations.seconds(config.windowDuration), Durations.seconds(config.slideDuration));
        //Выполняем для каждого собранного rdd функцию итерации
        window.foreachRDD(rdd -> StartIterarion(rdd, config.pathOut, config.mode, config.evaluatingMethod, config.numbClusters));
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            System.out.println("Haven't seen any document yet");
        }
        jssc.close();
    }

    /**
     * Метод создания стрима по твиттеру
     * @param jssc Контекст стрима
     * @return стрим
     */
    private static JavaDStream<String> getTwitterJavaDStream(JavaStreamingContext jssc) {
        //Параметры для апи твиттера
        String ConsumerKey =        "lhKGIq2qHL2NRQDJqcRkfQR1t";
        String ConsumerSecret =     "FbinmG2pnCnvH2UmQIOTuVJH5uTuXFHrsYKdo2FG4ebuEectAZ";
        String AccessToken =        "775075310974935040-YJdwr4XfgOvNAHqDSpYbevCuoCsIU6u";
        String AccessTokenSecret =  "ODxt0mrp1ZwSsQQj3yTaRkpEHAmjPoAN7CQITy7t0tmQn";
        //Собираем конфигурацию
        Configuration configuration = new ConfigurationBuilder()
                .setOAuthConsumerKey(ConsumerKey)
                .setOAuthConsumerSecret(ConsumerSecret)
                .setOAuthAccessToken(AccessToken)
                .setOAuthAccessTokenSecret(AccessTokenSecret)
                .build();
        //Аввторизуемся по параметрам
        Authorization twitterAuth  = AuthorizationFactory.getInstance(configuration);
        //Создаём стрим по авторизации и контексту
        return TwitterUtils.createStream(jssc, twitterAuth)
                .filter(status -> !status.isRetweet() && status.getLang().equals("en")) //устанавливаем фильтры по ретвиту и языку
                .map(status -> status.getText()); //полуаем только текст твита
    }

    /**
     * Метод обработки одного RDD
     * @param twits Твиты
     * @param pathOut Выходной путь
     * @param mode Режим выбора количества кластеров
     * @param evaluatingMethod Режим оценки кластеров
     * @param numbClusters Количество кластеров
     */
    public static void StartIterarion(JavaRDD<String> twits, String pathOut, StartModeEnum mode, EvaluatingMethodEnum evaluatingMethod, int numbClusters){
        twits.cache();
        //Пропускаем пустой RDD
        if (twits.count() == 0) return;
        //Путь текущей сессии (в формате текущей даты и времени)
        String sessionPath = pathOut + DateTime.now().toString("dd'.'MM'.'yyyy HH.mm.ss") + "/";
        TwitDataPreprocessor twitDataPreprocessor =  new TwitDataPreprocessor(twits);// отправляем на препроцессинг
       //получаем tf-idf
        //tfidfs.cache();
        JavaRDD<List<String>> words = twitDataPreprocessor.GetWords();//получаем слова
        words.cache();
        //Делаем RDD с типом ROW для будущего датасета
        JavaRDD<Row> dataTest= words.map(x -> {
            String[] arr = new String[x.size()];
            String[] arr1 = x.toArray(arr);
            return RowFactory.create(Arrays.asList(arr1));
        });
        //Собираем RDD в список
        List<Row> bbb = dataTest.collect();
        StructType testSchema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });


        SparkSession session = SparkSingleton.getSparkSession();
        //Создаем датасет из предобработанных твитов
        Dataset<Row> testDocument = session.createDataFrame(bbb, testSchema);
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("result")
                //.setVectorSize(50)
                .setMinCount(0);

        //Загружаем обученную ворд2век модель
        Word2VecModel model=Word2VecModel.load("w2vModel");
        //Получаем вектора сообщений
        Dataset<Row> result1 = model.transform(testDocument);//.transform(testDocument);

        FileManager tfFile = FileManagerFactory.CreateByCurrentFS(sessionPath + "tf.txt");//создаём файл для записи цифровых векторов
        TFDataProvider tfdp = new TFDataProvider(tfFile); //класс для записи цифровых векторов
        //JavaRDD<Vector> tfidfs = twitDataPreprocessor.GetTFidfMatrix();
        JavaRDD<Vector> tfidfs=result1.javaRDD().map(x -> Vectors.dense(((DenseVector) x.get(1)).values()));
        tfdp.Build(tfidfs);
        tfdp.Save();//записываем результаты tf-idf

        //tfidfs.cache();

        //Получаем оценщика для кластеров из фабрики по текущим настройкам
        ClusterEvaluator evaluator = ClusterEvaluatorFactory.Get(evaluatingMethod);;

        List<String> summoryList = new ArrayList<>(); //лист результатов оценивания
        switch (mode){
            case Fixed:
                ClusteringResult result = KMeansProcessor.Process(tfidfs, evaluator, numbClusters);//отправляем цифровые вектора в k-means, получаем результат
                summoryList.add(ClusterInfoWriter.GetSummary(result.ClustersNumber, result.Evaluation));//дополняем лист результатов данными о текущей итерации
                ClusterInfoWriter.WriteInfo(twits, words, tfidfs, result.GetClustersNumbers(tfidfs), result.GetCenters(), sessionPath);//записываем информацию о кластерах
                break;
            case Flexible:
                //отправляем цифровые вектора в k-means, получаем результат
                KMeansProcessor.Process(tfidfs, evaluator, cr -> { //даём метод по записи информации о кластерах, чтобы не мусорить сам обработчик, выполняется при каждой итерации оценивания
                    String iterPath = String.format("%sIteration%s- %s clusters/", sessionPath, cr.IterationNumber, cr.ClusteringResult.ClustersNumber);//путь для текущей итерации оценивания
                    summoryList.add(ClusterInfoWriter.GetSummary(cr.ClusteringResult.ClustersNumber, cr.ClusteringResult.Evaluation));//дополняем лист результатов данными о текущей итерации
                    ClusterInfoWriter.WriteInfo(twits, words, tfidfs, cr.ClusteringResult.GetClustersNumbers(tfidfs), cr.ClusteringResult.GetCenters(), iterPath);//записываем информацию о кластерах
                });
                break;
        }
        ClusterInfoWriter.WriteSummary(sessionPath, summoryList.toArray(new String[summoryList.size()]));//записываем результаты оценивания
    }
}
