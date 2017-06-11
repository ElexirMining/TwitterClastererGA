package utils;

import models.StartAppConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkSingleton {
    /**
     * Use for make JavaSparkContext singleton object
     */
    private static JavaSparkContext sparkContext; //инстанс контекста для Spark

    public static synchronized JavaSparkContext getContext() {
        if(sparkContext == null) {//ленивое создание и возвращение инстанса.
            try {
                SparkConf conf = new SparkConf()
                        .setAppName("Spark user-activity")
                        .set("spark.streaming.blockInterval", StartAppConfiguration.Instance.butchDuration + "ms");
                if (StartAppConfiguration.Instance.localStart)
                    conf = conf
                            .setMaster("local[2]")            //local - означает запуск в локальном режиме.
                            .set("spark.driver.host", "localhost");    //это тоже необходимо для локального режима
                sparkContext = new JavaSparkContext(conf);
            } catch (Exception e) {
                throw new RuntimeException("При создании sparkContext произошла ошибка\n" + e.toString());
            }
        }
        return sparkContext;
    }

    /**
     * Use for make JavaSparkContext singleton object
     */
    private static JavaStreamingContext sparkStreamingContext; //инстанс контекста для Spark

    /**
     * Make and return sparkContext
     * @return sparkContext
     */
    public static synchronized JavaStreamingContext getStreamingContext(){
        if(sparkStreamingContext == null){//ленивое создание и возвращение инстанса.
            try{
                sparkStreamingContext = new JavaStreamingContext(getContext(), Durations.milliseconds(StartAppConfiguration.Instance.butchDuration));
            }catch (Exception e){
                throw new RuntimeException("При создании sparkStreamingContext произошла ошибка\n"+e.toString());
            }
        }
        return sparkStreamingContext;
    }

    /**
     * Use for make SparkSession singleton object
     */
    private static SparkSession sparkSession; //инстанс контекста для Spark

    /**
     * Make and return SparkSession
     * @return sparkContext
     */
    public static synchronized SparkSession getSparkSession(){
        if(sparkSession == null){//ленивое создание и возвращение инстанса.
            try{
                sparkSession = SparkSession.builder()//сессия
                        .config("spark.sql.warehouse.dir", "spark-warehouse")
                        .getOrCreate();
            }catch (Exception e){
                throw new RuntimeException("При создании SparkSession произошла ошибка\n"+e.toString());
            }
        }
        return sparkSession;
    }
}
