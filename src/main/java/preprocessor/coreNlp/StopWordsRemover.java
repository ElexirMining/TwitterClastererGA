package preprocessor.coreNlp;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import utils.SparkSingleton;

public class StopWordsRemover {
    public static Dataset<Row> Execute(JavaRDD<Row> rows){
        SparkSession session = SparkSingleton.getSparkSession();                                                          //получаем или создаем Spark Session

        org.apache.spark.ml.feature.StopWordsRemover sw = new org.apache.spark.ml.feature.StopWordsRemover()
                .setInputCol("raw")
                .setOutputCol("filtered");                                                                              // coздаем экземпляр StopWordsRemover задаем колонку входную и выходную

        StructType schema = new StructType(new StructField[]{
                new StructField(
                        "raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
        });                                                                                                             //схема для Dataset

        Dataset<Row> dataset =  session.createDataFrame(rows.collect(), schema);


        return sw.transform(dataset).select("filtered");                                                                //производим удаление стоп слов и возвращаем колонку без стопслов
    }
}
