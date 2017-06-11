package preprocessor.coreNlp;

import functions.LemmatizeMapPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import utils.SparkSingleton;

import java.io.Serializable;
import java.util.List;

public class TwitDataPreprocessor implements Serializable{
    private JavaRDD<String> _twits;
    private JavaRDD<List<String>> _words;

    public TwitDataPreprocessor(JavaRDD<String> twits) {
        this._twits = twits;
        this._words = GetWordsFromTwits(twits);
    }

    public JavaRDD<Vector> GetTFidfMatrix(){
        return this.TFidfMatrix(GetWordsFromTwits(this._twits));
    }

    public JavaRDD<Vector> GetCustomTFidfMatrix(){
        return this.CustomTFidfMatrix(GetWordsFromTwits(this._twits));
    }

    public JavaRDD<List<String>> GetWords(){
        return _words;
    }

    /**
     * Возвращает RDD с списками лемматизированных\токенехированных твитов
     * @param twits RDD документов(твитов)
     * @return RDD списки очищенных документов
     */
    private JavaRDD<Row> Lemmatize(JavaRDD<String> twits){
        return twits.mapPartitions(new LemmatizeMapPartition());
    }

    private Dataset<Row> RemoveStopWords(JavaRDD<Row> twits){
        return StopWordsRemover.Execute(twits);
    }

    /**
     * Возвращает RDD с не пустыми списками очищенных слов из твитов
     * @param twits RDD "грязных" твитов
     * @return списки очищенных твитов
     */
    private JavaRDD<List<String>> GetWordsFromTwits(JavaRDD<String> twits){
        return this.RemoveStopWords(this.Lemmatize(twits)).toJavaRDD().filter(row -> !row.getList(0).isEmpty()).map(row -> row.getList(0));
    }

    /**
     * Возвращает tf-idf векторы в RDD
     * @param words RDD списков очищенных слов каждого твита
     * @return tf-idf векторы в RDD
     */
    private JavaRDD<Vector> TFidfMatrix(JavaRDD<List<String>> words){
        HashingTF hashingTF = new HashingTF(32);//num features 32 ???

        JavaRDD<Vector> featurizedData = hashingTF.transform(words);
        // alternatively, CountVectorizer can also be used to get term frequency vectors

        IDF idf = new IDF();
        IDFModel idfModel = idf.fit(featurizedData);

        featurizedData.cache();

        return idfModel.transform(featurizedData);
    }

    private JavaRDD<Vector> CustomTFidfMatrix(JavaRDD<List<String>> words){
        JavaRDD<Vector> tfidfResult = words.map(new Function<List<String>, Vector>() {
            @Override
            public Vector call(List<String> strings) throws Exception {
                JavaSparkContext sc = SparkSingleton.getContext();
                JavaRDD<String> rdd = sc.parallelize(strings);

                JavaRDD<Double> listrdd = rdd.map(new Function<String, Double>() {
                    @Override
                    public Double call(String s) throws Exception {
                        return Term.TFIDF(strings, words.collect(), s);
                    }
                });

                List<Double> list = listrdd.collect();
                double[] array = new double[list.size()];

                return Vectors.dense(array);
            }
        });

        return tfidfResult;
    }
}