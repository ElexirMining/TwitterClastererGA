package utils;

import models.entity.DocumentClusterData;
import models.entity.DocumentData;
import models.entity.DocumentVectorData;
import models.entity.SimpleClusterData;
import models.entity.io.DocumentClusterDataIO;
import models.entity.io.VectorDataIO;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

public class ClusterDataMapper {
    /**
     * Возвращает собранный набор примитивных данных о кластерах
     * @param tfidf TF-IDF модель
     * @param clustersNumbers Список номеров кластеров, соответствующий списку tf-idf
     * @param clustersCenters Список векторов центров кластеров
     * @return Сформированный набор данных о кластерах
     */
    public static JavaRDD<SimpleClusterData> GetSimpleClusterData(JavaRDD<Vector> tfidf, JavaRDD<Integer> clustersNumbers, Vector[] clustersCenters) {
        //Группируем по номеру (для получения групп по каждому кластераму)
        //И формируем SimpleClusterData
        return clustersNumbers.zip(tfidf).groupByKey().sortByKey().map(tuple -> new SimpleClusterData(tuple._1, clustersCenters[tuple._1], tuple._2));
    }

    /**
     * Возвращает собранный набор данных о документах
     * @param docs Список документов
     * @param words Список слов, соответствующий списку документов
     * @return Сформированный набор данных о документах
     */
    public static JavaRDD<DocumentData> GetDocumentData(JavaRDD<String> docs, JavaRDD<List<String>> words){
        return Zip(docs, words).map(tuple -> new DocumentData(tuple._1, tuple._2));
    }

    /**
     * Возвращает собранный набор полных данных о кластерах
     * @param documentDataJavaRDD Список дакументов
     * @param tfidf TF-IDF модель
     * @param clustersCenters Список векторов центров кластеров
     * @param clustersNumbers Список номеров кластеров, соответствующий списку tf-idf
     * @return Сформированный набор данных о кластерах
     */
    public static JavaRDD<DocumentClusterData> GetDocumentClusterData(JavaRDD<DocumentData> documentDataJavaRDD, JavaRDD<Vector> tfidf, Vector[] clustersCenters, JavaRDD<Integer> clustersNumbers)
    {
        JavaRDD<DocumentVectorData> vectors = GetVectorData(documentDataJavaRDD, tfidf);
        //Соединяем данные о векторах с номерами кластеров
        //Группируем по номеру (для получения групп по каждому кластераму)
        //И формируем DocumentClusterData
        return Zip(clustersNumbers, vectors).groupByKey().sortByKey().map(tuple -> new DocumentClusterData(tuple._1, clustersCenters[tuple._1], tuple._2));
    }

    /**
     * Возвращает собранный набор полных данных о кластерах
     * @param documentDataJavaRDD Список дакументов
     * @param tfidf TF-IDF модель
     * @param clustersCenters Список векторов центров кластеров
     * @param clustersNumbers Список номеров кластеров, соответствующий списку tf-idf
     * @return Сформированный набор данных о кластерах
     */
    public static JavaRDD<DocumentClusterDataIO> GetDocumentClusterDataIO(JavaRDD<DocumentData> documentDataJavaRDD, JavaRDD<Vector> tfidf, Vector[] clustersCenters, JavaRDD<Integer> clustersNumbers)
    {
        JavaRDD<VectorDataIO> vectors = GetVectorDataIO(documentDataJavaRDD, tfidf);
        //Соединяем данные о векторах с номерами кластеров
        //Группируем по номеру (для получения групп по каждому кластераму)
        //И формируем IODocumentClusterData
        return Zip(clustersNumbers, vectors).groupByKey().sortByKey().map(tuple ->  {
            List<VectorDataIO> vectorsList = new LinkedList<>();
            tuple._2.forEach(v -> vectorsList.add(v));
            return new DocumentClusterDataIO(tuple._1, clustersCenters[tuple._1].toArray(), vectorsList);
        });
    }

    /**
     * Возвращает набор данных о векторах
     * @param documentDataJavaRDD Список дакументов
     * @param tfidf TF-IDF модель
     * @return Сформированный набор данных о векторах
     */
    public static JavaRDD<DocumentVectorData> GetVectorData(JavaRDD<DocumentData> documentDataJavaRDD, JavaRDD<Vector> tfidf)
    {
        //Делаем соответствие каждому вектору его документу и заворачиваем в DocumentVectorData
        return Zip(tfidf, documentDataJavaRDD).map(tuple -> new DocumentVectorData(tuple._1, tuple._2));
    }

    /**
     * Возвращает набор данных о векторах
     * @param documentDataJavaRDD Список дакументов
     * @param tfidf TF-IDF модель
     * @return Сформированный набор данных о векторах
     */
    public static JavaRDD<VectorDataIO> GetVectorDataIO(JavaRDD<DocumentData> documentDataJavaRDD, JavaRDD<Vector> tfidf)
    {
        //Делаем соответствие каждому вектору его документу и заворачиваем в DocumentVectorData
        return Zip(tfidf, documentDataJavaRDD).map(tuple -> new VectorDataIO(tuple._1.toArray(), tuple._2));
    }

    /**
     * Функция соответствия двух rdd списков одинаковой длинны
     * @param rdd1 Первый rdd список
     * @param rdd2 Второй rdd список
     * @param <T> Тип первого списка
     * @param <U> Тип второго списка
     * @return Парный rdd список, где каждый элемент 1 списка соответствует элементу 2 списка
     */
    private static <T, U> JavaPairRDD<T, U> Zip(JavaRDD<T> rdd1, JavaRDD<U> rdd2){
        //Индексируем оба rdd, соединяем их по индексам, отбрасываем индексы и возвращаем соединённые данные
        return JavaPairRDD.fromJavaRDD(JavaPairRDD.fromJavaRDD(rdd1.zipWithIndex().map(x -> new Tuple2<>(x._2, x._1))).join(JavaPairRDD.fromJavaRDD(rdd2.zipWithIndex().map(x -> new Tuple2<>(x._2, x._1)))).map(x -> x._2));
    }
}
