package utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import java.util.Arrays;
import java.util.List;

/**
 * Утилита для проведения PCA над векторами
 */
public class PcaUtils {
    /**
     * Приводит вектора к 2 мерным значениям
     * @param rows Набор векторов
     * @return Вектора с 2 мерным значениям
     */
    public static JavaRDD<Vector> Process(JavaRDD<Vector> rows){
        RowMatrix mat = new RowMatrix(rows.rdd());

        //Приводим к 2 координатам
        Matrix pc = mat.computePrincipalComponents(2);
        RowMatrix projected = mat.multiply(pc);

        //Выводим обратно в JavaRDD
        return projected.rows().toJavaRDD();
    }

    /**
     * Приводит лист векторов к 2 мерным значениям
     * @param rows Набор векторов
     * @return Вектора с 2 мерным значениям
     */
    public static List<Vector> Process(List<Vector> rows){
        //При помощи SparkSingleton приводим лист к JavaRDD и пользуемся предыдущей функцией,
        //результат собираем и приводим к листу
        return Process(SparkSingleton.getContext().parallelize(rows)).collect();
    }

    /**
     * Приводит набор векторов к 2 мерным значениям
     * @param rows Набор векторов
     * @return Вектора с 2 мерным значениям
     */
    public static Vector[] Process(Vector[] rows){
        //Приводим набор к листу и пользуемся предыдущей функцией
        List<Vector> list = Process(Arrays.asList(rows));
        return list.toArray(new Vector[list.size()]);
    }
}
