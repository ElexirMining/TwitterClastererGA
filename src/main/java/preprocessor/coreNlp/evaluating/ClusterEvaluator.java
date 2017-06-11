package preprocessor.coreNlp.evaluating;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import java.util.ArrayList;
import java.util.List;

/**
 * Утилита для оценки кластеризации
 */
public abstract class ClusterEvaluator {
    /**
     * Получение данных о кластеризации, необходимых для оценки
     * @param data Данные, участвующие в кластеризации
     * @param centers Центры кластеров
     * @return Данные о кластеризации
     */
    List<ClusterInfo> GetClustersInfo (JavaRDD<Vector> data, Vector[] centers){
        //Получения количества элементов в данных
        long dataCount = data.count();

        //Формируем набор информации о кластерах, необходимой для оценки
        List<ClusterInfo> clustersInfo = new ArrayList<>();

        for (int i = 0; i < centers.length; i++) {
            clustersInfo.add(new ClusterInfo(i, centers[i].toArray(), dataCount));
        }
        return clustersInfo;
    }

    /**
     * Оценить кластеризацию
     * @param data Данные, участвующие в кластеризации
     * @param centers Центры кластеров
     * @return Результат оценки кластеризации
     */
    public abstract double Evaluate(JavaRDD<Vector> data, Vector[] centers);

    /**
     * Сравнить оценку кластеризации
     * @param evaluating1
     * @param evaluating2
     * @return
     */
    public abstract boolean IsBetter(double evaluating1, double evaluating2);

    /**
     * Сравнить оценку кластеризации с указанной погрешностью
     * @param evaluating1
     * @param evaluating2
     * @return
     */
    public abstract boolean IsBetter(double evaluating1, double evaluating2, double error);
}
