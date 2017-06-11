package models;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;

/**
 * Модель данных о результате кластеризации, содержит векторный набор центров кластеров и набор номеров кластеров
 */
public class ClusteringResult {
    public int ClustersNumber;
    public double Evaluation;

    private KMeansModel _model;

    public JavaRDD<Integer> GetClustersNumbers(JavaRDD<Vector> javaRDD){
        JavaRDD<Integer> numbers = _model.predict(javaRDD);
        numbers.cache();
        return numbers;
    }

    public Vector[] GetCenters(){
        return _model.clusterCenters();
    }

    public ClusteringResult(KMeansModel model, int clustersNumber, double evaluation){
        _model = model;
        ClustersNumber = clustersNumber;
        Evaluation = evaluation;
    }
}
