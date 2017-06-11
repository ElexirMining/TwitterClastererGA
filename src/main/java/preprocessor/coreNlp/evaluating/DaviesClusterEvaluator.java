package preprocessor.coreNlp.evaluating;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import kmeans.DaviesBouldinIndex;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import java.util.List;

public class DaviesClusterEvaluator extends ClusterEvaluator {
    public static DaviesClusterEvaluator Instance = new DaviesClusterEvaluator();

    /**
     * Оценить кластеризацию по методу Davies Bouldin
     * @param data Данные, участвующие в кластеризации
     * @param centers Центры кластеров
     * @return Результат оценки кластеризации
     */
    @Override
    public double Evaluate(JavaRDD<Vector> data, Vector[] centers) {
        List<ClusterInfo> clustersInfo = GetClustersInfo(data, centers);

        DaviesBouldinIndex daviesBouldinIndex = new DaviesBouldinIndex(clustersInfo);

        return daviesBouldinIndex.evaluate(data);
    }

    @Override
    public boolean IsBetter(double nextEval, double OldEval) {
        return nextEval < OldEval;
    }

    @Override
    public boolean IsBetter(double nextEval, double OldEval, double error) {
        return nextEval - OldEval < error;
    }
}
