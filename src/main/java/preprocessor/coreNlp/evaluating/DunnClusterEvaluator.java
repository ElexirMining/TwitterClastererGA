package preprocessor.coreNlp.evaluating;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import kmeans.DunnIndex;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import java.util.List;

public class DunnClusterEvaluator extends ClusterEvaluator {
    public static DunnClusterEvaluator Instance = new DunnClusterEvaluator();

    /**
     * Оценить кластеризацию по методу Dunn
     * @param data Данные, участвующие в кластеризации
     * @param centers Центры кластеров
     * @return Результат оценки кластеризации
     */
    @Override
    public double Evaluate(JavaRDD<Vector> data, Vector[] centers) {
        List<ClusterInfo> clustersInfo = GetClustersInfo(data, centers);

        DunnIndex dunnIndex = new DunnIndex(clustersInfo);

        return dunnIndex.evaluate(data);
    }

    @Override
    public boolean IsBetter(double nextEval, double OldEval) {
        return nextEval > OldEval;
    }

    @Override
    public boolean IsBetter(double nextEval, double OldEval, double error) {
        return nextEval - OldEval > -error;
    }
}
