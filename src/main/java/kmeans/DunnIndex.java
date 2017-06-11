package kmeans;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import java.util.ArrayList;
import java.util.List;

/**
 * Класс для оценки кластера по принципу Dunn Index
 */
public class DunnIndex extends AbstractKMeansEvaluation {

    public DunnIndex(List<ClusterInfo> clusters) {
        super(clusters);
    }

    /**
     * Оценить кластеризацию
     * @param evalData Данные для оценки
     * @return Оценки кластеризации
     */
    @Override
    public double evaluate(JavaRDD<Vector> evalData) {
        // Внутрикластерные расстояния (среднее расстояние до центра)
        double maxIntraClusterDistance = 0.0;
        for (ClusterMetric metric : fetchClusterMetrics(evalData).values().collect()) {
            maxIntraClusterDistance = Math.max(maxIntraClusterDistance, metric.getMeanDist());
        }

        // Межкластерное расстояние (расстояние между центрами)
        double minInterClusterDistance = Double.POSITIVE_INFINITY;
        List<ClusterInfo> clusters = new ArrayList<>(getClustersByID().values());
        DistanceFn<double[]> distanceFn = getDistanceFn();
        for (int i = 0; i < clusters.size(); i++) {
            double[] centerI = clusters.get(i).getCenter();
            // Расстояния симметричны, следовательно d(i,j) == d(j,i)
            for (int j = i + 1; j < clusters.size(); j++) {
                double[] centerJ = clusters.get(j).getCenter();
                minInterClusterDistance = Math.min(minInterClusterDistance, distanceFn.applyAsDouble(centerI, centerJ));
            }
        }

        return minInterClusterDistance / maxIntraClusterDistance;
    }

}