package kmeans;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import java.util.List;
import java.util.Map;

/**
 * Класс для оценки кластера по принципу Davies Bouldin
 */
public class DaviesBouldinIndex extends AbstractKMeansEvaluation {

    public DaviesBouldinIndex(List<ClusterInfo> clusters) {
        super(clusters);
    }

    /**
     * Оценить кластеризацию
     * @param evalData Данные для оценки
     * @return Оценки кластеризации
     */
    @Override
    public double evaluate(JavaRDD<Vector> evalData) {
        Map<Integer,ClusterMetric> clusterMetricsByID = fetchClusterMetrics(evalData).collectAsMap();
        double totalDBIndex = 0.0;
        Map<Integer,ClusterInfo> clustersByID = getClustersByID();
        DistanceFn<double[]> distanceFn = getDistanceFn();
        for (Map.Entry<Integer,ClusterInfo> entryI : clustersByID.entrySet()) {
            double maxDBIndex = 0.0;
            Integer idI = entryI.getKey();
            double[] centerI = entryI.getValue().getCenter();
            double clusterScatter1 = clusterMetricsByID.get(idI).getMeanDist();
            for (Map.Entry<Integer,ClusterInfo> entryJ : clustersByID.entrySet()) {
                Integer idJ = entryJ.getKey();
                if (!idI.equals(idJ)) {
                    double[] centerJ = entryJ.getValue().getCenter();
                    double clusterScatter2 = clusterMetricsByID.get(idJ).getMeanDist();
                    double dbIndex = (clusterScatter1 + clusterScatter2) / distanceFn.applyAsDouble(centerI, centerJ);
                    maxDBIndex = Math.max(maxDBIndex, dbIndex);
                }
            }
            totalDBIndex += maxDBIndex;
        }

        return totalDBIndex / clustersByID.size();
    }

}