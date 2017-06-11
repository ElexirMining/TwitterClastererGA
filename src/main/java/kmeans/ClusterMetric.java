package kmeans;

import java.io.Serializable;

/**
 * Модель кластера, содержащая расстояния, необходимые для оценки
 */
public class ClusterMetric implements Serializable {

    private final long count;
    private final double sumDist;
    private final double sumSquaredDist;

    ClusterMetric(long count, double sumDist, double sumSquaredDist) {
        this.count = count;
        this.sumDist = sumDist;
        this.sumSquaredDist = sumSquaredDist;
    }

    long getCount() {
        return count;
    }

    double getSumDist() {
        return sumDist;
    }

    double getSumSquaredDist() {
        return sumSquaredDist;
    }

    double getMeanDist() {
        return sumDist / count;
    }

    ClusterMetric add(ClusterMetric other) {
        return new ClusterMetric(count + other.getCount(),
                sumDist + other.getSumDist(),
                sumSquaredDist + other.getSumSquaredDist());
    }
}