package models.entity;

import org.apache.spark.mllib.linalg.Vector;

/**
 * Модель данных кластера с векторным центром и входящими элементами
 */
public class SimpleClusterData extends ClusterData<Vector, Vector> {
    public SimpleClusterData(Integer number, Vector center, Iterable<Vector> items){
        super(number, center, items);
    }
}
