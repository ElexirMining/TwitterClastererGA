package models.entity;

import org.apache.spark.mllib.linalg.Vector;

/**
 * Модель данных кластера с указанием данных о веторах
 */
public class DocumentClusterData extends ClusterData<Vector, DocumentVectorData> {
    public DocumentClusterData(Integer number, Vector center, Iterable<DocumentVectorData> items){
        super(number, center, items);
    }
}
