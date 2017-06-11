package models.entity.io;

import java.util.List;

/**
 * Модель данных кластера с указанием данных о веторах
 */
public class DocumentClusterDataIO extends ClusterDataIO<double[], VectorDataIO> {
    public DocumentClusterDataIO(Integer number, double[] center, List<VectorDataIO> items){
        super(number, center, items);
    }
}
