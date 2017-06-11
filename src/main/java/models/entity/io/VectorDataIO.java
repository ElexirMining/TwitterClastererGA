package models.entity.io;

import models.entity.DocumentData;
import models.entity.VectorData;


/**
 * Модель данных а документе, хранящая текст документа и входящие слова
 */
public class VectorDataIO extends VectorData<double[]> {
    public VectorDataIO(double[] vector, DocumentData document){
        super(vector, document);
    }
}
