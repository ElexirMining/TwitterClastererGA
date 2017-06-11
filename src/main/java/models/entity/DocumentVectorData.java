package models.entity;

import org.apache.spark.mllib.linalg.Vector;


/**
 * Модель данных а документе, хранящая текст документа и входящие слова
 */
public class DocumentVectorData extends VectorData<Vector> {
    public DocumentVectorData(Vector vector, DocumentData document){
        super(vector, document);
    }
}
