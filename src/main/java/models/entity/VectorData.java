package models.entity;

import java.io.Serializable;


/**
 * Модель данных а документе, хранящая текст документа и входящие слова
 */
public class VectorData<T> implements Serializable {
    public T Vector;
    public DocumentData Document;

    public VectorData(T vector, DocumentData document){
        Vector = vector;
        Document = document;
    }
}
