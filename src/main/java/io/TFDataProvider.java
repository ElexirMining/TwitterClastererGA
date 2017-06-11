package io;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

/**
 * Класс, реализующий функциональность построения и сохранения данных в текстовый файл (данных векторов tfidf)
 */
public class TFDataProvider implements Serializable, IDataProvider<JavaRDD<Vector>> {

    private String _currentTransaction;
    private FileManager _manager;

    public TFDataProvider(FileManager manager) {
        this._manager = manager;
        this._currentTransaction = "";
    }

    /**
     * Метод, отвечающий за построение транзакции текстовых данных
     * @param tfidf RDD-коллекция векторов tfidf
     */
    public void Build(JavaRDD<Vector> tfidf){

        JavaRDD<String> strings = tfidf.map(vector -> vector.toString());

        //запись транзации в переменную
        this._currentTransaction = StringUtils.join(strings.collect(), "\n");
    }

    /**
     * Метод, сохраняющий текстовые данные в файл
     */
    @Override
    public void Save() {
        //записываем транзакцию
        _manager.Write(this._currentTransaction);
    }
}
