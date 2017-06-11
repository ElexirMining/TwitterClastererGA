package io;

import models.entity.SimpleClusterData;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

/**
 * Класс, реализующий функциональность посмотроения и сохранения данных в текстовый файл (данных кластера)
 */
public class SimpleClusterDataProvider implements Serializable, IDataProvider<JavaRDD<SimpleClusterData>> {

    private String _currentTransaction;
    private FileManager _manager;

    public SimpleClusterDataProvider(FileManager manager) {
        this._manager = manager;
    }

    /**
     * Метод, отвечающий за построение транзакции текстовых данных
     * @param sourceData данные кластера
     */
    @Override
    public void Build(JavaRDD<SimpleClusterData> sourceData) {
        JavaRDD<String> strings = sourceData.map(simpleClusterData -> { //перебор RDD-коллекции данных кластера
            StringBuilder sb = new StringBuilder();
            //центр кластера
            sb.append(String.format("C%s: %s\n", simpleClusterData.Number, simpleClusterData.Center));
            //векторы
            for (Vector vector : simpleClusterData.Items) {
                sb.append(String.format("%s: %s\n", simpleClusterData.Number, vector));
            }
            return sb.toString();
        });

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
