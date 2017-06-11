package io;

import models.entity.DocumentClusterData;
import models.entity.DocumentVectorData;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Класс, реализующий функциональность построения и сохранения данных в текстовый файл (данных твитов кластера)
 */
public class DocumentClusterDataProvider implements Serializable, IDataProvider<JavaRDD<DocumentClusterData>> {

    private String _currentTransaction;
    private FileManager _manager;

    public DocumentClusterDataProvider(FileManager manager) {
        this._manager = manager;
    }

    /**
     * Метод, отвечающий за построение транзакции текстовых данных (данных твитов кластера)
     * @param sourceData RDD-коллекция данных твитов кластера
     */
    @Override
    public void Build(JavaRDD<DocumentClusterData> sourceData) {
        JavaRDD<String> strings = sourceData.map(data -> {
            StringBuilder sb = new StringBuilder();
            //вектор кластера
            sb.append(String.format("Кластер%s: %s\n", data.Number, data.Center));
            //данные вектора tfidf
            for (DocumentVectorData vectorData : data.Items) {
                sb.append(String.format("%s:\t %s\n\t %s\n\t", data.Number, vectorData.Vector,vectorData.Document.Text));

                for (String word : vectorData.Document.Words) {
                    sb.append(" " + word);
                }
                sb.append("\n");
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
