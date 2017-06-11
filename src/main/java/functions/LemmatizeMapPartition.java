package functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import preprocessor.coreNlp.Lemmatizer;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class LemmatizeMapPartition implements FlatMapFunction<Iterator<String>, Row> {
    /**
     * Реализация FlatMap функции для лемматизации токенизации очищения от ссылок и плохих символов
     * возвращает очищенный список слов документа.
     * @param iter итератор для итерации по партишину
     * @return возвращает итератор для внутреннего механизма mapPartition
     * @throws Exception --
     */
    public Iterator<Row> call(Iterator<String> iter) throws Exception {
        List<Row> lemmas = new LinkedList<>();

        while (iter.hasNext()) {
            //TODO: move string manipulation to other class or this.method
            String clearDocText = iter.next().replaceAll("(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?", "")//регулярка для удаленния ссылок
                    .replaceAll("[^A-z ]", "").toLowerCase();//регулярка для удаления всех символов которые не входят в промежуток A-z и приведение к нижнему регистру
            lemmas.add(RowFactory.create(Lemmatizer.Execute(clearDocText)));//наполнение списка после очистки в Lemmatizer
        }

        return lemmas.iterator();
    }
}
