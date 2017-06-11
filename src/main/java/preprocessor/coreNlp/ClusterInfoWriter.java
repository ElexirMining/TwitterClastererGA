package preprocessor.coreNlp;

import io.DocumentClusterDataProvider;
import io.FileManager;
import io.FileManagerFactory;
import io.SimpleClusterDataProvider;
import models.entity.DocumentClusterData;
import models.entity.DocumentData;
import models.entity.SimpleClusterData;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import utils.ClusterDataMapper;
import utils.Visualizer;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Класс, отвечющий за запись информации о кластеризации
 */
public class ClusterInfoWriter {
    /**
     * Записать информацию о кластеризации
     * @param twits Твиты
     * @param words Слова
     * @param tfidfs TF-IDF
     * @param clustersNumbers Количество кластеров
     * @param centers Центры кластеров
     * @param path Путь записи
     */
    public static void WriteInfo(JavaRDD<String> twits, JavaRDD<List<String>> words, JavaRDD<Vector> tfidfs, JavaRDD<Integer> clustersNumbers, Vector[] centers, String path){
        JavaRDD<SimpleClusterData> clustersData = ClusterDataMapper.GetSimpleClusterData(tfidfs, clustersNumbers, centers);//приводим результат кластеризации в общую форму

        FileManager cdpFileManager = FileManagerFactory.CreateByCurrentFS(path + "clusters.txt"); //объявляем файл для записи SimpleClusterData
        SimpleClusterDataProvider cdp = new SimpleClusterDataProvider(cdpFileManager);
        cdp.Build(clustersData);
        cdp.Save();//сохраняем в файл

        JavaRDD<DocumentData> documentDataJavaRDD = ClusterDataMapper.GetDocumentData(twits, words);//приводим данные о твитах и словах в общую форму
        JavaRDD<DocumentClusterData> documentClusterDataJavaRDD = ClusterDataMapper.GetDocumentClusterData(documentDataJavaRDD, tfidfs, centers, clustersNumbers);//приводим результат в общую форму

        FileManager dcdpFileManager = FileManagerFactory.CreateByCurrentFS(path + "clusters2.txt"); //объявляем файл для записи DocumentClusterData
        DocumentClusterDataProvider dcdp = new DocumentClusterDataProvider(dcdpFileManager);
        dcdp.Build(documentClusterDataJavaRDD);
        dcdp.Save();//сохраняем в файл

        //Формирование модели для чтения/сохранения
        //JavaRDD<DocumentClusterDataIO> ioDocumentClusterDataJavaRDD = ClusterDataMapper.GetDocumentClusterDataIO(documentDataJavaRDD, tfidfs, centers, clustersNumbers);
        //FileManager jsonFileManager = FileManagerFactory.CreateByCurrentFS(path + "clustersJSON.txt");
        //JsonSerializer.Dump(jsonFileManager, ioDocumentClusterDataJavaRDD.collect());//Сохранение в JSON

        FileManager imgFileManager = FileManagerFactory.CreateByCurrentFS(path + "clusters.png"); //объявляем файл для записи изображения
        Visualizer.Plot(imgFileManager, tfidfs, clustersNumbers, centers);
    }

    /**
     * Возвращает отформатированную строку с информацией о количестве кластеров и оценке
     * @param clustersNumber Количество кластров
     * @param evaluation Оценка
     * @return Отформатированная строка
     */
    public static String GetSummary(int clustersNumber, double evaluation) {
        return String.format("%s clusters, Index=%.3f",clustersNumber, evaluation);
    }

    /**
     * Записывает информацию о количестве кластеров и оценке
     * @param path Путь записи
     * @param info Информация о количестве кластеров и оценке
     */
    public static void WriteSummary(String path, String... info) {
        FileManager summaryInfoFileManager = FileManagerFactory.CreateByCurrentFS(path + "summaryInfo.txt"); //объявляем файл
        summaryInfoFileManager.Write(String.join("\n", info));
    }
}
