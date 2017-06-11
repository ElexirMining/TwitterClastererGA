package preprocessor.coreNlp;

import io.FileManager;
import io.FileManagerFactory;
import kmeans.EvaluatingMethodEnum;
import models.ClusteringIterationResult;
import models.ClusteringResult;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import preprocessor.coreNlp.evaluating.ClusterEvaluator;
import preprocessor.coreNlp.evaluating.ClusterEvaluatorFactory;

import java.util.function.Consumer;

/**
 * Кластеризатор с алгоритмом KMeans
 */
public class KMeansProcessor {
    static KMeans ss = new KMeans()
            .setInitializationMode("k-means||");

    /**
     * Провести кластеризацию с гибким количеством кластеров
     * @param data Вектора для кластеризации
     * @param evaluator Метод оценки кластеризации
     * @return Результат кластеризации
     */
    public static ClusteringResult Process(JavaRDD<Vector> data, ClusterEvaluator evaluator, Consumer<ClusteringIterationResult> callback) {
        int numClusters = 2;
        int numIteration = 1;
        int numCheckings = 20;
        int numStart = 1;

        ClusterEvaluator evaluator1 = ClusterEvaluatorFactory.Get(EvaluatingMethodEnum.Davies);
        ClusterEvaluator evaluator2 = ClusterEvaluatorFactory.Get(EvaluatingMethodEnum.Dunn);
        ClusterEvaluator evaluator3 = ClusterEvaluatorFactory.Get(EvaluatingMethodEnum.SilhouetteCoefficient);

        ClusteringResult result = Process(data, evaluator, numClusters);
        callback.accept(new ClusteringIterationResult(result, numIteration++));

        double eval1 = evaluator1.Evaluate(data, result.GetCenters());
        double eval2 = evaluator2.Evaluate(data, result.GetCenters());
        double eval3 = evaluator3.Evaluate(data, result.GetCenters());
        System.out.println(eval1);
        System.out.println(eval3);

        double nextEval1, nextEval2, nextEval3;
        FileManager log = FileManagerFactory.CreateByCurrentFS("D:/data/log.txt");
        StringBuilder sb = new StringBuilder();
        sb.append("#\tDavies\tDunn\tSilhouetteCoefficient\n");
        ClusteringResult nextResult = null;
        while (true) {
            boolean isBetter = false;
            for (int i = 0; i < numCheckings; i++) {
                numClusters++;
                for (int j = 0; j < numStart; j++) {
                    nextResult = Process(data, evaluator, numClusters);
                    nextEval1 = evaluator1.Evaluate(data, nextResult.GetCenters());
                    nextEval2 = evaluator2.Evaluate(data, nextResult.GetCenters());
                    nextEval3 = evaluator3.Evaluate(data, nextResult.GetCenters());
                    sb.append(String.format("%s\t%.3f\t%.3f\t%.3f\n", numClusters, nextEval1, nextEval2, nextEval3));
                    if (evaluator1.IsBetter(nextEval1, eval1) &&
                    //evaluator2.IsBetter(nextEval2, eval2, 0.7) &&
                      evaluator3.IsBetter(nextEval3, eval3)) {
                        eval1 = nextEval1;
                      //  eval2 = nextEval2;
                        eval3 = nextEval3;
                        isBetter = true;
                        break;
                    }
                }
                sb.append("\n");
                if (isBetter) break;
            }
            if (!isBetter) break;
            callback.accept(new ClusteringIterationResult(nextResult, numIteration++));
            result = nextResult;
        }
        log.Write(sb.toString());
        return result;
    }

    /**
     * Провести кластеризацию с указанным количеством кластеров
     * @param data Вектора для кластеризации
     * @param evaluator Метод оценки кластеризации
     * @return Результат кластеризации
     */
    public static ClusteringResult Process(JavaRDD<Vector> data, ClusterEvaluator evaluator, int numClusters){
        KMeansModel clusters = Train(data, numClusters, 20); //обучаем модель
        Vector[] centers = clusters.clusterCenters();
        System.out.println(centers);
        double evaluation = evaluator.Evaluate(data, centers);
        return new ClusteringResult(clusters, numClusters, evaluation);
    }

    public static KMeansModel Train(JavaRDD<Vector> data, int numClusters, int numIterations){
        return ss.train(data.rdd(), numClusters, numIterations); //обучаем модель
    }
}