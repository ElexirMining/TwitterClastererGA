package preprocessor.coreNlp.evaluating;

import kmeans.EvaluatingMethodEnum;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

public class ClusterEvaluatorFactory {
    public static ClusterEvaluator Get(EvaluatingMethodEnum evaluatingMethodEnum){
        switch (evaluatingMethodEnum)
        {
            case Davies:
                return DaviesClusterEvaluator.Instance;
            case Dunn:
                return DunnClusterEvaluator.Instance;
            case SilhouetteCoefficient:
                return SilhouetteCoefficientEvaluator.Instance;
            default:
                return null;
        }
    }

    public static double Evaluate(EvaluatingMethodEnum evaluatingMethodEnum, JavaRDD<Vector> data, Vector[] centers){
        return Get(evaluatingMethodEnum).Evaluate(data, centers);
    }

    public static boolean IsBetter(EvaluatingMethodEnum evaluatingMethodEnum, double evaluating1, double evaluating2){
        return Get(evaluatingMethodEnum).IsBetter(evaluating1, evaluating2);
    }
}
