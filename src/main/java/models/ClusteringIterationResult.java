package models;

public class ClusteringIterationResult {
    public ClusteringResult ClusteringResult;
    public Integer IterationNumber;

    public ClusteringIterationResult(ClusteringResult clusteringResult, Integer iterationNumber){
        ClusteringResult = clusteringResult;
        IterationNumber = iterationNumber;
    }
}
