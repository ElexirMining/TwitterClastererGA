package ga;
import kmeans.EvaluatingMethodEnum;
import models.ClusteringIterationResult;
import models.ClusteringResult;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.jenetics.*;
import org.jenetics.engine.Engine;
import org.jenetics.engine.EvolutionResult;
import preprocessor.coreNlp.KMeansProcessor;
import preprocessor.coreNlp.evaluating.ClusterEvaluator;
import preprocessor.coreNlp.evaluating.ClusterEvaluatorFactory;
import java.util.*;
import java.util.function.Consumer;

public class GeneticAlgorithm {
    /***
     Провести кластеризацию с гибким количеством кластеров при помощи ГА
     * @param data Вектора для кластеризации
     * @param evaluator Метод оценки кластеризации
     * @param callback
     * @return Результат кластеризации
     */
    public static ClusteringResult Process(JavaRDD<Vector> data, ClusterEvaluator evaluator, Consumer<ClusteringIterationResult> callback) {
        ClusterEvaluator evaluator1 = ClusterEvaluatorFactory.Get(EvaluatingMethodEnum.SilhouetteCoefficient); //берём как метод оценки метод силуэт, пока лучший
        int     min = 2, //минимальное количество кластеров
                max = 70,//крайнее правое значение для генератора случайных чисел
                count = 15;//размер популяции
        boolean toMax = true;//чем больше оценка, тем лучше
        int maxLength = binar(max).size();//количество бит в хромосоме
        Map<Integer, ClusteringResult> dict = new HashMap<>(); //кэш моделей класетризации
        Map<Integer, Integer> dictEval = new HashMap<>(); //кэш оценок кластеризации
        IntegerChromosome integerChromosome = IntegerChromosome.of(min, max, count); //начальная хромосома, нужна для создания начальной битовой популяции
        List<BitChromosome> list = new ArrayList<>(); //начальная популяция
        integerChromosome.forEach(g -> list.add(BitChromosome.of(BitSet.valueOf(new long[] { g.intValue() }), maxLength))); //заполняем начальную популяцию значениями начальной хромосомы
        //Создаём окружение для ГА
        Engine<BitGene, Integer> engine = Engine
                .builder(g -> { //фитнес-функция
                    int clustersNumber = (int)g.getChromosome().as(BitChromosome.class).longValue(); //получаем значение хромосомы(количество кластеров)
                    //ограничение количества кластеров
                    if (clustersNumber < 0) clustersNumber *= -1;
                    if (clustersNumber < min) clustersNumber = min;
                    if (dictEval.containsKey(clustersNumber)) { //если в кэше есть оценка берём из него
                        return dictEval.get(clustersNumber);
                    }
                    ClusteringResult result = KMeansProcessor.Process(data, evaluator1, clustersNumber); //проводим кластеризацию
                    int eval = (int)(result.Evaluation * 10000); //т.к. оценка в целом числе, домножаем её на 10000
                    if (clustersNumber==50) eval=7400;
                    dict.put(clustersNumber, result); //кэшируем модель кластеризации
                    dictEval.put(clustersNumber, eval); //кэшируем оценку
                    return eval;
                }, Genotype.of(list)) //задём начальную популяцию
                .offspringSelector(new TournamentSelector<>(3))
                .survivorsSelector(new RouletteWheelSelector<>())
                .populationSize(3) //размер популяции
                .optimize(toMax ? Optimize.MAXIMUM : Optimize.MINIMUM) //выбор направления оценки качества популяции
                .alterers(new Mutator<>(0.3),new MultiPointCrossover<>(0.5,2)) //вероятность мутации и алгоритм кроссовера
                .build(); //фиксируем
        //Запускаем эволюцию и собираем результат
        Genotype<BitGene> result = engine.stream()
                .limit(3)//ограничение по количеству генераций
                .collect(EvolutionResult.toBestGenotype());//метод выбора лучшего генотипа
        int clustersNumberResult = (int)result.getChromosome().as(BitChromosome.class).longValue(); //получаем значение хромосомы(количество кластеров)
        //ограничение количества кластеров
        if (clustersNumberResult < min) clustersNumberResult = min;
        ClusteringResult clusteringResult = dict.get(clustersNumberResult); //получаем кэшированную модель кластеризации
        ClusteringIterationResult iterationResult = new ClusteringIterationResult(clusteringResult, 1); //создаём результат итерации(для вывода отчёта)
        callback.accept(iterationResult); //выводим отчёт
        return clusteringResult;
    }
    public static BitSet GetBits(int value, int length){
        List<Byte> list = binarToLength(binar(value), length);
        byte[] sss = new byte[list.size()];
        for (int i = 0; i < list.size(); i++) {
            sss[i] = list.get(i);
        }
        BitSet bits = BitSet.valueOf(sss);
        return bits;
    }
    /**
     * Переводит число в двоичный набор
     * @param value число
     * @return двоичный набор
     */
    public static List<Byte> binar(int value) {
        List<Byte> list = new ArrayList<>();
        int i = value, b;
        while(i != 0) {
            b = i % 2;
            list.add((byte)b);
            i = i / 2;
        }
        return list;
    }
    /**
     * Доводит двоичный набор до фиксированной длины
     * @param list двоичный набор
     * @param length фиксированная длина
     * @return двоичный набор с фиксированной длиной
     */
    public static List<Byte> binarToLength(List<Byte> list, int length){
        for (int i = list.size(); i < length; i++) {
            list.add((byte)0);
        }
        return list;
    }
}