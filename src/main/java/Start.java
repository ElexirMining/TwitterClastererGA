import kmeans.EvaluatingMethodEnum;
import models.StartAppConfiguration;
import models.enums.StartModeEnum;
import models.enums.TwitsSourceEnum;

public class Start {
    /**
     * Точка входа в приложение
     * @param args аргументы которые получает программа при старте
     */
    public static void main(String[] args){
        //Получаем статический конфиг запуска программы
        StartAppConfiguration config = StartAppConfiguration.Instance;
        int currentArg = 0;

        //Выбор источника твитов
        config.twitsSource = Integer.parseInt(args[currentArg++]) == 0                                                  //1
                ? TwitsSourceEnum.FromFiles
                : TwitsSourceEnum.FromTwitter;

        //При режиме получения из файлов, получаем ещё и путь
        switch (config.twitsSource) {
            case FromFiles:
                config.pathIn = args[currentArg++];
                break;
        }

        //Выходной путь
        config.pathOut = args[currentArg++];                                                                            //3
        //Выбор режима выбора количества кластеров
        config.mode = Integer.parseInt(args[currentArg++]) == 0                                                         //4
                ? StartModeEnum.Fixed
                : StartModeEnum.Flexible;
        //Выбор режима оценки кластеризации
        config.evaluatingMethod = Integer.parseInt(args[currentArg++]) == 0                                             //5
                ? EvaluatingMethodEnum.Davies
                : EvaluatingMethodEnum.Dunn;
        //Выбор размера батча
        config.butchDuration = Integer.parseInt(args[currentArg++]);                                                    //6
        //Выбор размера окна
        config.windowDuration = Integer.parseInt(args[currentArg++]);                                                   //7
        //Выбор размера слайда
        config.slideDuration = Integer.parseInt(args[currentArg++]);                                                    //8

        //Разная логика при разных режимах
        switch (config.mode){
            case Fixed:
                //Если фиксированное количество кластеров, то получаем количество кластеров и флаг локалки
                config.numbClusters = Integer.parseInt(args[currentArg++]);                                             //9
                if (args.length > currentArg && args[currentArg++].equals("-local")) config.localStart = true;
                break;
            case Flexible:
                //Получаем флаг локалки
                if (args.length > currentArg && args[currentArg++].equals("-local")) config.localStart = true;
                break;
        }

        Init.StartWork();
    }
}
