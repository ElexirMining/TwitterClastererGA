package models;

import kmeans.EvaluatingMethodEnum;
import models.enums.StartModeEnum;
import models.enums.TwitsSourceEnum;

/**
 * Модель данных для настроек конфигурации запуска приложения
 */
public class StartAppConfiguration {
    public static StartAppConfiguration Instance = new StartAppConfiguration();

    public String pathIn;
    public String pathOut;
    public int numbClusters;
    public boolean localStart;
    public StartModeEnum mode;
    public EvaluatingMethodEnum evaluatingMethod;
    public TwitsSourceEnum twitsSource;

    public int butchDuration;
    public int windowDuration;
    public int slideDuration;
}
