package utils;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.util.Properties;

public class StanfordCoreNLPSingleton {
    private static StanfordCoreNLP pipeline;//инстанс CoreNLP
    private static Properties props; //инстанс параметров для CoreNLP
    private StanfordCoreNLPSingleton(){

    }

    public static StanfordCoreNLP getPipeline() {
        if(props==null){//ленивое создание инстанса параметров
            try {
                props = new Properties();
                props.put("annotators", "tokenize, ssplit, pos, lemma");//параметры токенизации, разделения, лемматизации.
            }catch (Exception e){
                throw new RuntimeException("При создании Properties для pipline произошла ошибка\n"+e.toString());
            }
        }
        if(pipeline==null){//ленивое создание инстанса CoreNLP
            try {
                pipeline = new StanfordCoreNLP(props);//инициализация с параметрами определенными выше
            }catch (Exception e){
                throw new RuntimeException("При создании StanfordCoreNLP произошла ошибка\n"+e.toString());
            }
        }
        return pipeline;//Возвращение инстанса по требованию
    }
}
