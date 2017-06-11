package preprocessor.coreNlp;

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import utils.StanfordCoreNLPSingleton;

import java.util.LinkedList;
import java.util.List;

public class Lemmatizer {

    private static StanfordCoreNLP pipeline;

    public static List<String> Execute(String documentText)
    {
        List<String> lemmas = new LinkedList<String>();//создаем список для наполнения
        StanfordCoreNLP pipeline = StanfordCoreNLPSingleton.getPipeline();//получаем экземпляр ламматизера токенизера и много чего еще из nlp core
        Annotation document = new Annotation(documentText);//создаем экземпляр аннотации  из документа
        pipeline.annotate(document);
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for (CoreMap sentence : sentences) {//перебираем все предложения
            //TODO: mb move tokenizer to separate helper
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {//перебираем токены
                lemmas.add(token.get(LemmaAnnotation.class));//заполняем список чистыми токенами
            }
        }

        return lemmas;//возвращаем список
    }
}
