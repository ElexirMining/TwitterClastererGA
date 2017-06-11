package preprocessor.coreNlp;

import java.util.List;

public class Term {
    private static double TermFrequency(List<String> doc, String term) {
        double result = 0;
        for (String word : doc) {
            if (term.equalsIgnoreCase(word))
                result++;
        }
        return result / doc.size();
    }

    private static double InverseTermFrequency(List<List<String>> docs, String term) {
        double n = 0;
        for (List<String> doc : docs) {
            for (String word : doc) {
                if (term.equalsIgnoreCase(word)) {
                    n++;
                    break;
                }
            }
        }
        return Math.log(docs.size() / n);
    }

    public static double TFIDF(List<String> doc, List<List<String>> docs, String term) {
        return TermFrequency(doc, term) * InverseTermFrequency(docs, term);
    }
}
