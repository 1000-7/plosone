public class TestStemmer {
    public static void main(String[] args) {
        Stemmer stemmer = new Stemmer();
        char[] chars = "analysis".toCharArray();
        stemmer.add(chars, chars.length);
        stemmer.stem();
        System.out.println(stemmer.toString());
    }
}
