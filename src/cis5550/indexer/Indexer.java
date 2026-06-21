package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cis5550.external.PorterStemmer;
import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.Hasher;

public class Indexer {
    // SOH (char 1): separator between url entries inside a single index value (also what
    // the Frontend's splitIndexEntries expects in the final pt-index value).
    private static final String SOH = "";
    // STX (char 2): separator between url and page in the packed pt-crawl row, and between
    // the word and the url:positions chunk in an inverted-index entry.
    private static final char STX = '';
    // Cap on how much page text we tokenize. Also caps the size of the packed (url, page) temp
    // table that fromTable produces — that table is held in the KVS workers' memory, so we keep
    // it modest. The first ~10 KB of a page is plenty for the MAX_WORDS_PER_PAGE-word index entry.
    private static final int MAX_PAGE_CHARS = 10_000;
    // Cap on distinct words indexed per page (incl. stems) — the body's first N distinct
    // words are the title + lead paragraphs, which capture the page's topic. This directly
    // bounds the number of KVS writes the inverted-index build does (≈ N x #pages), which is
    // the dominant cost on this small EC2 cluster — keep it small.
    private static final int MAX_WORDS_PER_PAGE = 30;
    // Cap on positions stored per word per page (enough for phrase detection).
    private static final int MAX_POS_CHARS_PER_WORD = 200;
    // Cap on the size of a word's accumulated posting list during foldByKey. A high-frequency
    // word (Wikipedia chrome like "edit", "references" — on ~every one of tens of thousands of
    // pages) would otherwise grow into a multi-MB string built by O(n^2) string concatenation,
    // wedging the KVS worker that owns that key. ~40 KB of postings is hundreds of docs — far
    // more than any ranked result page ever needs (and the IDF for such words is ~0 anyway).
    private static final int MAX_FOLDED_CHARS = 40_000;

    // NLTK list of stop words
    public static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
            "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you",
            "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself",
            "she", "her", "hers", "herself", "it", "its", "itself", "they", "them",
            "their", "theirs", "themselves", "what", "which", "who", "whom", "this",
            "that", "these", "those", "am", "is", "are", "was", "were", "be", "been",
            "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a",
            "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into",
            "through", "during", "before", "after", "above", "below", "to", "from", "up",
            "down", "in", "out", "on", "off", "over", "under", "again", "further", "then",
            "once", "here", "there", "when", "where", "why", "how", "all", "any", "both",
            "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not",
            "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will",
            "just", "don", "should", "now"
    ));

    public static String stemWord(String word) {
        PorterStemmer stemmer = new PorterStemmer();
        char[] chars = word.toCharArray();
        stemmer.add(chars, chars.length);
        stemmer.stem();
        return stemmer.toString();
    }

    private static final int MAX_WORD_LEN = 40;

    // Keep only the [a-z0-9] characters. Drops box-drawing/ASCII-art chars, HTML-entity
    // remnants, unicode punctuation, etc. — the kind of junk a real crawl is full of, and
    // which (left alone) produces "words" that even break the KVS row-key encoding.
    private static String sanitize(String w) {
        StringBuilder sb = new StringBuilder(w.length());
        for (int i = 0; i < w.length(); i++) {
            char c = w.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) sb.append(c);
            if (sb.length() > MAX_WORD_LEN) return "";   // junk blob, not a word
        }
        return sb.toString();
    }

    // Append position `pos` to the running lists for `word` and its stem in `wordPos`,
    // respecting the per-page word budget and per-word position budget.
    private static void addPos(Map<String, StringBuilder> wordPos, String word, int pos) {
        word = sanitize(word);
        if (word.length() < 2 || STOP_WORDS.contains(word)) return;
        addOne(wordPos, word, pos);
        String stemmed = stemWord(word);
        if (!stemmed.equals(word) && stemmed.length() >= 2 && !STOP_WORDS.contains(stemmed)) {
            addOne(wordPos, stemmed, pos);
        }
    }

    private static void addOne(Map<String, StringBuilder> wordPos, String w, int pos) {
        StringBuilder sb = wordPos.get(w);
        if (sb == null) {
            if (wordPos.size() >= MAX_WORDS_PER_PAGE) return;   // page word budget exhausted
            sb = new StringBuilder();
            wordPos.put(w, sb);
        }
        if (sb.length() > MAX_POS_CHARS_PER_WORD) return;
        if (sb.length() > 0) sb.append(' ');
        sb.append(pos);
    }

    public static void run(FlameContext context, String[] args) {
        try {
            // One scan of pt-crawl: pack (url, page) into a single hash-keyed value so the
            // downstream operation is spread evenly across all KVS/Flame workers.
            // (fromTable already drops null results, so no filter pass is needed.)
            FlameRDD data = context.fromTable("pt-crawl", row -> {
                String u = row.get("url");
                String p = row.get("page");
                if (u == null || p == null) return null;
                if (p.length() > MAX_PAGE_CHARS) p = p.substring(0, MAX_PAGE_CHARS);
                return u + STX + p;
            });

            int totalDocs = data.count();
            if (totalDocs <= 0) totalDocs = 1;

            // Build the inverted index. Each pair is (hash(word), "word<STX>url:pos1 pos2").
            // Keying by hash(word) — not the raw word — spreads writes evenly across the KVS
            // workers (raw words cluster near a single worker under consistent hashing).
            FlamePairRDD invertedIndex = data.flatMapToPair(packed -> {
                int sep = packed.indexOf(STX);
                if (sep < 0) return new ArrayList<>();
                String url = packed.substring(0, sep);
                String page = packed.substring(sep + 1);

                page = page.toLowerCase();
                if (page.length() > MAX_PAGE_CHARS) page = page.substring(0, MAX_PAGE_CHARS);
                // drop script/style blocks (and their contents), comments, then all tags
                page = page.replaceAll("(?s)<script[^>]*>.*?</script>", " ");
                page = page.replaceAll("(?s)<style[^>]*>.*?</style>", " ");
                page = page.replaceAll("(?s)<!--.*?-->", " ");
                page = page.replaceAll("<[^>]*>", " ");
                page = page.replaceAll("[,.:;?!'\"()\\-\\t\\n\\r]", " ");

                Map<String, StringBuilder> wordPos = new HashMap<>();
                String[] words = page.split("\\s+");
                for (int i = 0; i < words.length && wordPos.size() < MAX_WORDS_PER_PAGE; ++i) {
                    addPos(wordPos, words[i], i + 1);
                }

                List<FlamePair> pairs = new ArrayList<>(wordPos.size());
                for (Map.Entry<String, StringBuilder> e : wordPos.entrySet()) {
                    String w = e.getKey();
                    pairs.add(new FlamePair(Hasher.hash(w), w + STX + url + ":" + e.getValue()));
                }
                return pairs;
            });

            // Collapse all per-page entries for a word-hash into one SOH-joined string. The
            // MAX_FOLDED_CHARS guard stops a high-frequency word from blowing up into a multi-MB
            // string built by O(n^2) concatenation (which wedged a KVS worker on the 48K corpus).
            FlamePairRDD folded = invertedIndex.foldByKey("", (a, b) -> {
                if (a.isEmpty()) return b;
                if (b.isEmpty()) return a;
                if (a.length() >= MAX_FOLDED_CHARS) return a;
                return a + SOH + b;
            });

            final int totalDocsF = totalDocs;
            // Final pass: regroup by word, format each word's posting list, and write it
            // straight to pt-index under column "acc" (the column the Frontend reads).
            folded.flatMap(p -> {
                String[] entries = p._2().split(SOH);

                // word -> (url -> positions)   (a row key is one hash, normally one word)
                Map<String, Map<String, List<Integer>>> byWord = new HashMap<>();
                for (String entry : entries) {
                    if (entry.isEmpty()) continue;
                    int ws = entry.indexOf(STX);
                    if (ws < 0) continue;
                    String word = entry.substring(0, ws);
                    String urlPos = entry.substring(ws + 1);
                    int lastColon = urlPos.lastIndexOf(':');
                    if (lastColon < 0) continue;
                    String url = urlPos.substring(0, lastColon);
                    Map<String, List<Integer>> urlMap = byWord.computeIfAbsent(word, k -> new HashMap<>());
                    List<Integer> list = urlMap.computeIfAbsent(url, k -> new ArrayList<>());
                    for (String ps : urlPos.substring(lastColon + 1).split("\\s+")) {
                        if (ps.isEmpty()) continue;
                        try { list.add(Integer.parseInt(ps)); } catch (NumberFormatException ignored) {}
                    }
                }

                KVSClient kvs = context.getKVS();
                for (Map.Entry<String, Map<String, List<Integer>>> we : byWord.entrySet()) {
                    String word = we.getKey();
                    Map<String, List<Integer>> urlPos = we.getValue();
                    if (urlPos.isEmpty()) continue;
                    int df = urlPos.size();
                    double idf = Math.log((double) totalDocsF / df);

                    List<Map.Entry<String, List<Integer>>> es = new ArrayList<>(urlPos.entrySet());
                    es.sort((x, y) -> y.getValue().size() - x.getValue().size());

                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<String, List<Integer>> e : es) {
                        if (sb.length() > 0) sb.append(SOH);
                        List<Integer> positions = e.getValue();
                        positions.sort(null);
                        sb.append(e.getKey()).append(':').append(positions.size())
                          .append(':').append(String.format(java.util.Locale.US, "%.4f", idf)).append(':');
                        for (int i = 0; i < positions.size(); ++i) {
                            if (i > 0) sb.append(' ');
                            sb.append(positions.get(i));
                        }
                    }
                    kvs.put("pt-index", word, "acc", sb.toString());
                }
                return new ArrayList<String>();
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}