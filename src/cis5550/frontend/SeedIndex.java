package cis5550.frontend;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import java.util.*;

/**
 * Builds a local pt-index and pt-pageranks from existing pt-crawl data.
 * Run against a local KVS to enable frontend testing without the full indexer.
 *
 * Usage: java -cp "frontend-local.jar:lib/kvs.jar:lib/webserver.jar" \
 *             cis5550.frontend.SeedIndex [kvs-host:port] [max-pages]
 */
public class SeedIndex {

    static final Set<String> STOP = new HashSet<>(Arrays.asList(
        "a","an","the","is","it","in","on","of","and","or","to","for","with","at","by",
        "from","as","be","was","are","this","that","but","not","do","does","did","will",
        "would","could","should","can","may","might","shall","has","have","had","been",
        "being","its","his","her","he","she","they","them","their","we","our","you",
        "your","i","me","my","so","if","no","up","out","all","just","about","into",
        "over","after","than","then","also","how","what","when","where","which","who"
    ));

    public static void main(String[] args) throws Exception {
        String kvsAddr  = args.length > 0 ? args[0] : "localhost:8000";
        int    maxPages = args.length > 1 ? Integer.parseInt(args[1]) : 500;

        KVSClient kvs = new KVSClient(kvsAddr);
        System.out.println("Connecting to KVS at " + kvsAddr);
        System.out.println("Scanning pt-crawl (max " + maxPages + " pages)...");

        Iterator<Row> scan = kvs.scan("pt-crawl");

        // word -> url -> list of positions
        Map<String, Map<String, List<Integer>>> index = new LinkedHashMap<>();
        List<String> allUrls = new ArrayList<>();
        int pageCount = 0;

        while (scan.hasNext() && pageCount < maxPages) {
            Row row = scan.next();
            String url  = row.get("url");
            byte[] body = row.getBytes("page");
            if (url == null || body == null) continue;

            allUrls.add(url);
            String text = new String(body);
            text = text.replaceAll("(?s)<script[^>]*>.*?</script>", " ");
            text = text.replaceAll("(?s)<style[^>]*>.*?</style>", " ");
            text = text.replaceAll("<[^>]*>", " ");
            text = text.replaceAll("[^a-zA-Z0-9\\s]", " ").toLowerCase();

            String[] words = text.split("\\s+");
            for (int i = 0; i < words.length; i++) {
                String w = words[i];
                if (w.length() < 3 || w.length() > 25) continue;
                if (!w.matches("[a-z][a-z0-9]*"))       continue;
                if (STOP.contains(w))                   continue;
                index.computeIfAbsent(w, k -> new HashMap<>())
                     .computeIfAbsent(url, k -> new ArrayList<>())
                     .add(i + 1);
            }

            pageCount++;
            if (pageCount % 50 == 0)
                System.out.println("  Processed " + pageCount + " pages, " + index.size() + " unique words...");
        }

        int totalDocs = pageCount;
        System.out.println("Done scanning: " + pageCount + " pages, " + index.size() + " words.");
        System.out.println("Writing pt-index...");

        int written = 0;
        for (Map.Entry<String, Map<String, List<Integer>>> e : index.entrySet()) {
            String word = e.getKey();
            Map<String, List<Integer>> urlMap = e.getValue();
            int df = urlMap.size();
            double idf = Math.log((double) totalDocs / Math.max(df, 1));

            // sort by TF descending
            List<Map.Entry<String, List<Integer>>> entries = new ArrayList<>(urlMap.entrySet());
            entries.sort((a, b) -> b.getValue().size() - a.getValue().size());

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, List<Integer>> ue : entries) {
                if (sb.length() > 0) sb.append("");
                int tf = ue.getValue().size();
                sb.append(ue.getKey())
                  .append(":").append(tf)
                  .append(":").append(String.format(java.util.Locale.US, "%.4f", idf))
                  .append(":");
                List<Integer> pos = ue.getValue();
                Collections.sort(pos);
                for (int i = 0; i < Math.min(pos.size(), 30); i++) {
                    if (i > 0) sb.append(" ");
                    sb.append(pos.get(i));
                }
            }
            kvs.put("pt-index", word, "acc", sb.toString());
            written++;
            if (written % 2000 == 0)
                System.out.println("  Written " + written + "/" + index.size() + " words...");
        }

        System.out.println("Writing pt-pageranks (uniform 1.0 for all pages)...");
        for (String url : allUrls) {
            kvs.put("pt-pageranks", cis5550.tools.Hasher.hash(url), "rank", "1.0");
        }

        System.out.println("Seeding complete!");
        System.out.println("  pt-index:     " + written + " words");
        System.out.println("  pt-pageranks: " + allUrls.size() + " URLs");
        System.out.println("Now restart the frontend (or it's already running) and try a search.");
    }
}
