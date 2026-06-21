package cis5550.frontend;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.webserver.Server;
import cis5550.external.PorterStemmer;

import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.nio.file.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;

public class Frontend {

    static KVSClient kvs;
    static int totalDocs = 1;

    static final Set<String> STOP = new HashSet<>(Arrays.asList(
        "a","an","the","is","it","in","on","of","and","or","to","for",
        "with","at","by","from","as","be","was","are","this","that",
        "but","not","do","does","did","will","would","could","should",
        "can","may","might","shall","has","have","had","been","being",
        "its","his","her","he","she","they","them","their","we","our",
        "you","your","i","me","my","so","if","no","up","out","all",
        "just","about","into","over","after","than","then","also",
        "how","what","when","where","which","who","whom","why"
    ));

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: Frontend <port> <kvsCoordinator>");
            System.out.println("  e.g. Frontend 80 localhost:8000");
            return;
        }

        int port = Integer.parseInt(args[0]);
        String kvsAddr = args[1];
        kvs = new KVSClient(kvsAddr);

        try {
            ExecutorService tmp = Executors.newSingleThreadExecutor();
            int cnt = tmp.submit(() -> kvs.count("pt-crawl")).get(5, TimeUnit.SECONDS);
            tmp.shutdown();
            if (cnt > 0) totalDocs = cnt;
            System.out.println("Total docs in pt-crawl: " + totalDocs);
        } catch (Exception e) {
            System.out.println("Could not count pt-crawl, defaulting to 1");
        }

        Server.port(port);
        Server.staticFiles.location("static");

        Server.get("/", (req, res) -> {
            res.type("text/html");
            return loadFile("src/cis5550/frontend/home.html");
        });

        Server.get("/index.html", (req, res) -> {
            res.type("text/html");
            return loadFile("src/cis5550/frontend/home.html");
        });

        Server.get("/styles.css", (req, res) -> {
            res.type("text/css");
            return loadFile("src/cis5550/frontend/styles.css");
        });

        Server.get("/app.js", (req, res) -> {
            res.type("application/javascript");
            return loadFile("src/cis5550/frontend/app.js");
        });

        Server.get("/search", (req, res) -> {
            String q = req.queryParams("q");
            if (q == null || q.trim().isEmpty()) {
                res.type("text/html");
                return loadFile("src/cis5550/frontend/home.html");
            }
            q = q.trim();
            long start = System.currentTimeMillis();
            SearchResult sr = doSearch(q, 0, 100);
            String instantCard = instantAnswerCard(q, req.ip());
            String fromCity = null;
            try { fromCity = cityForIp(req.ip()); } catch (Exception e) { /* skip */ }
            long elapsed = System.currentTimeMillis() - start;
            res.type("text/html");
            return buildResultsPage(q, sr.results, sr.totalMatches, sr.didYouMean, instantCard, fromCity, elapsed);
        });

        // Image search: run a normal text search, then harvest <img> tags from the top pages.
        Server.get("/images", (req, res) -> {
            String q = req.queryParams("q");
            res.type("text/html");
            if (q == null || q.trim().isEmpty()) return loadFile("src/cis5550/frontend/home.html");
            q = q.trim();
            long start = System.currentTimeMillis();
            SearchResult sr = doSearch(q, 0, 30);
            List<String> qWords = new ArrayList<>(), qPhrases = new ArrayList<>();
            parseQuery(q, qPhrases, qWords);
            List<ImageHit> imgs = collectImages(sr.results, 60, qWords);
            long elapsed = System.currentTimeMillis() - start;
            return buildImagesPage(q, imgs, elapsed);
        });

        // Returns just the HTML for results 100..end — fetched asynchronously by the client
        // after the initial /search page renders, so the user sees the top 100 immediately
        // and the rest stream in below without blocking the first paint.
        Server.get("/search-more", (req, res) -> {
            String q = req.queryParams("q");
            res.type("text/html");
            if (q == null || q.trim().isEmpty()) return "";
            SearchResult sr = doSearch(q.trim(), 100, Integer.MAX_VALUE);
            return renderResultDivs(sr.results);
        });

        Server.get("/api/search", (req, res) -> {
            String q = req.queryParams("q");
            if (q == null || q.trim().isEmpty()) {
                res.type("application/json");
                return "{\"results\":[]}";
            }
            SearchResult sr = doSearch(q.trim(), 0, Integer.MAX_VALUE);
            res.type("application/json");
            return toJson(sr.results);
        });

        Server.get("/api/diag", (req, res) -> {
            res.type("text/html");
            String word = req.queryParams("q");
            if (word == null || word.isEmpty()) word = "university";
            StringBuilder sb = new StringBuilder(
                "<html><body style='font-family:monospace;background:#0E1220;color:#E8ECF5;padding:20px'>");
            sb.append("<h2>KVS Diagnostic — word: <em style='color:#7CC6FF'>").append(esc(word)).append("</em></h2>");
            sb.append("<h3>getRow result:</h3>");
            try {
                Row row = kvs.getRow("pt-index", word);
                if (row == null) {
                    sb.append("<p style='color:tomato'>getRow returned null (word not found in pt-index)</p>");
                } else {
                    sb.append("<p style='color:lightgreen'>Row found! key=").append(esc(row.key())).append("</p>");
                    for (String col : row.columns()) {
                        String val = row.get(col);
                        int len = val == null ? 0 : val.length();
                        String preview = val == null ? "null" : val.substring(0, Math.min(300, len));
                        sb.append("<p>col=").append(esc(col)).append(" len=").append(len).append("</p>");
                        sb.append("<pre style='color:#B6BECE;white-space:pre-wrap;word-break:break-all'>")
                          .append(esc(preview)).append("</pre>");
                    }
                }
            } catch (Exception e) {
                sb.append("<p style='color:tomato'>getRow exception: ").append(esc(e.toString())).append("</p>");
            }
            sb.append("<h3>Scan (prefix match):</h3>");
            try {
                Iterator<Row> rows = kvs.scan("pt-index", word, word + "~");
                int count = 0;
                while (rows != null && rows.hasNext() && count < 5) {
                    Row row = rows.next();
                    sb.append("<p>key=<strong>").append(esc(row.key())).append("</strong> cols=")
                      .append(esc(row.columns().toString())).append("</p>");
                    count++;
                }
                if (count == 0) sb.append("<p style='color:tomato'>No rows found in scan</p>");
                else sb.append("<p>Found ").append(count).append("+ rows in scan</p>");
            } catch (Exception e) {
                sb.append("<p style='color:tomato'>Scan exception: ").append(esc(e.toString())).append("</p>");
            }
            sb.append("</body></html>");
            return sb.toString();
        });

        Server.get("/cache", (req, res) -> {
            String url = req.queryParams("url");
            if (url == null || url.trim().isEmpty()) {
                res.type("text/html");
                return "<html><body style='font-family:monospace;padding:20px;background:#0E1220;color:#E8ECF5'>" +
                       "<h2>Missing url parameter</h2></body></html>";
            }
            try {
                byte[] pageBytes = kvs.get("pt-crawl", Hasher.hash(url.trim()), "page");
                if (pageBytes == null) {
                    res.type("text/html");
                    return "<html><body style='font-family:monospace;padding:20px;background:#0E1220;color:#E8ECF5'>" +
                           "<h2>Page not in cache</h2><p style='color:#8792A6'>" + esc(url) + "</p>" +
                           "<p><a href='javascript:history.back()' style='color:#7CC6FF'>← Back</a></p></body></html>";
                }
                String html = new String(pageBytes);
                String banner = "<div style='position:fixed;top:0;left:0;right:0;z-index:99999;" +
                        "background:#0E1220;color:#E8ECF5;font-family:monospace;font-size:12px;" +
                        "padding:8px 16px;border-bottom:1px solid #2E3547;" +
                        "display:flex;align-items:center;gap:16px;'>" +
                        "<span style='color:#7CC6FF;flex-shrink:0'>CEREBRO CACHE</span>" +
                        "<span style='color:#5A6578'>|</span>" +
                        "<span style='color:#8792A6;overflow:hidden;text-overflow:ellipsis;" +
                        "white-space:nowrap;flex:1;min-width:0'>" + esc(url) + "</span>" +
                        "<a href='javascript:history.back()' style='margin-left:auto;color:#7CC6FF;" +
                        "text-decoration:none;flex-shrink:0'>← Back</a></div>" +
                        "<div style='height:42px'></div>";
                String lower = html.toLowerCase();
                int bodyIdx = lower.indexOf("<body");
                if (bodyIdx >= 0) {
                    int bodyEnd = html.indexOf('>', bodyIdx);
                    html = bodyEnd >= 0
                        ? html.substring(0, bodyEnd + 1) + banner + html.substring(bodyEnd + 1)
                        : banner + html;
                } else {
                    html = banner + html;
                }
                res.type("text/html");
                return html;
            } catch (Exception e) {
                res.type("text/html");
                return "<html><body style='padding:20px'>Error: " + esc(e.getMessage()) + "</body></html>";
            }
        });

        Server.get("/api/suggest", (req, res) -> {
            res.type("application/json");
            String q = req.queryParams("q");
            if (q == null || q.trim().length() < 2) return "[]";
            String prefix = q.trim().toLowerCase().replaceAll("[^a-z0-9]", "");
            if (prefix.length() < 2) return "[]";
            List<String> suggestions = new ArrayList<>();
            try {
                Iterator<Row> rows = kvs.scan("pt-index", prefix, prefix + "~");
                int count = 0;
                while (rows != null && rows.hasNext() && count < 8) {
                    Row row = rows.next();
                    String key = row.key();
                    if (key.startsWith(prefix)) { suggestions.add(key); count++; }
                    else break;
                }
            } catch (Exception e) { /* pt-index may not exist yet */ }
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < suggestions.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("\"").append(jsonEsc(suggestions.get(i))).append("\"");
            }
            sb.append("]");
            return sb.toString();
        });

        Server.get("/debug", (req, res) -> {
            String q = req.queryParams("q");
            if (q == null || q.trim().isEmpty()) {
                res.type("text/html");
                return "<html><body style='font-family:monospace;padding:20px;background:#0E1220;color:#E8ECF5'>" +
                       "<h2>Cerebro Debug</h2><form action='/debug'>" +
                       "<input name='q' style='padding:8px;width:400px;background:#141A2B;color:#E8ECF5;border:1px solid #2E3547'/>" +
                       "<button style='padding:8px 16px;margin-left:8px'>Search</button></form></body></html>";
            }
            SearchResult sr = doSearch(q.trim(), 0, Integer.MAX_VALUE);
            res.type("text/html");
            return buildDebugPage(q.trim(), sr.results);
        });

        System.out.println("Cerebro frontend started on port " + port);
        System.out.println("KVS coordinator: " + kvsAddr);
    }

    // ========== SEARCH LOGIC ==========

    static SearchResult doSearch(String query, int fromIdx, int toIdx) {
        List<String> phrases = new ArrayList<>();
        List<String> words = new ArrayList<>();
        parseQuery(query, phrases, words);

        Set<String> lookupSet = new LinkedHashSet<>();
        for (String w : words) {
            if (STOP.contains(w)) continue;
            lookupSet.add(w);
            String s = stem(w);
            if (!s.isEmpty() && !s.equals(w)) lookupSet.add(s);
        }
        List<String> lookupTerms = new ArrayList<>(lookupSet);
        if (lookupTerms.isEmpty()) return emptySearchResult();

        // parallel KVS lookups for all terms
        Map<String, Map<String, DocEntry>> termDocs = new ConcurrentHashMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(lookupTerms.size(), 10));
        List<Future<?>> futures = new ArrayList<>();

        for (String term : lookupTerms) {
            futures.add(pool.submit(() -> {
                try {
                    Row row = kvs.getRow("pt-index", term);
                    if (row == null) return;
                    String val = row.get("acc");
                    if (val == null || val.isEmpty()) return;
                    Map<String, DocEntry> docs = parseIndexEntry(val);
                    termDocs.put(term, docs);
                } catch (Exception e) { /* skip */ }
            }));
        }
        for (Future<?> f : futures) {
            try { f.get(5, TimeUnit.SECONDS); } catch (Exception e) { }
        }
        pool.shutdown();

        // score each URL
        Map<String, Double> tfidfScores = new HashMap<>();
        Map<String, Double> prScores = new ConcurrentHashMap<>();
        Map<String, Integer> termHits = new HashMap<>();

        for (String term : lookupTerms) {
            Map<String, DocEntry> docs = termDocs.get(term);
            if (docs == null) continue;
            for (Map.Entry<String, DocEntry> e : docs.entrySet()) {
                String url = e.getKey();
                DocEntry de = e.getValue();
                double tfidf = de.tf * de.idf;
                tfidfScores.merge(url, tfidf, Double::sum);
                termHits.merge(url, 1, Integer::sum);
            }
        }

        if (tfidfScores.isEmpty()) {
            SearchResult sr = emptySearchResult();
            // No documents matched any term — try a "Did you mean?" suggestion for the first
            // non-stopword query term. Only useful when the user's query has a typo.
            for (String w : words) {
                if (w == null || w.length() < 4 || STOP.contains(w)) continue;
                String s = didYouMean(w);
                if (s != null && !s.equals(w)) { sr.didYouMean = s; break; }
            }
            return sr;
        }

        // parallel PageRank lookups
        Set<String> urlsToScore = tfidfScores.keySet();
        ExecutorService prPool = Executors.newFixedThreadPool(Math.min(urlsToScore.size(), 10));
        List<Future<?>> prFutures = new ArrayList<>();
        for (String url : urlsToScore) {
            prFutures.add(prPool.submit(() -> {
                try {
                    byte[] prBytes = kvs.get("pt-pageranks", Hasher.hash(url), "rank");
                    if (prBytes != null) {
                        prScores.put(url, Double.parseDouble(new String(prBytes)));
                    }
                } catch (Exception e) { /* skip */ }
            }));
        }
        for (Future<?> f : prFutures) {
            try { f.get(3, TimeUnit.SECONDS); } catch (Exception e) { }
        }
        prPool.shutdown();

        // combine TF-IDF + PageRank scores
        Map<String, Double> finalScores = new HashMap<>();
        int numTerms = lookupTerms.size();

        for (Map.Entry<String, Double> e : tfidfScores.entrySet()) {
            String url = e.getKey();
            double tfidf = e.getValue();
            double pr = prScores.getOrDefault(url, 0.0);
            int hits = termHits.getOrDefault(url, 0);
            double coverage = (numTerms > 1) ? (double) hits / numTerms : 1.0;
            double prBoost = (pr > 0) ? Math.log(1 + pr) * 0.5 : 0;
            double score = tfidf * (0.5 + coverage) + prBoost;

            for (String phrase : phrases) {
                if (hasPhrase(phrase, url, termDocs)) score *= 3.0;
            }
            for (String w : words) {
                if (!STOP.contains(w) && url.toLowerCase().contains(w)) {
                    score *= 1.3;
                    break;
                }
            }
            finalScores.put(url, score);
        }

        List<Map.Entry<String, Double>> sorted = new ArrayList<>(finalScores.entrySet());
        sorted.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        int total = sorted.size();
        int from = Math.max(0, fromIdx);
        int to = Math.min(toIdx, total);
        int slice = Math.max(0, to - from);
        ExecutorService metaPool = Executors.newFixedThreadPool(Math.max(1, Math.min(slice, 10)));
        List<Future<Result>> metaFutures = new ArrayList<>();

        for (int i = from; i < to; i++) {
            String url = sorted.get(i).getKey();
            double sc = sorted.get(i).getValue();
            double pr = prScores.getOrDefault(url, 0.0);
            int hits = termHits.getOrDefault(url, 0);
            final int rank = i + 1;

            metaFutures.add(metaPool.submit(() -> {
                String title = url;
                String snippet = "";
                boolean hasContent = false;
                try {
                    byte[] pageBytes = kvs.get("pt-crawl", Hasher.hash(url), "page");
                    if (pageBytes != null) {
                        String page = new String(pageBytes);
                        if (page.contains("<")) {
                            String t = extractTitle(page);
                            if (t != null && !t.isEmpty()) title = t;
                            snippet = extractSnippet(page, words);
                            hasContent = true;
                        }
                    }
                } catch (Exception e) { /* skip */ }

                Result r = new Result();
                r.url = url;
                r.title = title;
                r.snippet = snippet;
                r.titleHtml = highlightHtml(esc(title), words);
                r.snippetHtml = highlightHtml(esc(snippet), words);
                r.score = sc;
                r.pr = pr;
                r.termHits = hits;
                r.totalTerms = numTerms;
                r.rank = rank;
                r.hasContent = hasContent;
                return r;
            }));
        }

        List<Result> results = new ArrayList<>();
        for (Future<Result> f : metaFutures) {
            try { results.add(f.get(3, TimeUnit.SECONDS)); } catch (Exception e) { }
        }
        metaPool.shutdown();

        results.sort((a, b) -> Double.compare(b.score, a.score));
        SearchResult sr = new SearchResult();
        sr.results = results;
        sr.totalMatches = total;
        return sr;
    }

    static SearchResult emptySearchResult() {
        SearchResult sr = new SearchResult();
        sr.results = Collections.emptyList();
        sr.totalMatches = 0;
        return sr;
    }

    // ========== INDEX PARSING ==========

    // Format: url:tf:idf:pos1 pos2,...,url2:tf:idf:...
    static Map<String, DocEntry> parseIndexEntry(String val) {
        Map<String, DocEntry> docs = new HashMap<>();
        String[] entries = splitIndexEntries(val);
        for (String entry : entries) {
            entry = entry.trim();
            if (entry.isEmpty()) continue;
            try {
                // parse from right: positions, idf, tf, then url
                int lastColon = entry.lastIndexOf(':');
                if (lastColon < 0) continue;
                int secondLast = entry.lastIndexOf(':', lastColon - 1);
                if (secondLast < 0) continue;
                int thirdLast = entry.lastIndexOf(':', secondLast - 1);
                if (thirdLast < 0) continue;

                String posStr = entry.substring(lastColon + 1).trim();
                String idfStr = entry.substring(secondLast + 1, lastColon).trim();
                String tfStr = entry.substring(thirdLast + 1, secondLast).trim();
                String url = entry.substring(0, thirdLast);
                if (url.isEmpty()) continue;

                DocEntry de = new DocEntry();
                de.url = url;
                de.tf = Integer.parseInt(tfStr.trim());
                de.idf = Double.parseDouble(idfStr.trim().replace(',', '.'));
                de.positions = new ArrayList<>();
                if (!posStr.isEmpty()) {
                    for (String p : posStr.split("\\s+")) {
                        try { de.positions.add(Integer.parseInt(p)); } catch (Exception ex) { }
                    }
                }
                docs.put(url, de);
            } catch (Exception e) { /* skip malformed */ }
        }
        return docs;
    }

    // Entries are joined by  (SOH) by the Flame indexer's foldByKey accumulator
    static String[] splitIndexEntries(String val) {
        return val.split("", -1);
    }

    // ========== PHRASE SEARCH ==========

    static boolean hasPhrase(String phrase, String url, Map<String, Map<String, DocEntry>> termDocs) {
        String[] pWords = phrase.toLowerCase().split("\\s+");
        if (pWords.length < 2) return false;

        List<List<Integer>> posLists = new ArrayList<>();
        for (String w : pWords) {
            if (STOP.contains(w)) continue;
            String s = stem(w);
            Map<String, DocEntry> docs = termDocs.get(s);
            if (docs == null) docs = termDocs.get(w);
            if (docs == null) return false;
            DocEntry de = docs.get(url);
            if (de == null) return false;
            posLists.add(de.positions);
        }
        if (posLists.size() < 2) return false;

        for (int startPos : posLists.get(0)) {
            boolean match = true;
            for (int i = 1; i < posLists.size(); i++) {
                if (!posLists.get(i).contains(startPos + i)) { match = false; break; }
            }
            if (match) return true;
        }
        return false;
    }

    // ========== HELPERS ==========

    static String stem(String word) {
        PorterStemmer st = new PorterStemmer();
        word = word.toLowerCase().replaceAll("[^a-z0-9]", "");
        if (word.isEmpty()) return "";
        st.add(word.toCharArray(), word.length());
        st.stem();
        return st.toString();
    }

    static void parseQuery(String query, List<String> phrases, List<String> words) {
        int i = 0;
        while (i < query.length()) {
            if (query.charAt(i) == '"') {
                int end = query.indexOf('"', i + 1);
                if (end > i) {
                    String phrase = query.substring(i + 1, end).trim();
                    if (!phrase.isEmpty()) {
                        phrases.add(phrase);
                        for (String w : phrase.toLowerCase().split("\\s+")) {
                            if (!w.isEmpty()) words.add(w);
                        }
                    }
                    i = end + 1;
                    continue;
                }
            }
            int end = i;
            while (end < query.length() && query.charAt(end) != ' ' && query.charAt(end) != '"') end++;
            String w = query.substring(i, end).toLowerCase().trim();
            if (!w.isEmpty()) words.add(w);
            i = end + 1;
        }
    }

    static String extractTitle(String html) {
        String lower = html.toLowerCase();
        int start = lower.indexOf("<title");
        if (start < 0) return null;
        int gt = html.indexOf('>', start);
        if (gt < 0) return null;
        int end = lower.indexOf("</title>", gt);
        if (end < 0) return null;
        String title = html.substring(gt + 1, end).replaceAll("<[^>]*>", "").trim();
        if (title.length() > 200) title = title.substring(0, 200);
        return title;
    }

    static String extractSnippet(String page, List<String> queryWords) {
        String text = page.replaceAll("(?s)<script[^>]*>.*?</script>", " ");
        text = text.replaceAll("(?s)<style[^>]*>.*?</style>", " ");
        text = text.replaceAll("<[^>]*>", " ");
        text = text.replaceAll("&[a-zA-Z]+;", " ").replaceAll("&#[0-9]+;", " ");
        text = text.replaceAll("\\s+", " ").trim();
        if (text.length() < 10) return "";

        String lower = text.toLowerCase();
        int bestIdx = -1;
        for (String w : queryWords) {
            if (STOP.contains(w)) continue;
            int idx = lower.indexOf(w.toLowerCase());
            if (idx >= 0) { bestIdx = idx; break; }
        }

        if (bestIdx < 0) return text.substring(0, Math.min(text.length(), 200)) + "...";

        int start = Math.max(0, bestIdx - 60);
        int end = Math.min(text.length(), bestIdx + 160);
        if (start > 0) {
            int sp = text.indexOf(' ', start);
            if (sp > 0 && sp < bestIdx) start = sp + 1;
        }
        String snippet = text.substring(start, end);
        if (start > 0) snippet = "..." + snippet;
        if (end < text.length()) snippet = snippet + "...";
        return snippet;
    }

    static String loadFile(String path) {
        try {
            return new String(Files.readAllBytes(Paths.get(path)));
        } catch (Exception e) {
            return "<html><body>File not found: " + path + "</body></html>";
        }
    }

    // ========== DATA CLASSES ==========

    static class DocEntry {
        String url;
        int tf;
        double idf;
        List<Integer> positions = new ArrayList<>();
    }

    // Wraps a search response: the materialized slice of results plus the TRUE total number
    // of matching documents (so the page can render "N of M shown" and the client can decide
    // whether to fetch the rest asynchronously) and an optional "Did you mean?" suggestion.
    static class SearchResult {
        List<Result> results;
        int totalMatches;
        String didYouMean;
    }

    static class ImageHit { String imgUrl; String pageUrl; String alt; int score; }

    // ========== HTML RENDERING ==========

    static String esc(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace("\"", "&quot;");
    }

    static String cleanUrl(String url) {
        return url.replaceAll(":80/", "/").replaceAll(":443/", "/")
                  .replaceAll(":80$", "").replaceAll(":443$", "");
    }

    static String displayDomain(String url) {
        try {
            String clean = cleanUrl(url).replaceAll("https?://", "");
            String[] parts = clean.split("/");
            StringBuilder sb = new StringBuilder(parts[0]);
            for (int i = 1; i < Math.min(parts.length, 4); i++) {
                String p = parts[i].trim();
                if (p.isEmpty()) continue;
                if (p.length() > 30) p = p.substring(0, 30) + "...";
                sb.append(" › ").append(p);
            }
            return sb.toString();
        } catch (Exception e) { return url; }
    }

    // Renders the URL breadcrumb with a favicon + bold domain + path crumbs (Google-style).
    // Returns ready-to-inject HTML (already escaped). The favicon image is fetched from
    // Google's public favicon service; if it fails the onerror handler hides it cleanly.
    static String renderDomainBar(String url) {
        try {
            String clean = cleanUrl(url).replaceAll("https?://", "");
            int slash = clean.indexOf('/');
            String host = (slash > 0) ? clean.substring(0, slash) : clean;
            String rest = (slash > 0) ? clean.substring(slash + 1) : "";
            StringBuilder sb = new StringBuilder();
            sb.append("<img class=\"fav\" src=\"https://www.google.com/s2/favicons?domain=")
              .append(urlEncode(host))
              .append("&sz=32\" width=\"16\" height=\"16\" alt=\"\" loading=\"lazy\" onerror=\"this.style.display='none'\">");
            sb.append("<strong class=\"dom\">").append(esc(host)).append("</strong>");
            if (!rest.isEmpty()) {
                String[] parts = rest.split("/");
                int count = 0;
                for (String p : parts) {
                    p = p.trim();
                    if (p.isEmpty()) continue;
                    if (count >= 3) break;
                    if (p.length() > 30) p = p.substring(0, 30) + "...";
                    sb.append(" <span class=\"sep\">›</span> ").append(esc(p));
                    count++;
                }
            }
            return sb.toString();
        } catch (Exception e) { return esc(url); }
    }

    static String toJson(List<Result> results) {
        StringBuilder sb = new StringBuilder("{\"results\":[");
        for (int i = 0; i < results.size(); i++) {
            Result r = results.get(i);
            if (i > 0) sb.append(",");
            sb.append("{\"url\":\"").append(jsonEsc(r.url))
              .append("\",\"title\":\"").append(jsonEsc(r.title))
              .append("\",\"snippet\":\"").append(jsonEsc(r.snippet))
              .append("\",\"score\":").append(r.score).append("}");
        }
        sb.append("]}");
        return sb.toString();
    }

    static String urlEncode(String s) {
        try { return java.net.URLEncoder.encode(s, "UTF-8"); }
        catch (Exception e) { return esc(s); }
    }

    static String jsonEsc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }

    // Wrap occurrences of any query word (case-insensitive, word-boundary) with <mark> tags
    // inside an already-HTML-escaped string. The input MUST already be escaped (so we don't
    // re-mangle the user-visible text); the <mark> we insert is the only raw HTML allowed.
    static String highlightHtml(String escapedText, List<String> queryWords) {
        if (escapedText == null || escapedText.isEmpty() || queryWords == null) return escapedText;
        String result = escapedText;
        Set<String> seen = new HashSet<>();
        for (String w : queryWords) {
            if (w == null || w.length() < 2) continue;
            String lw = w.toLowerCase();
            if (STOP.contains(lw) || !seen.add(lw)) continue;
            try {
                String pattern = "(?i)\\b(" + Pattern.quote(lw) + ")\\b";
                result = result.replaceAll(pattern, "<mark>$1</mark>");
            } catch (Exception e) { /* skip malformed term */ }
        }
        return result;
    }

    // Bounded Levenshtein: returns maxDist+1 the moment the running minimum exceeds maxDist
    // (so a 2-distance scan over ~tens of thousands of words finishes in ~50-100ms).
    static int levenshtein(String a, String b, int maxDist) {
        int la = a.length(), lb = b.length();
        if (Math.abs(la - lb) > maxDist) return maxDist + 1;
        int[] prev = new int[lb + 1], curr = new int[lb + 1];
        for (int j = 0; j <= lb; j++) prev[j] = j;
        for (int i = 1; i <= la; i++) {
            curr[0] = i;
            int rowMin = curr[0];
            for (int j = 1; j <= lb; j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                curr[j] = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
                if (curr[j] < rowMin) rowMin = curr[j];
            }
            if (rowMin > maxDist) return maxDist + 1;
            int[] tmp = prev; prev = curr; curr = tmp;
        }
        return prev[lb];
    }

    // For a query word that has no pt-index entry, scan the same-first-letter slice of pt-index
    // and return the closest existing word at edit distance ≤ 2 (Google-style "Did you mean?").
    // We only scan one letter's worth of words (~5-20K rows) — fast (<100ms) and good enough.
    static String didYouMean(String word) {
        if (word == null || word.length() < 4) return null;
        String w = word.toLowerCase();
        char fc = w.charAt(0);
        if (fc < 'a' || fc > 'z') return null;
        String startK = String.valueOf(fc);
        String endK = String.valueOf((char) (fc + 1));
        String best = null;
        int bestDist = Integer.MAX_VALUE;
        try {
            Iterator<Row> it = kvs.scan("pt-index", startK, endK);
            int scanned = 0;
            while (it.hasNext() && scanned < 20000) {
                Row row = it.next();
                String key = row.key();
                scanned++;
                if (key == null || key.equals(w)) continue;
                if (Math.abs(key.length() - w.length()) > 2) continue;
                int d = levenshtein(key, w, 2);
                if (d > 0 && d < bestDist) {
                    bestDist = d;
                    best = key;
                    if (d == 1) break;
                }
            }
        } catch (Exception e) { return null; }
        return (bestDist <= 2) ? best : null;
    }

    // ========== WEATHER (task-specific result + IP geolocation) ==========

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    static String httpGet(String url, int timeoutMs) {
        try {
            HttpRequest r = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofMillis(timeoutMs))
                    .header("User-Agent", "cis5550-cerebro")
                    .GET().build();
            HttpResponse<String> resp = HTTP_CLIENT.send(r, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) return resp.body();
        } catch (Exception e) { /* caller treats null as "no data" */ }
        return null;
    }

    static Double jsonNum(String json, String key) {
        if (json == null) return null;
        Matcher m = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?)").matcher(json);
        try { return m.find() ? Double.parseDouble(m.group(1)) : null; } catch (Exception e) { return null; }
    }
    static String jsonStr(String json, String key) {
        if (json == null) return null;
        Matcher m = Pattern.compile("\"" + Pattern.quote(key) + "\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"").matcher(json);
        return m.find() ? m.group(1) : null;
    }

    // Open-Meteo WMO weather codes -> [symbol, description].
    static String[] wmoDesc(int c) {
        switch (c) {
            case 0:  return new String[]{"☀", "Clear sky"};
            case 1:  return new String[]{"☀", "Mainly clear"};
            case 2:  return new String[]{"⛅", "Partly cloudy"};
            case 3:  return new String[]{"☁", "Overcast"};
            case 45: case 48: return new String[]{"☁", "Fog"};
            case 51: case 53: case 55: return new String[]{"☔", "Drizzle"};
            case 56: case 57: return new String[]{"☔", "Freezing drizzle"};
            case 61: case 63: case 65: return new String[]{"☔", "Rain"};
            case 66: case 67: return new String[]{"☔", "Freezing rain"};
            case 71: case 73: case 75: return new String[]{"❄", "Snow"};
            case 77: return new String[]{"❄", "Snow grains"};
            case 80: case 81: case 82: return new String[]{"☔", "Rain showers"};
            case 85: case 86: return new String[]{"❄", "Snow showers"};
            case 95: return new String[]{"⛈", "Thunderstorm"};
            case 96: case 99: return new String[]{"⛈", "Thunderstorm with hail"};
            default: return new String[]{"☁", "—"};
        }
    }

    // If `query` looks like a weather query, returns the place to look up. "" means "use the
    // client's IP location (or a sensible default)"; null means it's not a weather query.
    static String weatherQueryLocation(String query) {
        if (query == null) return null;
        String raw = query.trim();
        String q = raw.toLowerCase(Locale.ROOT);
        if (q.isEmpty()) return null;
        if (q.equals("weather") || q.equals("the weather") || q.equals("what's the weather")
                || q.equals("whats the weather") || q.equals("current weather") || q.equals("weather today")) return "";
        String[] prefixes = {"weather in ", "weather for ", "weather at ", "weather near ",
                "temperature in ", "temperature at ", "forecast in ", "forecast for ", "forecast at "};
        for (String p : prefixes) {
            if (q.startsWith(p)) {
                String loc = raw.substring(p.length()).trim();
                return loc.isEmpty() ? "" : loc;
            }
        }
        if (q.endsWith(" weather") && raw.length() <= 32)
            return raw.substring(0, raw.length() - " weather".length()).trim();
        if (q.endsWith(" forecast") && raw.length() <= 33)
            return raw.substring(0, raw.length() - " forecast".length()).trim();
        return null;
    }

    // Best-effort IP -> "lat,lon,city" via ip-api.com (free, no key, HTTP). Returns null on
    // private/loopback IPs or any failure.
    static String geolocateIp(String ip) {
        if (ip == null || ip.isEmpty()) return null;
        if (ip.startsWith("10.") || ip.startsWith("127.") || ip.startsWith("192.168.")
                || ip.startsWith("172.16.") || ip.startsWith("172.17.") || ip.startsWith("172.18.")
                || ip.startsWith("172.19.") || ip.startsWith("172.2") || ip.startsWith("172.3")
                || ip.startsWith("::1") || ip.equals("0:0:0:0:0:0:0:1")) return null;
        String j = httpGet("http://ip-api.com/json/" + urlEncode(ip) + "?fields=status,lat,lon,city", 2500);
        if (j == null || !"success".equals(jsonStr(j, "status"))) return null;
        Double lat = jsonNum(j, "lat"), lon = jsonNum(j, "lon");
        if (lat == null || lon == null) return null;
        String city = jsonStr(j, "city");
        return lat + "," + lon + "," + (city == null ? "" : city);
    }

    // Returns ready-to-inject HTML for a "weather card", or null if the query isn't weather-
    // related or the lookups fail. Calls Open-Meteo's free geocoding + forecast APIs.
    static String weatherCard(String query, String clientIp) {
        String loc = weatherQueryLocation(query);
        if (loc == null) return null;
        double lat, lon; String place;
        if (loc.isEmpty()) {
            String geo = geolocateIp(clientIp);
            if (geo != null) {
                String[] p = geo.split(",", 3);
                try { lat = Double.parseDouble(p[0]); lon = Double.parseDouble(p[1]); }
                catch (Exception e) { return null; }
                place = (p.length > 2 && !p[2].isEmpty()) ? p[2] : "Your location";
            } else { lat = 39.9526; lon = -75.1652; place = "Philadelphia"; }
        } else {
            String j = httpGet("https://geocoding-api.open-meteo.com/v1/search?count=1&language=en&format=json&name=" + urlEncode(loc), 3000);
            Double glat = jsonNum(j, "latitude"), glon = jsonNum(j, "longitude");
            if (glat == null || glon == null) return null;   // no such place
            lat = glat; lon = glon;
            String name = jsonStr(j, "name"), admin1 = jsonStr(j, "admin1"), country = jsonStr(j, "country");
            place = (name != null) ? name : loc;
            if (admin1 != null && !admin1.isEmpty() && !admin1.equalsIgnoreCase(name)) place += ", " + admin1;
            else if (country != null && !country.isEmpty()) place += ", " + country;
        }
        String w = httpGet("https://api.open-meteo.com/v1/forecast?timezone=auto&current=temperature_2m,apparent_temperature,relative_humidity_2m,weather_code,wind_speed_10m&latitude=" + lat + "&longitude=" + lon, 3000);
        Double temp = jsonNum(w, "temperature_2m");
        if (temp == null) return null;
        Double feels = jsonNum(w, "apparent_temperature"), hum = jsonNum(w, "relative_humidity_2m"), wind = jsonNum(w, "wind_speed_10m"), codeD = jsonNum(w, "weather_code");
        String[] cd = wmoDesc(codeD != null ? codeD.intValue() : -1);
        StringBuilder sb = new StringBuilder();
        sb.append("<div class=\"weather-card\">");
        sb.append("<div class=\"wx-icon\" aria-hidden=\"true\">").append(cd[0]).append("</div>");
        sb.append("<div class=\"wx-main\"><div class=\"wx-place\">").append(esc(place)).append("</div>");
        sb.append("<div class=\"wx-temp\">").append(Math.round(temp)).append("&deg;C</div>");
        sb.append("<div class=\"wx-cond\">").append(esc(cd[1]));
        if (feels != null) sb.append(" &middot; feels like ").append(Math.round(feels)).append("&deg;");
        sb.append("</div></div>");
        sb.append("<div class=\"wx-meta\">");
        if (hum != null) sb.append("<span>Humidity ").append(Math.round(hum)).append("%</span>");
        if (wind != null) sb.append("<span>Wind ").append(Math.round(wind)).append(" km/h</span>");
        sb.append("<span class=\"wx-src\">via open-meteo.com</span></div></div>");
        return sb.toString();
    }

    // ========== INSTANT ANSWERS: currency conversion + NBA ==========

    private static final Map<String, String> CURR = new HashMap<>();
    static {
        String[][] c = {
            {"usd","usd"},{"dollar","usd"},{"dollars","usd"},{"us$","usd"},{"bucks","usd"},
            {"eur","eur"},{"euro","eur"},{"euros","eur"},
            {"gbp","gbp"},{"pound","gbp"},{"pounds","gbp"},{"sterling","gbp"},
            {"jpy","jpy"},{"yen","jpy"},
            {"chf","chf"},{"franc","chf"},{"francs","chf"},
            {"cny","cny"},{"yuan","cny"},{"rmb","cny"},{"renminbi","cny"},
            {"inr","inr"},{"rupee","inr"},{"rupees","inr"},
            {"cad","cad"},{"aud","aud"},{"nzd","nzd"},
            {"mxn","mxn"},{"peso","mxn"},{"pesos","mxn"},
            {"brl","brl"},{"real","brl"},{"reais","brl"},
            {"krw","krw"},{"won","krw"},{"rub","rub"},{"ruble","rub"},{"rubles","rub"},
            {"try","try"},{"lira","try"},{"zar","zar"},{"rand","zar"},
            {"sek","sek"},{"nok","nok"},{"dkk","dkk"},{"pln","pln"},{"zloty","pln"},
            {"sgd","sgd"},{"hkd","hkd"},{"thb","thb"},{"baht","thb"},
            {"idr","idr"},{"php","php"},{"myr","myr"},{"ils","ils"},
            {"czk","czk"},{"huf","huf"},{"ron","ron"},{"isk","isk"},{"bgn","bgn"}
        };
        for (String[] e : c) CURR.put(e[0], e[1].toUpperCase());
    }

    static String fmtNum(double d) {
        if (d == Math.rint(d) && Math.abs(d) < 1e12) return String.format(Locale.US, "%,d", (long) d);
        if (Math.abs(d) >= 1) return String.format(Locale.US, "%,.2f", d);
        return String.format(Locale.US, "%,.4f", d);
    }

    // "100 usd to eur" / "convert 50 dollars to pounds" / "usd to eur" -> a conversion card.
    // Uses frankfurter.app (free, no key, ECB reference rates; ~30 fiat currencies).
    static String currencyCard(String query) {
        if (query == null) return null;
        String q = query.trim().toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
        if (q.startsWith("convert ")) q = q.substring(8).trim();
        if (q.startsWith("how much is ")) q = q.substring(12).trim();
        double amt = 1; String fromW, toW;
        Matcher m = Pattern.compile("^([0-9]+(?:[.,][0-9]+)?)\\s*([a-z$€£¥]{1,12})\\s+(?:to|in|into|=|->|→)\\s+([a-z$€£¥]{1,12})\\??$").matcher(q);
        if (m.matches()) {
            try { amt = Double.parseDouble(m.group(1).replace(",", ".")); } catch (Exception e) { return null; }
            fromW = m.group(2); toW = m.group(3);
        } else {
            Matcher m2 = Pattern.compile("^([a-z$€£¥]{1,12})\\s+(?:to|in|into|=|->|→)\\s+([a-z$€£¥]{1,12})\\??$").matcher(q);
            if (!m2.matches()) return null;
            fromW = m2.group(1); toW = m2.group(2);
        }
        // currency symbols
        if (fromW.equals("$")) fromW = "usd"; if (toW.equals("$")) toW = "usd";
        if (fromW.equals("€")) fromW = "eur"; if (toW.equals("€")) toW = "eur";
        if (fromW.equals("£")) fromW = "gbp"; if (toW.equals("£")) toW = "gbp";
        if (fromW.equals("¥")) fromW = "jpy"; if (toW.equals("¥")) toW = "jpy";
        String from = (fromW.length() == 3 && CURR.containsValue(fromW.toUpperCase())) ? fromW.toUpperCase() : CURR.get(fromW);
        String to = (toW.length() == 3 && CURR.containsValue(toW.toUpperCase())) ? toW.toUpperCase() : CURR.get(toW);
        if (from == null || to == null || from.equals(to)) return null;
        if (amt <= 0 || amt > 1e12) return null;
        String j = httpGet("https://api.frankfurter.app/latest?amount=" + amt + "&from=" + from + "&to=" + to, 3000);
        Double converted = jsonNum(j, to);
        if (converted == null) return null;
        String date = jsonStr(j, "date");
        StringBuilder sb = new StringBuilder();
        sb.append("<div class=\"answer-card\">");
        sb.append("<div class=\"ac-icon\" aria-hidden=\"true\">⇄</div>");
        sb.append("<div class=\"ac-body\"><div class=\"ac-title\">").append(fmtNum(amt)).append(" ").append(from)
          .append(" = <strong>").append(fmtNum(converted)).append(" ").append(to).append("</strong></div>");
        sb.append("<div class=\"ac-sub\">1 ").append(from).append(" = ").append(fmtNum(converted / amt)).append(" ").append(to);
        if (date != null) sb.append(" &middot; ").append(esc(date));
        sb.append(" &middot; <span class=\"ac-src\">via frankfurter.app (ECB)</span></div></div></div>");
        return sb.toString();
    }

    // Live NBA playoff bracket, reconstructed from ESPN's playoff-game feed: each game carries its
    // round ("notes":[{"headline":"East 1st Round - Game 1"}]) and its series state
    // ("series":{...,"summary":"CLE leads series 2-1",...}). We keep the latest summary per matchup
    // (games are in date order) and group by round.
    static String nbaBracketCard() {
        String j = httpGet("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?seasontype=3&dates=20260401-20260601", 5000);
        if (j == null) return null;
        List<String> names = new ArrayList<>();
        Matcher nm = Pattern.compile("\"name\"\\s*:\\s*\"([A-Z][^\"]{3,38} at [A-Z][^\"]{3,38})\"").matcher(j);
        while (nm.find()) names.add(nm.group(1).trim());
        List<String> heads = new ArrayList<>();
        Matcher hm = Pattern.compile("\"headline\"\\s*:\\s*\"([^\"]+)\"").matcher(j);
        while (hm.find()) heads.add(hm.group(1).trim());
        List<String> summs = new ArrayList<>();
        Matcher sm = Pattern.compile("\"series\"\\s*:\\s*\\[?\\s*\\{[^\\[]*?\"summary\"\\s*:\\s*\"([^\"]+)\"").matcher(j);
        while (sm.find()) summs.add(sm.group(1).trim());
        int n = Math.min(names.size(), Math.min(heads.size(), summs.size()));
        if (n < 4) return null;
        LinkedHashMap<String, String[]> series = new LinkedHashMap<>();   // sorted-pair key -> [round, summary, matchupLabel]
        for (int i = 0; i < n; i++) {
            String[] parts = names.get(i).split(" at ", 2);
            if (parts.length != 2) continue;
            String a = parts[0].trim(), b = parts[1].trim();
            String key = (a.compareTo(b) < 0) ? a + "" + b : b + "" + a;
            String round = heads.get(i).replaceAll("\\s*-\\s*Game.*$", "").trim();
            series.put(key, new String[]{round, summs.get(i), a + " vs " + b});
        }
        if (series.isEmpty()) return null;
        LinkedHashMap<String, List<String[]>> byRound = new LinkedHashMap<>();   // round -> list of [matchup, summary]
        for (String[] v : series.values()) byRound.computeIfAbsent(v[0], k -> new ArrayList<>()).add(new String[]{v[2], v[1]});
        String[] order = {"East 1st Round", "West 1st Round", "East Semifinals", "West Semifinals",
                "East Conference Finals", "West Conference Finals", "East Conference Final", "West Conference Final",
                "East Finals", "West Finals", "NBA Finals"};
        StringBuilder sb = new StringBuilder();
        sb.append("<div class=\"answer-card nba-card\"><div class=\"ac-icon\" aria-hidden=\"true\">●</div><div class=\"ac-body\">");
        sb.append("<div class=\"ac-title\">NBA Playoffs &mdash; 2026</div><div class=\"nba-bracket-grid\">");
        java.util.LinkedHashSet<String> done = new java.util.LinkedHashSet<>();
        for (String r : order) if (byRound.containsKey(r)) { renderNbaRound(sb, r, byRound.get(r)); done.add(r); }
        for (Map.Entry<String, List<String[]>> e : byRound.entrySet()) if (!done.contains(e.getKey())) renderNbaRound(sb, e.getKey(), e.getValue());
        sb.append("</div><div class=\"ac-sub\"><span class=\"ac-src\">live, via espn.com</span></div></div></div>");
        return sb.toString();
    }

    static void renderNbaRound(StringBuilder sb, String round, List<String[]> seriesList) {
        sb.append("<div class=\"nba-round\"><div class=\"nba-round-name\">").append(esc(round)).append("</div>");
        for (String[] s : seriesList) {
            sb.append("<div class=\"nba-series2\"><span class=\"nba-mu\">").append(esc(s[0])).append("</span>")
              .append("<span class=\"nba-st\">").append(esc(s[1])).append("</span></div>");
        }
        sb.append("</div>");
    }

    // Robust version: parse the playoff-game feed event-by-event. For each "name":"<A> at <B>",
    // look in that event's slice of JSON for a playoff round headline (must match "... - Game N",
    // so regular-season "Game Highlights" recaps don't sneak in) and the series summary. Keeps the
    // latest summary per matchup (events are in date order) and groups by round.
    static String nbaBracketCard2() {
        String j = httpGet("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?seasontype=3&dates=20260415-20260615", 5000);
        if (j == null) return null;
        Pattern namePat = Pattern.compile("\"name\"\\s*:\\s*\"([A-Z][^\"]{3,38} at [A-Z][^\"]{3,38})\"");
        Pattern roundPat = Pattern.compile("\"headline\"\\s*:\\s*\"([^\"]*?)\\s*-\\s*Game\\s*\\d+[^\"]*\"");
        Pattern summPat = Pattern.compile("\"series\"\\s*:\\s*\\[?\\s*\\{[^\\[]*?\"summary\"\\s*:\\s*\"([^\"]+)\"");
        Matcher nm = namePat.matcher(j);
        List<int[]> bounds = new ArrayList<>(); List<String> matchups = new ArrayList<>();
        while (nm.find()) { matchups.add(nm.group(1).trim()); bounds.add(new int[]{nm.start(), nm.end()}); }
        LinkedHashMap<String, String[]> series = new LinkedHashMap<>();   // sorted-pair key -> [round, summary, label]
        for (int i = 0; i < matchups.size(); i++) {
            int from = bounds.get(i)[1];
            int to = (i + 1 < bounds.size()) ? bounds.get(i + 1)[0] : Math.min(j.length(), from + 8000);
            String chunk = j.substring(from, to);
            Matcher rm = roundPat.matcher(chunk);
            if (!rm.find()) continue;                       // no playoff round headline -> not a playoff game
            String round = rm.group(1).trim();
            Matcher sm = summPat.matcher(chunk);
            if (!sm.find()) continue;
            String summ = sm.group(1).trim();
            String[] parts = matchups.get(i).split(" at ", 2);
            if (parts.length != 2) continue;
            String a = parts[0].trim(), b = parts[1].trim();
            String key = (a.compareTo(b) < 0) ? a + "~" + b : b + "~" + a;
            series.put(key, new String[]{round, summ, a + " vs " + b});
        }
        if (series.size() < 2) return null;
        LinkedHashMap<String, List<String[]>> byRound = new LinkedHashMap<>();   // round -> list of [matchup, summary]
        for (String[] v : series.values()) byRound.computeIfAbsent(v[0], k -> new ArrayList<>()).add(new String[]{v[2], v[1]});
        String[] order = {"East 1st Round", "West 1st Round", "Eastern Conference 1st Round", "Western Conference 1st Round",
                "East Semifinals", "West Semifinals", "Eastern Conference Semifinals", "Western Conference Semifinals",
                "East Conference Finals", "West Conference Finals", "Eastern Conference Finals", "Western Conference Finals",
                "East Finals", "West Finals", "NBA Finals"};
        StringBuilder sb = new StringBuilder();
        sb.append("<div class=\"answer-card nba-card\"><div class=\"ac-icon\" aria-hidden=\"true\">●</div><div class=\"ac-body\">");
        sb.append("<div class=\"ac-title\">NBA Playoffs &mdash; 2026</div><div class=\"nba-bracket-grid\">");
        java.util.LinkedHashSet<String> done = new java.util.LinkedHashSet<>();
        for (String r : order) if (byRound.containsKey(r)) { renderNbaRound(sb, r, byRound.get(r)); done.add(r); }
        for (Map.Entry<String, List<String[]>> e : byRound.entrySet()) if (!done.contains(e.getKey())) renderNbaRound(sb, e.getKey(), e.getValue());
        sb.append("</div><div class=\"ac-sub\"><span class=\"ac-src\">live, via espn.com</span></div></div></div>");
        return sb.toString();
    }

    // "nba" / "nba scores" / "nba games" -> today's NBA games. "nba bracket" / "nba playoffs" ->
    // the live playoff bracket. All from ESPN's public scoreboard API.
    static String nbaCard(String query) {
        if (query == null) return null;
        String q = query.trim().toLowerCase(Locale.ROOT);
        boolean wantsBracket = q.equals("nba bracket") || q.equals("nba playoffs") || q.equals("nba playoff bracket")
                || q.equals("playoff bracket") || q.equals("nba playoff") || q.equals("nba playoffs bracket");
        boolean wantsScores = q.equals("nba") || q.equals("nba scores") || q.equals("nba scoreboard")
                || q.equals("nba games") || q.equals("nba today") || q.equals("nba results")
                || q.startsWith("nba score") || q.equals("basketball scores");
        if (!wantsBracket && !wantsScores) return null;
        if (wantsBracket) {
            try { String br = nbaBracketCard2(); if (br != null) return br; } catch (Exception e) { /* fall through to scores */ }
        }
        String today, dateLabel;
        try {
            java.time.LocalDate d = java.time.LocalDate.now(java.time.ZoneId.of("America/New_York"));
            today = d.toString().replace("-", "");
            dateLabel = d.format(java.time.format.DateTimeFormatter.ofPattern("EEE, MMM d", Locale.US));
        } catch (Exception e) {
            today = java.time.LocalDate.now().toString().replace("-", ""); dateLabel = "today";
        }
        // try today first, then fall back to the default (most recent) slate
        for (int t = 0; t < 2; t++) {
            String j = httpGet("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
                    + (t == 0 ? "?dates=" + today : ""), 3500);
            if (j == null) continue;
            List<String> games = new ArrayList<>();
            Matcher m = Pattern.compile("\"name\"\\s*:\\s*\"([A-Z][^\"]{4,38} at [A-Z][^\"]{4,38})\"").matcher(j);
            while (m.find() && games.size() < 16) games.add(m.group(1).trim());
            if (games.isEmpty()) continue;
            // statuses: ESPN repeats shortDetail ~twice per game; take every other
            List<String> allStats = new ArrayList<>();
            Matcher st = Pattern.compile("\"shortDetail\"\\s*:\\s*\"([^\"]{1,40})\"").matcher(j);
            while (st.find()) allStats.add(st.group(1));
            List<String> stats = new ArrayList<>();
            for (int i = 0; i < allStats.size(); i += 2) stats.add(allStats.get(i));
            StringBuilder sb = new StringBuilder();
            sb.append("<div class=\"answer-card nba-card\"><div class=\"ac-icon\" aria-hidden=\"true\">●</div><div class=\"ac-body\">");
            sb.append("<div class=\"ac-title\">NBA &mdash; ").append(games.size()).append(" game").append(games.size() == 1 ? "" : "s")
              .append(t == 0 ? " &middot; " + esc(dateLabel) : " &middot; latest").append("</div><div class=\"nba-games\">");
            for (int i = 0; i < games.size(); i++) {
                sb.append("<span class=\"nba-game\">").append(esc(games.get(i)));
                if (i < stats.size() && !stats.get(i).isEmpty()) sb.append(" <em>").append(esc(stats.get(i))).append("</em>");
                sb.append("</span>");
            }
            sb.append("</div><div class=\"ac-sub\"><span class=\"ac-src\">via espn.com</span></div></div></div>");
            return sb.toString();
        }
        return null;
    }

    // Tries the instant-answer cards (currency, NBA, weather) in order — at most one matches.
    static String instantAnswerCard(String query, String clientIp) {
        try { String c = currencyCard(query); if (c != null) return c; } catch (Exception e) {}
        try { String c = nbaCard(query); if (c != null) return c; } catch (Exception e) {}
        try { String c = weatherCard(query, clientIp); if (c != null) return c; } catch (Exception e) {}
        return null;
    }

    // ========== IMAGE SEARCH (text search -> harvest <img> from the top pages) ==========

    private static final Pattern IMG_TAG = Pattern.compile("(?i)<img\\s+([^>]*?)/?>");
    private static final Pattern IMG_SRC = Pattern.compile("(?i)\\bsrc\\s*=\\s*[\"']?([^\"'\\s>]+)");
    private static final Pattern IMG_ALT = Pattern.compile("(?i)\\balt\\s*=\\s*[\"']([^\"']*)[\"']");
    private static final Pattern IMG_W   = Pattern.compile("(?i)\\bwidth\\s*=\\s*[\"']?(\\d{1,5})");
    private static final Pattern IMG_H   = Pattern.compile("(?i)\\bheight\\s*=\\s*[\"']?(\\d{1,5})");

    // Extract <img> candidates from a page, each with a relevance SCORE: + for query terms in the
    // alt text, + for a real description, + for content-CDN paths / large dimensions; - for empty
    // alt or tiny dimensions. Decorative junk (icons, sprites, pixels, ads, SVG) is dropped outright.
    static List<ImageHit> extractImages(String html, String pageUrl, List<String> queryWords) {
        List<ImageHit> out = new ArrayList<>();
        if (html == null) return out;
        String scheme = "https", host = "";
        try {
            int s = pageUrl.indexOf("://");
            if (s > 0) scheme = pageUrl.substring(0, s);
            String c = pageUrl.replaceFirst("^https?://", "");
            int slash = c.indexOf('/');
            host = (slash > 0) ? c.substring(0, slash) : c;
        } catch (Exception e) { /* ignore */ }
        Matcher m = IMG_TAG.matcher(html);
        int count = 0;
        while (m.find() && count < 40) {
            String attrs = m.group(1);
            Matcher sm = IMG_SRC.matcher(attrs);
            if (!sm.find()) continue;
            String src = sm.group(1).trim();
            if (src.isEmpty() || src.startsWith("data:")) continue;
            String low = src.toLowerCase();
            if (low.endsWith(".svg") || low.contains("sprite") || low.contains("/icon") || low.contains("icon.")
                    || low.contains("logo") || low.contains("pixel") || low.contains("spacer")
                    || low.contains("blank.gif") || low.contains("1x1") || low.contains("avatar")
                    || low.contains("emoji") || low.contains("/ad/") || low.contains("/ads/") || low.contains("banner")
                    || low.contains("button") || low.contains("badge")) continue;
            String full;
            if (src.startsWith("http://") || src.startsWith("https://")) full = src;
            else if (src.startsWith("//")) full = scheme + ":" + src;
            else if (src.startsWith("/")) full = host.isEmpty() ? null : (scheme + "://" + host + src);
            else full = null;   // page-relative — skip
            if (full == null) continue;
            Matcher am = IMG_ALT.matcher(attrs);
            String alt = am.find() ? am.group(1).trim() : "";
            int score = 0;
            String altLow = alt.toLowerCase();
            int qMatches = 0;
            if (queryWords != null) for (String w : queryWords) {
                if (w != null && w.length() >= 3 && altLow.contains(w.toLowerCase())) qMatches++;
            }
            score += qMatches * 3;   // each query word found in the alt text is a strong signal
            if (alt.length() >= 4) score += 1; else score -= 2;   // a real caption vs decorative
            String fl = full.toLowerCase();
            if (fl.contains("upload.wikimedia.org") || fl.contains("/commons/") || fl.contains("/media/")
                    || fl.contains("/uploads/") || fl.contains("/photos/")) score += 1;
            int w = 0, h = 0;
            Matcher wm = IMG_W.matcher(attrs); if (wm.find()) try { w = Integer.parseInt(wm.group(1)); } catch (Exception e) {}
            Matcher hm = IMG_H.matcher(attrs); if (hm.find()) try { h = Integer.parseInt(hm.group(1)); } catch (Exception e) {}
            if ((w > 0 && w < 80) || (h > 0 && h < 80)) score -= 3;
            else if (w >= 150 || h >= 150) score += 1;
            if (score <= -2) continue;
            ImageHit ih = new ImageHit();
            ih.imgUrl = full; ih.pageUrl = pageUrl; ih.alt = alt; ih.score = score;
            out.add(ih);
            count++;
        }
        return out;
    }

    static List<ImageHit> collectImages(List<Result> results, int max, List<String> queryWords) {
        List<ImageHit> all = new ArrayList<>();
        if (results == null || results.isEmpty()) return all;
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, Math.min(results.size(), 12)));
        List<Future<List<ImageHit>>> fs = new ArrayList<>();
        int rank = 0;
        for (Result r : results) {
            final String pageUrl = r.url;
            final int rk = rank++;
            fs.add(pool.submit(() -> {
                List<ImageHit> hits = new ArrayList<>();
                try {
                    byte[] b = kvs.get("pt-crawl", Hasher.hash(pageUrl), "page");
                    if (b != null) {
                        int cap = (rk < 3) ? 12 : 6, n = 0;
                        for (ImageHit ih : extractImages(new String(b), pageUrl, queryWords)) {
                            ih.score += (rk < 3) ? 2 : (rk < 10 ? 1 : 0);   // boost images from top-ranked pages
                            hits.add(ih);
                            if (++n >= cap) break;
                        }
                    }
                } catch (Exception e) { /* skip */ }
                return hits;
            }));
        }
        for (Future<List<ImageHit>> f : fs) {
            try { all.addAll(f.get(4, TimeUnit.SECONDS)); } catch (Exception e) { /* skip */ }
        }
        pool.shutdownNow();
        all.sort((a, b) -> b.score - a.score);
        java.util.LinkedHashSet<String> seen = new java.util.LinkedHashSet<>();
        List<ImageHit> out = new ArrayList<>();
        for (ImageHit ih : all) { if (out.size() >= max) break; if (seen.add(ih.imgUrl)) out.add(ih); }
        return out;
    }

    // ========== Location-aware: cached IP -> "city" ==========
    private static final ConcurrentHashMap<String, String> GEO_CACHE = new ConcurrentHashMap<>();
    static String cityForIp(String ip) {
        if (ip == null || ip.isEmpty()) return null;
        String cached = GEO_CACHE.get(ip);
        if (cached != null) return cached.isEmpty() ? null : cached;
        String geo = geolocateIp(ip);
        String city = "";
        if (geo != null) {
            String[] p = geo.split(",", 3);
            if (p.length > 2 && !p[2].isEmpty()) city = p[2];
        }
        GEO_CACHE.put(ip, city);
        return city.isEmpty() ? null : city;
    }

    static String buildResultsPage(String query, List<Result> results, int totalMatches, String didYouMean, String weatherCard, String fromCity, long elapsed) {
        StringBuilder sb = new StringBuilder();
        sb.append("<!doctype html><html lang=\"en\"><head>");
        sb.append("<script>try{if(localStorage.getItem('cerebro-theme')==='light')document.documentElement.classList.add('light');}catch(e){}</script>");
        sb.append("<meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
        sb.append("<title>Cerebro &mdash; ").append(esc(query)).append("</title>");
        sb.append("<link rel=\"icon\" href=\"/cerebro-mark.svg\" type=\"image/svg+xml\">");
        sb.append("<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">");
        sb.append("<link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin>");
        sb.append("<link href=\"https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600");
        sb.append("&family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap\" rel=\"stylesheet\">");
        sb.append("<link rel=\"stylesheet\" href=\"/styles.css\">");
        sb.append("<style>mark{background:rgba(255,213,79,.32);color:inherit;padding:1px 3px;border-radius:3px;font-weight:600}");
        sb.append(".dym{padding:14px 0;font-size:18px;color:var(--fg-2,#B6BECE)}");
        sb.append(".dym a{color:#7CC6FF;font-weight:600;text-decoration:none}.dym a:hover{text-decoration:underline}");
        sb.append(".url .fav{width:16px;height:16px;vertical-align:-3px;margin-right:7px;border-radius:3px;background:rgba(255,255,255,0.05)}");
        sb.append(".url .dom{color:var(--fg-1,#E8ECF5);font-weight:600}");
        sb.append(".url .sep{color:var(--fg-4,#5B6478);opacity:.75;margin:0 2px}");
        sb.append(".weather-card{display:flex;align-items:center;gap:18px;flex-wrap:wrap;padding:18px 22px;margin:0 0 20px;border:1px solid var(--fg-5,#2E3547);border-radius:14px;background:var(--surface,#141A2B)}");
        sb.append(".weather-card .wx-icon{font-size:40px;line-height:1}");
        sb.append(".weather-card .wx-main{flex:1;min-width:170px}");
        sb.append(".weather-card .wx-place{font-size:14px;color:var(--fg-3,#8792A6);margin-bottom:3px}");
        sb.append(".weather-card .wx-temp{font-size:32px;font-weight:600;color:var(--fg-1,#E8ECF5);line-height:1}");
        sb.append(".weather-card .wx-cond{font-size:14px;color:var(--fg-2,#B6BECE);margin-top:5px}");
        sb.append(".weather-card .wx-meta{display:flex;gap:14px;flex-wrap:wrap;font-size:13px;color:var(--fg-4,#5B6478)}");
        sb.append(".weather-card .wx-src{opacity:.7}");
        sb.append(".tabs{display:flex;gap:16px;margin:0 0 6px;font-size:14px}");
        sb.append(".tabs a{color:var(--fg-3,#8792A6);text-decoration:none;padding:3px 0;border-bottom:2px solid transparent}");
        sb.append(".tabs a.active{color:var(--fg-1,#E8ECF5);border-bottom-color:var(--signal,#7CC6FF)}.tabs a:hover{color:var(--fg-1,#E8ECF5)}");
        sb.append(".answer-card{display:flex;align-items:flex-start;gap:16px;padding:18px 22px;margin:0 0 20px;border:1px solid var(--fg-5,#2E3547);border-radius:14px;background:var(--surface,#141A2B)}");
        sb.append(".answer-card .ac-icon{font-size:24px;line-height:1.2;color:var(--signal,#7CC6FF);flex-shrink:0}");
        sb.append(".answer-card .ac-body{flex:1;min-width:0}.answer-card .ac-title{font-size:21px;font-weight:600;color:var(--fg-1,#E8ECF5)}");
        sb.append(".answer-card .ac-sub{font-size:13px;color:var(--fg-4,#5B6478);margin-top:6px}.answer-card .ac-src{opacity:.8}");
        sb.append(".nba-section{margin-top:4px}.nba-section + .nba-section{margin-top:14px}");
        sb.append(".nba-games{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}");
        sb.append(".nba-game{font-size:13px;padding:4px 10px;border-radius:8px;background:var(--surface-2,#0E1220);color:var(--fg-2,#B6BECE);border:1px solid var(--fg-5,#2E3547)}");
        sb.append(".nba-game em{color:var(--fg-4,#5B6478);font-style:normal;margin-left:6px}");
        sb.append(".nba-bracket{display:flex;flex-wrap:wrap;gap:4px 28px;margin-top:4px}");
        sb.append(".nba-conf{margin-top:8px;min-width:250px}.nba-conf-name{font-size:11px;letter-spacing:.07em;text-transform:uppercase;color:var(--fg-4,#5B6478);margin-bottom:4px}");
        sb.append(".nba-series{display:flex;align-items:center;gap:8px;font-size:13px;color:var(--fg-2,#B6BECE);padding:2px 0}");
        sb.append(".nba-team{display:flex;align-items:center;gap:6px}.nba-vs{color:var(--fg-4,#5B6478);font-size:11px}");
        sb.append(".nba-seed{font-size:11px;font-weight:600;color:var(--ink,#07090F);background:var(--signal,#7CC6FF);border-radius:4px;padding:1px 5px;min-width:13px;text-align:center}");
        sb.append(".nba-bracket-grid{display:flex;gap:14px;flex-wrap:wrap;margin-top:8px;overflow-x:auto}");
        sb.append(".nba-round{min-width:215px;flex:1}");
        sb.append(".nba-round-name{font-size:11px;letter-spacing:.07em;text-transform:uppercase;color:var(--fg-4,#5B6478);margin-bottom:6px;font-weight:600}");
        sb.append(".nba-series2{padding:7px 10px;margin-bottom:6px;border-radius:9px;background:var(--surface-2,#0E1220);border:1px solid var(--fg-5,#2E3547)}");
        sb.append(".nba-series2 .nba-mu{display:block;font-size:13px;color:var(--fg-1,#E8ECF5);font-weight:500}");
        sb.append(".nba-series2 .nba-st{display:block;font-size:12px;color:var(--fg-3,#8792A6);margin-top:2px}</style>");
        sb.append("</head><body>");

        // topbar
        sb.append("<header class=\"topbar\">");
        sb.append("<a class=\"brand\" href=\"/\">").append(logoSvg());
        sb.append("<span class=\"wordmark\">Cerebro</span></a>");
        sb.append("<div class=\"search-wrap\"><form action=\"/search\" method=\"GET\" autocomplete=\"off\">");
        sb.append("<label class=\"search search--md\" id=\"searchBox\" for=\"q\">").append(searchIconSvg());
        sb.append("<input id=\"q\" name=\"q\" type=\"text\" autocomplete=\"off\" spellcheck=\"false\" ");
        sb.append("placeholder=\"Search the web\" value=\"").append(esc(query)).append("\"/>");
        sb.append("<span class=\"enter-badge\" aria-hidden=\"true\">&#9166;</span>");
        sb.append("</label>");
        sb.append("<div class=\"suggest-box\" id=\"suggestBox\" role=\"listbox\"></div>");
        sb.append("</form></div></header>");

        // results area
        sb.append("<main class=\"results\">");
        if (weatherCard != null && !weatherCard.isEmpty()) sb.append(weatherCard);
        sb.append("<div class=\"tabs\"><a class=\"active\" href=\"/search?q=").append(urlEncode(query))
          .append("\">All</a><a href=\"/images?q=").append(urlEncode(query)).append("\">Images</a></div>");
        if (results != null && !results.isEmpty()) {
            sb.append("<div class=\"count\">").append(totalMatches)
              .append(totalMatches == 1 ? " result" : " results");
            if (fromCity != null && !fromCity.isEmpty()) sb.append(" &middot; from ").append(esc(fromCity));
            if (totalMatches > results.size()) {
                sb.append(" &middot; <span class=\"loading-more\">loading more&hellip;</span>");
            }
            sb.append(" &middot; ").append(String.format("%.2f", elapsed / 1000.0)).append("s</div>");
            int delay = 0;
            int idx = 0;
            for (Result r : results) {
                String hidden = idx >= 10 ? " result--hidden" : "";
                sb.append("<div class=\"result").append(hidden).append("\" style=\"animation-delay:")
                  .append(Math.min(delay, 270)).append("ms;\">");
                sb.append("<div class=\"url\">").append(renderDomainBar(r.url)).append("</div>");
                sb.append("<a class=\"title\" href=\"").append(esc(r.url))
                  .append("\" target=\"_blank\" rel=\"noopener\">")
                  .append(r.titleHtml != null ? r.titleHtml : esc(r.title)).append("</a>");
                if (r.snippet != null && !r.snippet.isEmpty())
                    sb.append("<div class=\"snippet\">")
                      .append(r.snippetHtml != null ? r.snippetHtml : esc(r.snippet)).append("</div>");
                if (r.hasContent) {
                    sb.append("<div class=\"result-actions\">");
                    sb.append("<a class=\"cache-link\" href=\"/cache?url=").append(urlEncode(r.url))
                      .append("\" target=\"_blank\" rel=\"noopener\">Cached</a>");
                    sb.append("</div>");
                }
                sb.append("</div>");
                delay += 30;
                idx++;
            }
            if (results.size() > 10) {
                sb.append("<div id=\"loadMoreWrap\" style=\"text-align:center;padding:28px 0;\">");
                sb.append("<button class=\"load-more\" onclick=\"loadMore()\">Load more results</button>");
                sb.append("</div>");
            }
        } else {
            sb.append("<div class=\"count\">No results found</div>");
            if (didYouMean != null && !didYouMean.isEmpty()) {
                sb.append("<div class=\"dym\">Did you mean: <a href=\"/search?q=")
                  .append(urlEncode(didYouMean)).append("\"><em>")
                  .append(esc(didYouMean)).append("</em></a>?</div>");
            }
            sb.append("<div style=\"padding:14px;color:var(--fg-3);font-size:15px\">");
            sb.append("<p>Your search &mdash; <strong style=\"color:var(--fg-1)\">")
              .append(esc(query)).append("</strong> &mdash; did not match any documents.</p>");
            sb.append("<ul style=\"margin-top:8px;margin-left:20px;color:var(--fg-4)\">");
            sb.append("<li>Make sure all words are spelled correctly.</li>");
            sb.append("<li>Try different or more general keywords.</li></ul></div>");
        }
        sb.append("</main><script src=\"/app.js\"></script>");
        sb.append("<script>function loadMore(){");
        sb.append("var h=document.querySelectorAll('.result--hidden');");
        sb.append("var c=0;for(var i=0;i<h.length&&c<10;i++){h[i].classList.remove('result--hidden');c++;}");
        sb.append("var lm=document.getElementById('loadMoreWrap');");
        sb.append("if(lm&&!document.querySelector('.result--hidden'))lm.style.display='none';");
        sb.append("}");
        // Infinite scroll: when the viewport gets within ~700px of the page bottom, reveal
        // the next batch of hidden results automatically. Debounced via `ifsBusy` so a single
        // scroll burst can't fire loadMore() dozens of times.
        sb.append("var ifsBusy=false;function ifsCheck(){");
        sb.append("if(ifsBusy)return;");
        sb.append("if(!document.querySelector('.result--hidden'))return;");
        sb.append("if(window.innerHeight+window.scrollY<document.body.offsetHeight-700)return;");
        sb.append("ifsBusy=true;loadMore();setTimeout(function(){ifsBusy=false;ifsCheck();},120);}");
        sb.append("window.addEventListener('scroll',ifsCheck,{passive:true});");
        sb.append("window.addEventListener('resize',ifsCheck,{passive:true});");
        sb.append("</script>");
        if (results != null && totalMatches > results.size()) {
            // Async-load the remaining (totalMatches - 100) results in the background and inject
            // them above the "Load more" button. The user sees the top 100 immediately; the rest
            // stream in within a couple of seconds and the count's "loading more..." badge clears.
            sb.append("<script>(async function(){try{");
            sb.append("var q='").append(jsonEsc(query)).append("';");
            sb.append("var r=await fetch('/search-more?q='+encodeURIComponent(q));");
            sb.append("if(!r.ok)return; var html=await r.text(); if(!html)return;");
            sb.append("var lm=document.getElementById('loadMoreWrap');");
            sb.append("if(lm){lm.insertAdjacentHTML('beforebegin',html);} else {");
            sb.append("var m=document.querySelector('main.results'); if(m)m.insertAdjacentHTML('beforeend',html);}");
            sb.append("var more=document.querySelector('.loading-more'); if(more)more.remove();");
            sb.append("if(!lm){var totalNow=document.querySelectorAll('.result').length;");
            sb.append("if(totalNow>10){var m2=document.querySelector('main.results');");
            sb.append("m2.insertAdjacentHTML('beforeend','<div id=\"loadMoreWrap\" style=\"text-align:center;padding:28px 0;\"><button class=\"load-more\" onclick=\"loadMore()\">Load more results</button></div>');}}");
            sb.append("}catch(e){var more=document.querySelector('.loading-more'); if(more)more.textContent='(load more failed)';}})();</script>");
        }
        sb.append("<button class=\"theme-toggle\" id=\"themeToggle\" type=\"button\" aria-label=\"Toggle light/dark theme\" title=\"Toggle light/dark\">&#9680;</button>");
        sb.append("<script>(function(){var b=document.getElementById('themeToggle');if(!b)return;");
        sb.append("b.addEventListener('click',function(){var lt=document.documentElement.classList.toggle('light');");
        sb.append("try{localStorage.setItem('cerebro-theme',lt?'light':'dark');}catch(e){}});})();</script>");
        sb.append("</body></html>");
        return sb.toString();
    }

    static String buildImagesPage(String query, List<ImageHit> images, long elapsed) {
        StringBuilder sb = new StringBuilder();
        sb.append("<!doctype html><html lang=\"en\"><head>");
        sb.append("<script>try{if(localStorage.getItem('cerebro-theme')==='light')document.documentElement.classList.add('light');}catch(e){}</script>");
        sb.append("<meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
        sb.append("<title>Cerebro Images &mdash; ").append(esc(query)).append("</title>");
        sb.append("<link rel=\"icon\" href=\"/cerebro-mark.svg\" type=\"image/svg+xml\">");
        sb.append("<link rel=\"stylesheet\" href=\"/styles.css\">");
        sb.append("<style>");
        sb.append("mark{background:rgba(255,213,79,.32);color:inherit;padding:1px 3px;border-radius:3px;font-weight:600}");
        sb.append(".tabs{display:flex;gap:16px;margin:0 0 6px;font-size:14px}");
        sb.append(".tabs a{color:var(--fg-3,#8792A6);text-decoration:none;padding:3px 0;border-bottom:2px solid transparent}");
        sb.append(".tabs a.active{color:var(--fg-1,#E8ECF5);border-bottom-color:var(--signal,#7CC6FF)}.tabs a:hover{color:var(--fg-1,#E8ECF5)}");
        sb.append(".img-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:10px;padding:8px 0}");
        sb.append(".img-tile{position:relative;display:block;border-radius:10px;overflow:hidden;background:var(--surface-2,#141A2B);border:1px solid var(--fg-5,#2E3547);aspect-ratio:4/3}");
        sb.append(".img-tile img{width:100%;height:100%;object-fit:cover;display:block;transition:transform .25s ease}");
        sb.append(".img-tile:hover img{transform:scale(1.06)}");
        sb.append(".img-tile .cap{position:absolute;left:0;right:0;bottom:0;padding:6px 9px;font-size:11px;color:#fff;background:linear-gradient(transparent,rgba(0,0,0,.78));white-space:nowrap;overflow:hidden;text-overflow:ellipsis}");
        sb.append("</style></head><body>");
        sb.append("<header class=\"topbar\"><a class=\"brand\" href=\"/\">").append(logoSvg());
        sb.append("<span class=\"wordmark\">Cerebro</span></a>");
        sb.append("<div class=\"search-wrap\"><form action=\"/search\" method=\"GET\" autocomplete=\"off\">");
        sb.append("<label class=\"search search--md\" for=\"q\">").append(searchIconSvg());
        sb.append("<input id=\"q\" name=\"q\" type=\"text\" autocomplete=\"off\" spellcheck=\"false\" placeholder=\"Search the web\" value=\"").append(esc(query)).append("\"/>");
        sb.append("<span class=\"enter-badge\" aria-hidden=\"true\">&#9166;</span></label></form></div></header>");
        sb.append("<main class=\"results\">");
        sb.append("<div class=\"tabs\"><a href=\"/search?q=").append(urlEncode(query)).append("\">All</a>");
        sb.append("<a class=\"active\" href=\"/images?q=").append(urlEncode(query)).append("\">Images</a></div>");
        if (images != null && !images.isEmpty()) {
            sb.append("<div class=\"count\">").append(images.size()).append(" images &middot; ")
              .append(String.format("%.2f", elapsed / 1000.0)).append("s</div>");
            sb.append("<div class=\"img-grid\">");
            for (ImageHit ih : images) {
                String dom = "";
                try { dom = cleanUrl(ih.pageUrl).replaceFirst("^https?://", "").split("/")[0]; } catch (Exception e) {}
                String capText = (ih.alt != null && !ih.alt.isEmpty()) ? ih.alt : dom;
                sb.append("<a class=\"img-tile\" href=\"").append(esc(ih.pageUrl)).append("\" target=\"_blank\" rel=\"noopener\" title=\"").append(esc(capText)).append("\">");
                sb.append("<img src=\"").append(esc(ih.imgUrl)).append("\" loading=\"lazy\" alt=\"").append(esc(ih.alt)).append("\" referrerpolicy=\"no-referrer\" onerror=\"var t=this.closest('.img-tile');if(t)t.style.display='none';\">");
                sb.append("<span class=\"cap\">").append(esc(capText)).append("</span></a>");
            }
            sb.append("</div>");
        } else {
            sb.append("<div class=\"count\">No images found</div>");
            sb.append("<p style=\"padding:14px;color:var(--fg-3,#8792A6)\">No images found in the pages matching <strong>").append(esc(query)).append("</strong>. Try a broader term.</p>");
        }
        sb.append("</main>");
        sb.append("<button class=\"theme-toggle\" id=\"themeToggle\" type=\"button\" aria-label=\"Toggle light/dark theme\" title=\"Toggle light/dark\">&#9680;</button>");
        sb.append("<script>(function(){var b=document.getElementById('themeToggle');if(!b)return;b.addEventListener('click',function(){var lt=document.documentElement.classList.toggle('light');try{localStorage.setItem('cerebro-theme',lt?'light':'dark');}catch(e){}});})();</script>");
        sb.append("</body></html>");
        return sb.toString();
    }

    // Returns just the result-div sequence for a list of results — used by /search-more to
    // inject more results into a page already showing the top 100. All divs are hidden
    // (result--hidden) so the existing "Load more" button can reveal them in batches of 10.
    static String renderResultDivs(List<Result> results) {
        if (results == null || results.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (Result r : results) {
            sb.append("<div class=\"result result--hidden\" style=\"animation-delay:0ms;\">");
            sb.append("<div class=\"url\">").append(esc(displayDomain(r.url))).append("</div>");
            sb.append("<a class=\"title\" href=\"").append(esc(r.url))
              .append("\" target=\"_blank\" rel=\"noopener\">")
              .append(r.titleHtml != null ? r.titleHtml : esc(r.title)).append("</a>");
            if (r.snippet != null && !r.snippet.isEmpty())
                sb.append("<div class=\"snippet\">")
                  .append(r.snippetHtml != null ? r.snippetHtml : esc(r.snippet)).append("</div>");
            if (r.hasContent) {
                sb.append("<div class=\"result-actions\">");
                sb.append("<a class=\"cache-link\" href=\"/cache?url=").append(urlEncode(r.url))
                  .append("\" target=\"_blank\" rel=\"noopener\">Cached</a>");
                sb.append("</div>");
            }
            sb.append("</div>");
        }
        return sb.toString();
    }

    static String buildDebugPage(String query, List<Result> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("<html><head><title>Debug: ").append(esc(query)).append("</title>");
        sb.append("<style>body{font-family:'JetBrains Mono',monospace;padding:20px;background:#0E1220;color:#E8ECF5}");
        sb.append("table{border-collapse:collapse;width:100%} th,td{border:1px solid #2E3547;padding:6px;font-size:12px}");
        sb.append("th{background:#141A2B;color:#7CC6FF} a{color:#7CC6FF} tr:hover{background:#141A2B}</style></head><body>");
        sb.append("<h2 style='color:#7CC6FF'>Debug: \"").append(esc(query)).append("\"</h2>");
        sb.append("<p style='color:#8792A6'>Total docs: ").append(totalDocs).append("</p>");
        sb.append("<form action='/debug'><input name='q' value=\"").append(esc(query))
          .append("\" style='padding:6px;width:400px;background:#141A2B;color:#E8ECF5;border:1px solid #2E3547'/>");
        sb.append("<button style='padding:6px 12px;margin-left:8px;background:#141A2B;color:#7CC6FF;border:1px solid #2E3547'>Search</button></form>");
        sb.append("<hr style='border-color:#2E3547'/>");
        if (results == null || results.isEmpty()) {
            sb.append("<p>No results found.</p>");
        } else {
            sb.append("<table><tr><th>#</th><th>Score</th><th>PR</th><th>Terms</th><th>Title</th><th>URL</th></tr>");
            for (Result r : results) {
                sb.append("<tr><td>").append(r.rank).append("</td>");
                sb.append("<td>").append(String.format("%.4f", r.score)).append("</td>");
                sb.append("<td>").append(String.format("%.4f", r.pr)).append("</td>");
                sb.append("<td>").append(r.termHits).append("/").append(r.totalTerms).append("</td>");
                sb.append("<td>").append(esc(r.title)).append("</td>");
                sb.append("<td><a href='").append(esc(r.url)).append("' target='_blank'>")
                  .append(esc(cleanUrl(r.url))).append("</a></td></tr>");
            }
            sb.append("</table>");
        }
        sb.append("</body></html>");
        return sb.toString();
    }

    static String logoSvg() {
        return "<svg class=\"mark\" viewBox=\"0 0 64 64\" fill=\"none\" aria-hidden=\"true\">" +
               "<g stroke=\"var(--signal)\" stroke-width=\"1.25\" fill=\"none\">" +
               "<circle cx=\"32\" cy=\"32\" r=\"22\" opacity=\"0.25\"/>" +
               "<circle cx=\"32\" cy=\"32\" r=\"15\" opacity=\"0.5\"/>" +
               "<circle cx=\"32\" cy=\"32\" r=\"8\" opacity=\"0.85\"/></g>" +
               "<circle cx=\"32\" cy=\"32\" r=\"2.5\" fill=\"var(--signal)\"/>" +
               "<g stroke=\"var(--signal)\" stroke-width=\"1.25\" stroke-linecap=\"round\" opacity=\"0.6\">" +
               "<line x1=\"32\" y1=\"6\" x2=\"32\" y2=\"10\"/><line x1=\"32\" y1=\"54\" x2=\"32\" y2=\"58\"/>" +
               "<line x1=\"6\" y1=\"32\" x2=\"10\" y2=\"32\"/><line x1=\"54\" y1=\"32\" x2=\"58\" y2=\"32\"/>" +
               "</g></svg>";
    }

    static String searchIconSvg() {
        return "<svg class=\"icon-search\" width=\"16\" height=\"16\" viewBox=\"0 0 24 24\" fill=\"none\" " +
               "stroke-width=\"1.25\" stroke-linecap=\"round\" stroke-linejoin=\"round\" aria-hidden=\"true\">" +
               "<circle cx=\"10.5\" cy=\"10.5\" r=\"6.5\"/><line x1=\"15.25\" y1=\"15.25\" x2=\"20\" y2=\"20\"/></svg>";
    }
}
