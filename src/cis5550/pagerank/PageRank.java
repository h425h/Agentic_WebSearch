package cis5550.jobs;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class PageRank {

    // STX (char 2): separator between url and page in the packed pt-crawl row.
    // Written as (char) 2 — not a literal control char — so it survives source round-trips.
    private static final char STX = (char) 2;
    private static final int MAX_PAGE_CHARS = 200_000;
    private static final int MAX_LINKS_PER_PAGE = 20;
    // Each iteration is a full Flame shuffle (~MAX_LINKS_PER_PAGE writes per page); on a
    // 4-node t3.medium cluster with a ~50K-page corpus, 5 iterations keeps the run to ~30 min.
    private static final int MAX_ITERATIONS = 5;
    private static final double DAMPING = 0.85;

    // `<a ...>` opening tags. [^>]* (not .*?) so an unclosed tag can't trigger huge backtracking.
    private static final Pattern LINK_RE =
            Pattern.compile("<a\\s+([^>]*)>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    private static final Pattern HREF_RE =
            Pattern.compile("href\\s*=\\s*[\"']?([^\"'\\s>]+)", Pattern.CASE_INSENSITIVE);

    public static String normalizeURL(String baseUrl, String url) {
        String[] baseParts = URLParser.parseURL(baseUrl);
        String[] urlParts = URLParser.parseURL(url);

        if (urlParts[1] == null) {                 // relative — no host
            urlParts[0] = baseParts[0];
            urlParts[1] = baseParts[1];
            urlParts[2] = baseParts[2];
            String rel = urlParts[3] == null ? "/" : urlParts[3];
            if (!rel.startsWith("/")) {
                String basePath = baseParts[3] == null ? "/" : baseParts[3];
                int s = basePath.lastIndexOf('/');
                basePath = s >= 0 ? basePath.substring(0, s + 1) : "/";
                while (rel.startsWith("../")) {
                    rel = rel.substring(3);
                    if (basePath.endsWith("/") && basePath.length() > 1) basePath = basePath.substring(0, basePath.length() - 1);
                    s = basePath.lastIndexOf('/');
                    basePath = s >= 0 ? basePath.substring(0, s + 1) : "/";
                }
                rel = basePath + rel;
            }
            urlParts[3] = rel;
        }
        if (urlParts[3] == null) urlParts[3] = "/";
        if (urlParts[2] == null) urlParts[2] = "https".equals(urlParts[0]) ? "443" : "80";
        return urlParts[0] + "://" + urlParts[1] + ":" + urlParts[2] + urlParts[3];
    }

    // Up to MAX_LINKS_PER_PAGE distinct http(s) links from a page. Any malformed href is
    // silently skipped — a single broken link must not abort the whole Flame job.
    public static List<String> extractLinks(String page, String baseUrl) {
        Set<String> out = new LinkedHashSet<>();
        if (page == null) return new ArrayList<>();
        if (page.length() > MAX_PAGE_CHARS) page = page.substring(0, MAX_PAGE_CHARS);
        Matcher m = LINK_RE.matcher(page);
        while (m.find() && out.size() < MAX_LINKS_PER_PAGE) {
            Matcher h = HREF_RE.matcher(m.group(1));
            if (!h.find()) continue;
            try {
                String url = h.group(1);
                int hash = url.indexOf('#');
                if (hash >= 0) url = url.substring(0, hash);
                if (url.isEmpty()) continue;
                String lc = url.toLowerCase();
                if (lc.startsWith("javascript:") || lc.startsWith("mailto:")
                        || lc.startsWith("data:") || lc.startsWith("tel:")) continue;
                String[] cp = URLParser.parseURL(normalizeURL(baseUrl, url));
                if (cp[0] == null || (!cp[0].equals("http") && !cp[0].equals("https"))) continue;
                if (cp[1] == null) continue;
                String path = cp[3] == null ? "/" : cp[3].toLowerCase();
                if (path.endsWith(".jpg") || path.endsWith(".jpeg") || path.endsWith(".gif")
                        || path.endsWith(".png") || path.endsWith(".svg") || path.endsWith(".ico")
                        || path.endsWith(".css") || path.endsWith(".js") || path.endsWith(".pdf")
                        || path.endsWith(".zip") || path.endsWith(".txt")) continue;
                String port = cp[2] == null ? ("https".equals(cp[0]) ? "443" : "80") : cp[2];
                String norm = cp[0] + "://" + cp[1] + ":" + port + (cp[3] == null ? "/" : cp[3]);
                if (norm.length() <= 2000) out.add(norm);
            } catch (Exception e) { /* skip malformed link */ }
        }
        return new ArrayList<>(out);
    }

    public static void run(FlameContext context, String[] args) throws Exception {
        double threshold = args.length > 0 ? Double.parseDouble(args[0]) : 0.01;
        double percentage = args.length > 1 ? Double.parseDouble(args[1]) : 100.0;

        // Hash-keyed scan of pt-crawl: pack (url, page). Keying by raw url would put every
        // http(s):// row on a single worker — pack into the hash-keyed fromTable output so
        // the link-extraction step spreads across all KVS/Flame workers.
        FlameRDD packed = context.fromTable("pt-crawl", row -> {
            String u = row.get("url");
            String p = row.get("page");
            if (u == null || p == null) return null;
            if (p.length() > MAX_PAGE_CHARS) p = p.substring(0, MAX_PAGE_CHARS);
            return u + STX + p;
        });

        // state: hash(url) -> "rc,rp,linkHash1 linkHash2 ..."   (out-links stored as hashes)
        FlamePairRDD state = packed.flatMapToPair(s -> {
            int sep = s.indexOf(STX);
            if (sep < 0) return new ArrayList<>();
            String url = s.substring(0, sep);
            String page = s.substring(sep + 1);
            StringBuilder L = new StringBuilder();
            for (String link : extractLinks(page, url)) {
                if (L.length() > 0) L.append(' ');
                L.append(Hasher.hash(link));
            }
            return List.of(new FlamePair(Hasher.hash(url), "1.0,1.0," + L));
        });

        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            FlamePairRDD transferTable = state.flatMapToPair(p -> {
                String urlHashed = p._1();
                String[] split = p._2().split(",", 3);
                double rc = Double.parseDouble(split[0]);
                String[] linkList = (split.length > 2 && !split[2].isEmpty()) ? split[2].split(" ") : new String[0];
                int n = linkList.length;
                List<FlamePair> pairs = new ArrayList<>();
                pairs.add(new FlamePair(urlHashed, "0.0"));   // page always present in aggregated
                if (n > 0) {
                    String tv = String.valueOf(DAMPING * rc / n);
                    for (String linkHash : linkList) {
                        if (!linkHash.isEmpty()) pairs.add(new FlamePair(linkHash, tv));
                    }
                }
                return pairs;
            });

            FlamePairRDD aggregated = transferTable.foldByKey("", (a, b) -> {
                if (a.isEmpty()) return b;
                if (b.isEmpty()) return a;
                return String.valueOf(Double.parseDouble(a) + Double.parseDouble(b));
            });

            FlamePairRDD joined = state.join(aggregated);

            state = joined.flatMapToPair(p -> {
                String v = p._2();
                int lastComma = v.lastIndexOf(',');
                double transferVal = Double.parseDouble(v.substring(lastComma + 1));
                String[] split = v.substring(0, lastComma).split(",", 3);
                double oldRc = Double.parseDouble(split[0]);
                double newRc = 1.0 - DAMPING + transferVal;
                String links = split.length > 2 ? split[2] : "";
                return List.of(new FlamePair(p._1(), newRc + "," + oldRc + "," + links));
            });

            FlameRDD diff = state.flatMap(p -> {
                String[] split = p._2().split(",");
                double rc = Double.parseDouble(split[0]);
                double rp = Double.parseDouble(split[1]);
                return List.of((Math.abs(rc - rp) < threshold ? "1" : "0") + ",1");
            });
            String agg = diff.fold("0,0", (v1, v2) -> {
                String[] s1 = v1.split(","), s2 = v2.split(",");
                return (Integer.parseInt(s1[0]) + Integer.parseInt(s2[0])) + "," + (Integer.parseInt(s1[1]) + Integer.parseInt(s2[1]));
            });
            String[] parts = agg.split(",");
            int conv = Integer.parseInt(parts[0]), tot = Integer.parseInt(parts[1]);
            if (tot > 0 && conv * 100.0 / tot >= percentage) break;
        }

        state.flatMap(p -> {
            String[] split = p._2().split(",");
            double rc = Double.parseDouble(split[0]);
            context.getKVS().put("pt-pageranks", p._1(), "rank", String.valueOf(rc));
            return new ArrayList<String>();
        });
    }
}
