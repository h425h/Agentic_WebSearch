package cis5550.jobs;

import java.util.ArrayList;
import java.util.List;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.pagerank.PageRank;
import cis5550.tools.Hasher;

public class AnchorExtractor {
    public static void run(FlameContext context, String[] args) throws Exception {
        FlameRDD data = context.fromTable("pt-crawl", row -> {
            String url = row.get("url");
            String page = row.get("page");
            if (url == null || page == null) { return null; }
            return url; 
        }).filter(d -> d != null);


        // for evevery page, find all anchor tags, and save the (dest URL, text)
        FlamePairRDD pairs = data.flatMapToPair(url -> {
            KVSClient kvs = context.getKVS();
            String page = new String(kvs.get("pt-crawl", Hasher.hash(url), "page"));

            List<FlamePair> res = new ArrayList<>();
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                "<a\\s+[^>]*href=[\"']([^\"']*)[\"'][^>]*>(.*?)</a>",
                java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.DOTALL
            );
            java.util.regex.Matcher matcher = pattern.matcher(page);

            while (matcher.find()) {
                String link = matcher.group(1).trim();
                // strip nested html and trim whitespace
                String anchorText = matcher.group(2).replaceAll("<.*?>", "").trim();
                if (!link.isEmpty() && !anchorText.isEmpty()) {
                    int hashInd = link.indexOf("#");
                    // strip off url fragment
                    if (hashInd != -1) {
                        link = link.substring(0, hashInd);
                    }
                    if (!link.isEmpty()) {
                        try {
                            String normalized = PageRank.normalizeURL(url, link);
                            res.add(new FlamePair(normalized, anchorText));
                        } catch (Exception e) {

                        }
                    }
                }
            }
            return res;
        });

        // combine all anchor texts pointing to the same url
        FlamePairRDD combine = pairs.foldByKey("", (a, b) -> {
            // combine anchor texts
            if (a.isEmpty()) {
                return b;
            } if (b.isEmpty()) {
                return a;
            }
            return a + " " + b;
        });

        // save to kvs.
        combine.flatMapToPair(p -> {
            String destUrl = p._1();
            String anchorText = p._2();

            context.getKVS().put("pt-anchors", Hasher.hash(destUrl), "text", anchorText);
            return new ArrayList<>();
        });
    }    
}
