package cis5550.crawler;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.LanguageFilter;
import cis5550.tools.URLParser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.time.Instant;

public class Crawler {
	private static final int MAX_CONTENT_SIZE = 5 * 1024 * 1024;

	// --- crawl-shaping limits (slides 13/15: balance coverage between sites, avoid getting
	// "sucked into" one site / spider traps, bias toward breadth) ---
	// Max pages crawled per host PER FLAME-WORKER JVM (so the rough global cap is this times the
	// number of Flame workers). Persists across crawl iterations within a worker's JVM.
	private static final int MAX_PAGES_PER_HOST = 300;
	// Drop very deep URLs (a sign of a deep crawl getting stuck): more path segments than this.
	private static final int MAX_PATH_DEPTH = 12;
	// Per page: how many out-links to add to the frontier, and how many anchor-text rows to write to
	// pt-anchors. Link-heavy pages (Wikipedia portals etc.) have thousands of links — one pt-anchors
	// PUT per distinct target swamped a single hotspot KVS worker and stalled the whole crawl.
	private static final int MAX_LINKS_FOLLOWED = 250;
	private static final int MAX_ANCHORS_RECORDED = 60;
	private static final ConcurrentHashMap<String, AtomicInteger> HOST_PAGE_COUNT = new ConcurrentHashMap<>();
	// Per-host lock so the rate-limit "read lastAccessed / claim a slot" is atomic. Without it,
	// concurrent threads read the same stale timestamp and all fetch at once — a politeness bug.
	private static final ConcurrentHashMap<String, Object> HOST_LOCKS = new ConcurrentHashMap<>();
	// Query-string params that carry no content (tracking / session ids) — stripped during URL
	// normalization so the same page under different params isn't crawled many times (slide 16).
	private static final Set<String> TRACKING_PARAMS = new HashSet<>(Arrays.asList(
			"utm_source","utm_medium","utm_campaign","utm_term","utm_content","utm_id","utm_name",
			"utm_reader","utm_referrer","fbclid","gclid","gclsrc","dclid","msclkid","mc_cid","mc_eid",
			"yclid","_ga","_gl","igshid","ref","ref_src","referrer","source","src","campaign",
			"sessionid","session_id","sid","phpsessid","jsessionid","aspsessionid","cfid","cftoken",
			"replytocom","share","sharer","shared","amp","from","spm","cmpid","ncid","__twitter_impression"));

	private static final boolean DIAG_ENABLED = isDiagEnabled();

	private static boolean isDiagEnabled() {
		String prop = System.getProperty("cerebro.diag");
		if (prop == null || prop.isEmpty()) {
			prop = System.getenv("CEREBRO_DIAG");
		}
		if (prop == null) {
			return false;
		}
		prop = prop.trim().toLowerCase(Locale.ROOT);
		return prop.equals("1") || prop.equals("true") || prop.equals("yes") || prop.equals("on");
	}

	private static String jsonEscape(String s) {
		if (s == null) return "";
		StringBuilder out = new StringBuilder(s.length() + 16);
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			switch (c) {
				case '\\': out.append("\\\\"); break;
				case '"': out.append("\\\""); break;
				case '\n': out.append("\\n"); break;
				case '\r': out.append("\\r"); break;
				case '\t': out.append("\\t"); break;
				default:
					if (c < 0x20) {
						out.append(String.format("\\u%04x", (int) c));
					} else {
						out.append(c);
					}
			}
		}
		return out.toString();
	}

	private static synchronized void diag(String phase, int iter, String status, String metricsJson, String message) {
		if (!DIAG_ENABLED) {
			return;
		}


		StringBuilder sb = new StringBuilder();
		sb.append("{\"tsWall\":\"").append(Instant.now().toString()).append("\"");
		sb.append(",\"component\":\"CRAWLER\"");
		sb.append(",\"phase\":\"").append(phase).append("\"");
		sb.append(",\"iter\":").append(iter);
		sb.append(",\"status\":\"").append(status).append("\"");
		if (metricsJson != null && !metricsJson.isEmpty()) {
			sb.append(",\"metrics\":").append(metricsJson);
		}
		if (message != null) {
			sb.append(",\"message\":\"").append(jsonEscape(message)).append("\"");
		}
		sb.append("}");
		System.err.println(sb);
	}

	private static String iterationMetricsJson(long elapsedMs, long frontierIn, long frontierOut, long countBeforeMs, long flatMapMs, long countAfterMs) {
		return "{\"elapsedMs\":" + elapsedMs +
				",\"frontierIn\":" + frontierIn +
				",\"frontierOut\":" + frontierOut +
				",\"countBeforeMs\":" + countBeforeMs +
				",\"flatMapMs\":" + flatMapMs +
				",\"countAfterMs\":" + countAfterMs + "}";
	}

	public static void run(FlameContext ctx, String[] args) throws Exception {
		if (args.length < 1) {
			ctx.output("ERROR: please provide at least one seed URL");
			return;
		}

		// Parse arguments: seed URLs, optional --concurrency=N, optional blacklist table
		List<String> seedURLs = new ArrayList<>();
		String blacklistTable = null;
		int concurrencyMultiplier = 4;  // default

		for (String arg : args) {
			if (arg.startsWith("http://") || arg.startsWith("https://")) {
				seedURLs.add(arg);
			} else if (arg.startsWith("--concurrency=")) {
				try {
					int value = Integer.parseInt(arg.substring("--concurrency=".length()).trim());
					if (value > 0) {
						concurrencyMultiplier = value;
					}
				} catch (NumberFormatException e) {
					ctx.output("WARNING: invalid --concurrency value, using default 4");
				}
			} else {
				// Non-URL, non-option argument: blacklist table name
				blacklistTable = arg;
			}
		}

		if (seedURLs.isEmpty()) {
			ctx.output("ERROR: please provide at least one seed URL");
			return;
		}

		// Load blacklist patterns if provided
		List<Pattern> blacklistPatterns = new ArrayList<>();
		if (blacklistTable != null) {
			KVSClient kvsInit = ctx.getKVS();
			try {
				var rows = kvsInit.scan(blacklistTable);
				while (rows.hasNext()) {
					Row r = rows.next();
					String pattern = r.get("pattern");
					if (pattern != null) {
						blacklistPatterns.add(compileGlob(pattern));
					}
				}
			} catch (Exception e) {
				// blacklist table not found or empty — proceed without it
			}
		}
		List<Pattern> blacklist = blacklistPatterns;

		// Normalize all seed URLs
		List<String> normalizedSeeds = new ArrayList<>();
		for (String seed : seedURLs) {
			String normalized = normalizeURL(seed, seed);
			if (normalized != null) {
				normalizedSeeds.add(normalized);
			}
		}
		ctx.output("seed URLs: " + String.join(", ", normalizedSeeds));

		// Set concurrency level: multiplier × numWorkers
		// Higher multiplier values create more parallel fetch tasks but can strain KVS write throughput
		ctx.setConcurrencyLevel(concurrencyMultiplier * ctx.getKVS().numWorkers());
		ctx.output("concurrency: " + concurrencyMultiplier + " × " + ctx.getKVS().numWorkers() + " workers = " + (concurrencyMultiplier * ctx.getKVS().numWorkers()));

		// Restartability (handout: make the crawler restartable so you can stop/resume): if pt-crawl
		// already has pages, the seeds alone are useless (they're already crawled → skipped → empty
		// frontier → instant stop). Rebuild the frontier from the out-links of every already-crawled
		// page, minus what's already crawled. Falls back to the seeds if that comes up empty.
		FlameRDD urlQueue;
		long alreadyCrawled = 0;
		try { alreadyCrawled = ctx.getKVS().count("pt-crawl"); } catch (Exception e) { /* table may not exist */ }
		if (alreadyCrawled > 0) {
			ctx.output("resuming: pt-crawl already has ~" + alreadyCrawled + " rows; rebuilding the frontier from their out-links");
			final int REBUILD_PAGE_CAP = 300_000;   // links live near the top; keep the small Flame workers from OOMing on 5 MB pages
			FlameRDD packedCrawl = ctx.fromTable("pt-crawl", row -> {
				String u = row.get("url");
				String p = row.get("page");
				if (u == null || p == null) return null;
				if (p.length() > REBUILD_PAGE_CAP) p = p.substring(0, REBUILD_PAGE_CAP);
				return u + ((char) 2) + p;
			});
			FlameRDD rebuilt = packedCrawl.flatMap(packed -> {
				int sep = packed.indexOf((char) 2);
				if (sep < 0) return new ArrayList<>();
				String base = packed.substring(0, sep);
				String page = packed.substring(sep + 1);
				List<String> out = new ArrayList<>();
				for (String[] a : extractAnchors(page)) {
					String n = normalizeURL(base, a[0]);
					if (n != null && (blacklist.isEmpty() || !matchesBlacklist(n, blacklist))) {
						out.add(n);
						if (out.size() >= MAX_LINKS_FOLLOWED) break;
					}
				}
				return out;
			}).distinct().filter(u -> {
				try { return !ctx.getKVS().existsRow("pt-crawl", Hasher.hash(u)); } catch (Exception e) { return true; }
			});
			packedCrawl.destroy();
			if (rebuilt.count() > 0) {
				urlQueue = rebuilt;
			} else {
				rebuilt.destroy();
				urlQueue = ctx.parallelize(normalizedSeeds);
			}
		} else {
			urlQueue = ctx.parallelize(normalizedSeeds);
		}
		long crawlStartMs = System.currentTimeMillis();
		int iter = 0;

		// Max URLs to process per iteration to stop the frontier from exploding
		// into too many entries that would make each iteration take a long time.
		final int FRONTIER_CAP = 50_000;

		while (true){ // keep looping as long as there are URLs in the queue
			long countBeforeStart = System.currentTimeMillis();
			long frontierIn = urlQueue.count();
			long countBeforeMs = System.currentTimeMillis() - countBeforeStart;
			if (frontierIn == 0) {
				break;
			}

			final int iterId = iter;
			FlameRDD oldQueue = urlQueue;
			long flatMapStart = System.currentTimeMillis();
			FlameRDD rawFrontier = oldQueue.flatMap(url -> { // for each URL in the queue, run this lambda
				try {
				String pageContent = null;
				byte[] pageBytes = null;
				java.util.Set<String> normalizedURLs = new java.util.HashSet<>();
				KVSClient kvs = ctx.getKVS(); //handle to the KVS

				// Check blacklist
				if (!blacklist.isEmpty() && matchesBlacklist(url, blacklist)) {
					return new ArrayList<>();
				}

				if (kvs.existsRow("pt-crawl", Hasher.hash(url))) {
					return new ArrayList<String>();   // url was crawled already
				}

				URL urlObj = URI.create(url).toURL();
				String host = urlObj.getHost();
				if (host == null || host.isEmpty()) return new ArrayList<>();

				// Don't get "sucked into" one site / a spider trap: cap pages per host (per Flame-
				// worker JVM, so the rough global cap is this x #Flame-workers). Counts attempts,
				// not just successes — a host full of 404s/non-HTML stops being retried too.
				if (HOST_PAGE_COUNT.computeIfAbsent(host, k -> new AtomicInteger()).incrementAndGet() > MAX_PAGES_PER_HOST) {
					return new ArrayList<>();
				}

				// Rate limiting: don't hit a host more often than its delay (default 1s, or
				// Crawl-delay from robots.txt). The read-and-claim is done under a per-host lock so
				// concurrent threads stagger their slots instead of all reading the same stale time.
				long delayMs = 1000;
				byte[] crawlDelayBytes = kvs.get("pt-hosts", host, "crawlDelay");
				if (crawlDelayBytes != null) {
					try {
						double parsedDelay = Double.parseDouble(new String(crawlDelayBytes).trim());
						if (parsedDelay > 0) {
							delayMs = (long) Math.min(parsedDelay * 1000, 30_000);   // cap absurd Crawl-delay
						}
					} catch (NumberFormatException e) {
						// Malformed crawlDelay; use default
					}
				}
				long sleepMs = 0;
				Object hostLock = HOST_LOCKS.computeIfAbsent(host, k -> new Object());
				synchronized (hostLock) {
					long nowMs = System.currentTimeMillis();
					long earliestAllowed = nowMs;
					byte[] lastAccessBytes = kvs.get("pt-hosts", host, "lastAccessed");
					if (lastAccessBytes != null) {
						try {
							long prev = Long.parseLong(new String(lastAccessBytes).trim());
							earliestAllowed = Math.max(nowMs, prev + delayMs);
						} catch (NumberFormatException e) {
							// Malformed timestamp; proceed now
						}
					}
					kvs.put("pt-hosts", host, "lastAccessed", String.valueOf(earliestAllowed));
					sleepMs = earliestAllowed - nowMs;
				}
				if (sleepMs > 0) {
					Thread.sleep(sleepMs);
				}

				// Fetch robots.txt on first visit to this host.
				// The `robots` column is both the gate and the data: it's written exactly
				// once, at the end of the fetch block, so a null read means nobody has
				// finished fetching yet. Concurrent workers may redundantly fetch — that's
				// idempotent and safe. Empty content = "checked, no restrictions apply".
				byte[] robotsBytes = kvs.get("pt-hosts", host, "robots");
				if (robotsBytes == null) {
					String robotsContent = "";
					HttpURLConnection robotsConn = null;
					try {
						int robotsPort = urlObj.getPort();
						String robotsURL = urlObj.getProtocol() + "://" + host + (robotsPort != -1 ? ":" + robotsPort : "") + "/robots.txt";
						robotsConn = (HttpURLConnection) URI.create(robotsURL).toURL().openConnection();
						robotsConn.setRequestMethod("GET");
						robotsConn.setRequestProperty("User-Agent", "cis5550-crawler");
						robotsConn.setConnectTimeout(5000);
						robotsConn.setReadTimeout(10000);
						if (robotsConn.getResponseCode() == 200) {
							InputStream ris = robotsConn.getInputStream();
							robotsContent = new String(ris.readAllBytes());
							ris.close();
						}
					} catch (Exception e) {
						// robots.txt fetch failed — treat as no restrictions (empty content)
					} finally {
						if (robotsConn != null) robotsConn.disconnect();
					}
					// Parse Crawl-delay first, then write `robots` last — its presence is
					// the "fetch complete" signal for concurrent workers.
					if (!robotsContent.isEmpty()) {
						String crawlDelay = parseCrawlDelay(robotsContent);
						if (crawlDelay != null) {
							kvs.put("pt-hosts", host, "crawlDelay", crawlDelay);
						}
					}
					kvs.put("pt-hosts", host, "robots", robotsContent);
					robotsBytes = robotsContent.getBytes();
				}

				// Check robots.txt rules (empty bytes = checked, no restrictions apply)
				if (robotsBytes.length > 0) {
					String robotsContent = new String(robotsBytes);
					if (!isAllowedByRobots(robotsContent, urlObj.getPath())) {
						return new ArrayList<>();
					}
				}

				// GET request with inline redirect handling (replaces old HEAD+GET sequence)
				int maxRedirects = 5;
				int redirectCount = 0;
				String currentURL = url;
				URL currentObj = urlObj;
				int responseCode = -1;
				String contentType = null;
				String contentLength = null;
				String contentLanguage = null;
				HttpURLConnection conn = null;
				boolean tooLarge = false;

				while (redirectCount <= maxRedirects) {
					conn = (HttpURLConnection) currentObj.openConnection();
					conn.setInstanceFollowRedirects(false);
					conn.setRequestMethod("GET");
					conn.setRequestProperty("User-Agent", "cis5550-crawler");
					conn.setConnectTimeout(5000);
					conn.setReadTimeout(10000);

					String rowKey = Hasher.hash(currentURL);

					try {
						responseCode = conn.getResponseCode();
					} catch (IOException e) {
						conn.disconnect();
						System.err.println("GET failed for " + currentURL + ": " + e.getMessage());
						kvs.put("pt-crawl", rowKey, "url", currentURL);
						kvs.put("pt-crawl", rowKey, "responseCode", "-1");
						return new ArrayList<>();
					}

					kvs.put("pt-crawl", rowKey, "url", currentURL);
					kvs.put("pt-crawl", rowKey, "responseCode", String.valueOf(responseCode));

					// Handle redirects — read Location header but DON'T read body
					if (responseCode == 301 || responseCode == 302 || responseCode == 303 || responseCode == 307 || responseCode == 308) {
						String newURL = conn.getHeaderField("Location");
						conn.disconnect();
						newURL = normalizeURL(currentURL, newURL);
						if (newURL == null) {
							return new ArrayList<>();
						}
						redirectCount++;
						if (redirectCount > maxRedirects) {
							return new ArrayList<>();
						}
						currentURL = newURL;
						currentObj = URI.create(newURL).toURL();
						continue;
					}

					// Non-redirect: read headers (available before body stream)
					contentType = conn.getContentType();
					contentLength = conn.getHeaderField("Content-Length");
					contentLanguage = conn.getHeaderField("Content-Language");
					if (contentLength != null) kvs.put("pt-crawl", rowKey, "length", contentLength);
					if (contentType != null) kvs.put("pt-crawl", rowKey, "contentType", contentType);

					// Bail early if not HTML (before reading body)
					if (responseCode != 200 || contentType == null || !contentType.contains("text/html")) {
						conn.disconnect();
						return new ArrayList<>();
					}

					// Language filter — stage 1: reject on Content-Language header before body download
					String headerLang = LanguageFilter.parseContentLanguage(contentLanguage);
					if (headerLang != null && !headerLang.isEmpty() && !LanguageFilter.isEnglishTag(headerLang)) {
						kvs.put("pt-crawl", rowKey, "lang", headerLang);
						conn.disconnect();
						return new ArrayList<>();
					}

					// Reject early if Content-Length > 5 MB (before reading body)
					if (contentLength != null) {
						try {
							long len = Long.parseLong(contentLength);
							if (len > MAX_CONTENT_SIZE) {
								conn.disconnect();
								return new ArrayList<>();
							}
						} catch (NumberFormatException e) {
							// Malformed Content-Length; proceed with stream-size cutoff
						}
					}

					// All checks passed — now read the body (with size cutoff for chunked encoding)
					try {
						InputStream is = conn.getInputStream();
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						byte[] buffer = new byte[8192];
						int totalRead = 0;
						int bytesRead;
						while ((bytesRead = is.read(buffer)) != -1) {
							totalRead += bytesRead;
							if (totalRead > MAX_CONTENT_SIZE) {
								tooLarge = true;
								break;
							}
							baos.write(buffer, 0, bytesRead);
						}
						is.close();
						pageBytes = baos.toByteArray();
					} catch (IOException e) {
						System.err.println("GET body read failed for " + currentURL + ": " + e.getMessage());
						conn.disconnect();
						return new ArrayList<>();
					}

					if (tooLarge) {
						conn.disconnect();
						return new ArrayList<>();
					}

					conn.disconnect();
					break;  // Success — exit redirect loop
				}

				// Use final destination after redirects
				String rowKey = Hasher.hash(currentURL);
				pageContent = new String(pageBytes);

				// Language filter — stages 2 and 3: <html lang> + stopword heuristic
				LanguageFilter.Decision langDecision = LanguageFilter.decide(contentLanguage, pageContent);
				kvs.put("pt-crawl", rowKey, "lang", langDecision.lang);
				if (!langDecision.accept) {
					// Connection already disconnected at line 463 after successful read
					return new ArrayList<>();
				}

				// Content-seen test: hash the page content and check for duplicates
				MessageDigest md = MessageDigest.getInstance("SHA-256");
				byte[] contentHash = md.digest(pageBytes);
				StringBuilder hexString = new StringBuilder();
				for (byte b : contentHash) {
					hexString.append(String.format("%02x", b));
				}
				String hashStr = hexString.toString();

				byte[] existingURL = kvs.get("pt-content-seen", hashStr, "url");
				if (existingURL != null) {
					// Duplicate content — store canonicalURL instead of page
					kvs.put("pt-crawl", rowKey, "canonicalURL", new String(existingURL));
				} else {
					// New content — record it and store the page
					kvs.put("pt-content-seen", hashStr, "url", currentURL);
					kvs.put("pt-crawl", rowKey, "page", pageContent);
				}

				// Extract URLs and anchor text. Aggregate anchor text in memory by target URL,
				// then write one PUT per target after the loop. The column "anchor:" + currentURL
				// is unique per source page, so within a page no GET/merge is needed; cross-page
				// merging happens in the post-crawl scan that copies pt-anchors -> pt-crawl.
				// Frontier "uncrawled" filtering is handled downstream by .filter() on the RDD.
				List<String[]> anchors = extractAnchors(pageContent);
				java.util.Map<String, java.util.List<String>> anchorTextByTarget = new java.util.HashMap<>();
				for (String[] anchor : anchors) {
					if (normalizedURLs.size() >= MAX_LINKS_FOLLOWED && anchorTextByTarget.size() >= MAX_ANCHORS_RECORDED) break;
					String link = anchor[0];
					String text = anchor[1];
					String normalized = normalizeURL(currentURL, link);
					if (normalized != null) {
						if ((blacklist.isEmpty() || !matchesBlacklist(normalized, blacklist)) && normalizedURLs.size() < MAX_LINKS_FOLLOWED) {
							normalizedURLs.add(normalized);
						}
						if (text != null && !text.trim().isEmpty()
								&& (anchorTextByTarget.containsKey(normalized) || anchorTextByTarget.size() < MAX_ANCHORS_RECORDED)) {
							anchorTextByTarget
								.computeIfAbsent(normalized, k -> new ArrayList<>())
								.add(text.trim());
						}
					}
				}

				if (!anchorTextByTarget.isEmpty()) {
					String anchorCol = "anchor:" + currentURL;
					for (java.util.Map.Entry<String, java.util.List<String>> e : anchorTextByTarget.entrySet()) {
						String targetKey = Hasher.hash(e.getKey());
						String joined = String.join(" ", e.getValue());
						kvs.put("pt-anchors", targetKey, anchorCol, joined);
					}
				}

				// Connection already disconnected at line 463 after successful read
				return normalizedURLs;
				} catch (Exception e) {
					System.err.println("Crawler exception for " + url + ": " + e.getMessage());
					e.printStackTrace();
					return new ArrayList<>();
				}
			});
			long flatMapMs = System.currentTimeMillis() - flatMapStart;
			oldQueue.destroy();

			// Collapse the multiset emitted by flatMap so each unique URL appears as one row.
			FlameRDD deduped = rawFrontier.distinct();
			rawFrontier.destroy();

			// Drop URLs already in pt-crawl. Runs on the Flame worker side via /rdd/filter so the
			// existsRow cost is per-unique-URL, not per anchor edge.
			FlameRDD filtered = deduped.filter(u -> {
				try {
					return !ctx.getKVS().existsRow("pt-crawl", Hasher.hash(u));
				} catch (Exception e) {
					return true;
				}
			});
			deduped.destroy();

			long countAfterStart = System.currentTimeMillis();
			long frontierOut = filtered.count();
			long countAfterMs = System.currentTimeMillis() - countAfterStart;

			// Cap unique uncrawled URLs (not raw rows) so each iteration's work is bounded.
			if (frontierOut > FRONTIER_CAP) {
				FlameRDD oversized = filtered;
				filtered = oversized.sample((double) FRONTIER_CAP / frontierOut);
				oversized.destroy();
				frontierOut = filtered.count();
			}

			urlQueue = filtered;

			diag("ITERATION", iterId, "ok",
					iterationMetricsJson(System.currentTimeMillis() - crawlStartMs, frontierIn, frontierOut, countBeforeMs, flatMapMs, countAfterMs),
					null);
			iter++;
		}
		urlQueue.destroy();

		// Post-crawl: merge anchor text from staging table into pt-crawl
		try {
			KVSClient kvsPost = ctx.getKVS();
			java.util.Iterator<Row> anchorIter = kvsPost.scan("pt-anchors", null, null);
			while (anchorIter.hasNext()) {
				Row ar = anchorIter.next();
				String key = ar.key();
				// Only add anchor text to rows that were actually crawled
				if (kvsPost.get("pt-crawl", key, "url") != null) {
					for (String col : ar.columns()) {
						kvsPost.put("pt-crawl", key, col, ar.get(col));
					}
				}
			}
		} catch (Exception e) {
			// pt-anchors table may not exist
		}
	}

	public static String normalizeURL (String baseURL, String link){
		if (link.startsWith("#")) return null;

		String[] parsedBase = URLParser.parseURL(baseURL);
		String[] parsedLink = URLParser.parseURL(link);

		String protocol, host, port, path;

		if (link.startsWith("http://") || link.startsWith("https://")) {
			// Absolute URL — use everything from the link
			protocol = parsedLink[0];
			host = parsedLink[1];
			port = parsedLink[2];
			path = parsedLink[3];
		} else if (link.startsWith("/")) {
			// Absolute path — inherit protocol/host/port from base
			protocol = parsedBase[0];
			host = parsedBase[1];
			port = parsedBase[2];
			path = parsedLink[3];
		} else {
			// Relative path — resolve against the base URL's directory
			protocol = parsedBase[0];
			host = parsedBase[1];
			port = parsedBase[2];
			String basePath = parsedBase[3];
			int lastSlash = basePath.lastIndexOf('/');
			String baseDir = (lastSlash >= 0) ? basePath.substring(0, lastSlash + 1) : "/";
			path = baseDir + parsedLink[3];
			while (path.contains("/../")) {
				int dotdot = path.indexOf("/../");
				int prevSlash = path.lastIndexOf('/', dotdot - 1);
				if (prevSlash < 0) {
					// Path begins with "/.." — no segment to consume; collapse to remainder.
					path = path.substring(dotdot + 3);
				} else {
					path = path.substring(0, prevSlash) + path.substring(dotdot + 3);
				}
			}
		}

		if (protocol != null) protocol = protocol.toLowerCase();
		if (host     != null) host     = host.toLowerCase();

		// Set default port if not specified
		if (port == null) {
			if ("http".equals(protocol)) port = "80";
			else if ("https".equals(protocol)) port = "443";
		}

		if (path == null || path.isEmpty()) path = "/";

		// Strip fragment
		if (path.contains("#")) {
			path = path.substring(0, path.indexOf('#'));
		}
		if (protocol == null || (!protocol.equals("http") && !protocol.equals("https"))) return null;
		if (host == null || host.isEmpty()) return null;

		// Drop tracking / session params so the same page under different params isn't a new URL.
		path = stripTrackingParams(path);

		// Breadth bias: drop very deep paths (a sign of a deep crawl getting stuck in one site).
		int qi = path.indexOf('?');
		String pathOnly = (qi >= 0) ? path.substring(0, qi) : path;
		int depth = 0;
		for (int i = 0; i < pathOnly.length(); i++) if (pathOnly.charAt(i) == '/') depth++;
		if (depth > MAX_PATH_DEPTH) return null;

		return protocol + "://" + host + ":" + port + path;
	}

	// Remove well-known tracking/session query parameters; if nothing meaningful is left, drop the
	// whole query string. Keeps content-bearing params (page numbers, ids, etc.).
	private static String stripTrackingParams(String path) {
		int q = path.indexOf('?');
		if (q < 0) return path;
		String base = path.substring(0, q);
		String query = path.substring(q + 1);
		StringBuilder kept = new StringBuilder();
		for (String kv : query.split("&")) {
			if (kv.isEmpty()) continue;
			int eq = kv.indexOf('=');
			String key = (eq >= 0 ? kv.substring(0, eq) : kv).toLowerCase();
			if (TRACKING_PARAMS.contains(key)) continue;
			if (kept.length() > 0) kept.append('&');
			kept.append(kv);
		}
		return (kept.length() > 0) ? base + "?" + kept : base;
	}

	public static List<String> extractURLs(String pageContent){
		List<String> links = new ArrayList<>();
		for (String[] anchor : extractAnchors(pageContent)) {
			links.add(anchor[0]);
		}
		return links;
	}

	// Returns the position right after the closing tag for a <script>/<style> region
	// starting at openTagStart, or -1 if the open tag isn't actually a real tag.
	private static int endOfBlockedRegion(String lower, int openTagStart, String openTag, String closeTag) {
		int after = openTagStart + openTag.length();
		if (after >= lower.length()) return -1;
		char c = lower.charAt(after);
		if (c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '>') return -1;
		int end = lower.indexOf(closeTag, after);
		return (end < 0) ? lower.length() : end + closeTag.length();
	}

	// Rejects hrefs that are obviously not URLs (JS template residue, control chars, etc.)
	private static boolean looksLikeJunkHref(String href) {
		if (href == null || href.isEmpty()) return true;
		if (href.contains("' +") || href.contains("\" +")) return true;
		if (href.contains("+ '") || href.contains("+ \"")) return true;
		if (href.contains("${") || href.indexOf('`') >= 0) return true;
		for (int i = 0; i < href.length(); i++) {
			char c = href.charAt(i);
			if (c <= 0x20 || c == '<' || c == '>' || c == '{' || c == '}') return true;
		}
		return false;
	}

	// Returns list of [href, anchorText] pairs
	public static List<String[]> extractAnchors(String pageContent) {
		List<String[]> anchors = new ArrayList<>();
		String lower = pageContent.toLowerCase();
		int idx = 0;
		while (idx < lower.length()) {
			// Find opening <a tag
			int aStart = lower.indexOf("<a", idx);
			if (aStart == -1) break;
			// Skip past any <script>/<style> region that begins before this <a.
			int scriptStart = lower.indexOf("<script", idx);
			if (scriptStart != -1 && scriptStart < aStart) {
				int skipTo = endOfBlockedRegion(lower, scriptStart, "<script", "</script>");
				if (skipTo > 0) { idx = skipTo; continue; }
			}
			int styleStart = lower.indexOf("<style", idx);
			if (styleStart != -1 && styleStart < aStart) {
				int skipTo = endOfBlockedRegion(lower, styleStart, "<style", "</style>");
				if (skipTo > 0) { idx = skipTo; continue; }
			}
			// Make sure it's actually an <a tag (followed by space or >)
			int afterA = aStart + 2;
			if (afterA < lower.length() && lower.charAt(afterA) != ' ' && lower.charAt(afterA) != '\t' && lower.charAt(afterA) != '\n' && lower.charAt(afterA) != '\r' && lower.charAt(afterA) != '>') {
				idx = afterA;
				continue;
			}
			int tagEnd = pageContent.indexOf('>', aStart);
			if (tagEnd == -1) break;
			String tagContent = pageContent.substring(aStart + 1, tagEnd); // content inside < >

			// Extract href
			String href = null;
			String tagLower = tagContent.toLowerCase();
			int hrefIdx = tagLower.indexOf("href=");
			if (hrefIdx != -1) {
				int valStart = hrefIdx + 5;
				if (valStart < tagContent.length()) {
					char quote = tagContent.charAt(valStart);
					if (quote == '"' || quote == '\'') {
						int valEnd = tagContent.indexOf(quote, valStart + 1);
						if (valEnd != -1) {
							href = tagContent.substring(valStart + 1, valEnd);
						}
					} else {
						// No quotes — take until space or end
						int valEnd = valStart;
						while (valEnd < tagContent.length() && tagContent.charAt(valEnd) != ' ' && tagContent.charAt(valEnd) != '\t' && tagContent.charAt(valEnd) != '>') {
							valEnd++;
						}
						href = tagContent.substring(valStart, valEnd);
					}
				}
			}

			// Find closing </a> and extract anchor text
			int closeTag = lower.indexOf("</a", tagEnd);
			String anchorText = "";
			if (closeTag != -1) {
				anchorText = pageContent.substring(tagEnd + 1, closeTag).replaceAll("<[^>]*>", "").trim();
				idx = closeTag + 4;
			} else {
				idx = tagEnd + 1;
			}

			if (href != null && !looksLikeJunkHref(href)) {
				anchors.add(new String[]{href, anchorText});
			}
		}
		return anchors;
	}

	public static Pattern compileGlob(String pattern) {
		String[] parts = pattern.split("\\*", -1);
		StringBuilder regex = new StringBuilder();
		for (int i = 0; i < parts.length; i++) {
			if (i > 0) regex.append(".*");
			if (!parts[i].isEmpty()) regex.append(Pattern.quote(parts[i]));
		}
		return Pattern.compile(regex.toString());
	}

	public static boolean matchesBlacklist(String url, List<Pattern> patterns) {
		for (Pattern p : patterns) {
			if (p.matcher(url).matches()) return true;
		}
		return false;
	}

	public static boolean isAllowedByRobots(String robotsContent, String path) {
		List<String> specificRules = new ArrayList<>();
		List<String> wildcardRules = new ArrayList<>();
		boolean inSpecific = false;
		boolean inWildcard = false;
		boolean hasSpecific = false;

		String[] lines = robotsContent.split("\n");
		for (String line : lines) {
			line = line.trim();
			if (line.startsWith("#") || line.isEmpty()) continue;

			if (line.toLowerCase().startsWith("user-agent:")) {
				String agent = line.substring(11).trim().toLowerCase();
				if (agent.equals("cis5550-crawler")) {
					inSpecific = true;
					inWildcard = false;
					hasSpecific = true;
				} else if (agent.equals("*")) {
					inWildcard = true;
					inSpecific = false;
				} else {
					inSpecific = false;
					inWildcard = false;
				}
			} else if (line.toLowerCase().startsWith("allow:") || line.toLowerCase().startsWith("disallow:")) {
				if (inSpecific) {
					specificRules.add(line);
				} else if (inWildcard) {
					wildcardRules.add(line);
				}
			}
		}

		// Use specific rules if any exist, otherwise fall back to wildcard
		List<String> rules = hasSpecific ? specificRules : wildcardRules;

		// First matching rule wins (prefix match)
		for (String rule : rules) {
			boolean isAllow = rule.toLowerCase().startsWith("allow:");
			String prefix = rule.substring(rule.indexOf(':') + 1).trim();
			if (prefix.isEmpty()) continue;
			if (path.startsWith(prefix)) {
				return isAllow;
			}
		}

		// No matching rule — allowed by default
		return true;
	}

	public static String parseCrawlDelay(String robotsContent) {
		// Same logic as isAllowedByRobots: prefer cis5550-crawler rules, fall back to *
		String specificDelay = null;
		String wildcardDelay = null;
		boolean inSpecific = false;
		boolean inWildcard = false;
		boolean hasSpecific = false;

		String[] lines = robotsContent.split("\n");
		for (String line : lines) {
			line = line.trim();
			if (line.startsWith("#") || line.isEmpty()) continue;

			if (line.toLowerCase().startsWith("user-agent:")) {
				String agent = line.substring(11).trim().toLowerCase();
				if (agent.equals("cis5550-crawler")) {
					inSpecific = true;
					inWildcard = false;
					hasSpecific = true;
				} else if (agent.equals("*")) {
					inWildcard = true;
					inSpecific = false;
				} else {
					inSpecific = false;
					inWildcard = false;
				}
			} else if (line.toLowerCase().startsWith("crawl-delay:")) {
				String val = line.substring(12).trim();
				if (inSpecific) {
					specificDelay = val;
				} else if (inWildcard) {
					wildcardDelay = val;
				}
			}
		}

		if (hasSpecific) return specificDelay;
		return wildcardDelay;
	}
}
