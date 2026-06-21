package cis5550.crawler;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class BlacklistSmokeTest {

	static int pass = 0, fail = 0;
	static List<String> failures = new ArrayList<>();

	public static void main(String[] args) throws Exception {
		System.out.println("=== Unit: compileGlob shapes ===");
		runUnitTests();

		System.out.println();
		System.out.println("=== Simulation: real URLs vs config/blacklist.txt ===");
		runWebSimulation();

		System.out.println();
		System.out.printf("TOTAL: %d passed, %d failed%n", pass, fail);
		if (fail > 0) {
			System.out.println();
			System.out.println("--- Failures ---");
			for (String f : failures) System.out.println("  " + f);
		}
		System.exit(fail == 0 ? 0 : 1);
	}

	// -----------------------------------------------------------------
	// Unit tests: one pattern, one URL, exact expectation.
	// -----------------------------------------------------------------
	static void runUnitTests() {
		String[][] cases = {
			{"*.jpg",                      "http://x.com/a/b.jpg",              "true"},
			{"*.jpg",                      "http://x.com/page.html",            "false"},
			{"*.jpg",                      "http://x.com/a.jpg?q=1",            "false"},
			{"*.tar.gz",                   "http://x.com/dl/pkg.tar.gz",        "true"},
			{"*://www.youtube.com/*",      "https://www.youtube.com/watch?v=1", "true"},
			{"*://www.youtube.com/*",      "https://example.com/watch",         "false"},
			{"*mailto:*",                  "http://h/base/mailto:a@b",          "true"},
			{"*?PHPSESSID=*",              "http://x/p?PHPSESSID=abc",          "true"},
			{"*?PHPSESSID=*",              "http://x/p?foo=bar",                "false"},
			{"*://cdn.*",                  "https://cdn.example.com/a.js",      "true"},
			{"*://cdn.*",                  "https://static.example.com/cdn.js", "false"}, // "cdn." not after "://"
			{"*/login*",                   "https://site.com/user/login",       "true"},
			{"*/login*",                   "https://site.com/user/loginpage",   "true"},
			{"*/login*",                   "https://site.com/home",             "false"},
			{"*&utm_*",                    "http://x/p?a=1&utm_source=fb",      "true"},
			{"*.min.js",                   "https://x.com/lib.min.js",          "true"},
			{"*.min.js",                   "https://x.com/lib.js",              "false"},
		};
		for (String[] c : cases) {
			boolean got = Crawler.compileGlob(c[0]).matcher(c[1]).matches();
			boolean want = Boolean.parseBoolean(c[2]);
			record(got == want, String.format("%-28s vs %-45s -> %s (want %s)", c[0], c[1], got, want));
		}
	}

	// -----------------------------------------------------------------
	// Web simulation: load the actual blacklist file, compile all 200+
	// patterns, then run a curated set of realistic URLs through
	// matchesBlacklist. Each URL is tagged with whether it SHOULD be
	// blocked; the test reports which blacklist pattern triggered.
	// -----------------------------------------------------------------
	static void runWebSimulation() throws Exception {
		List<String> rawPatterns = loadBlacklist("config/blacklist.txt");
		List<Pattern> compiled = new ArrayList<>();
		for (String p : rawPatterns) compiled.add(Crawler.compileGlob(p));
		System.out.println("Loaded " + compiled.size() + " patterns.");
		System.out.println();

		Object[][] urls = {
			// --- real content pages that MUST NOT be blocked ---
			{"https://en.wikipedia.org/wiki/Cat",                                    false},
			{"https://en.wikipedia.org/wiki/Main_Page",                              false},
			{"https://news.ycombinator.com/item?id=12345",                           false},
			{"https://stackoverflow.com/questions/12345/how-to-parse-json",          false},
			{"https://arxiv.org/abs/2301.00000",                                     false},
			{"https://www.nytimes.com/2024/01/15/world/article.html",                false},
			{"https://www.bbc.co.uk/news/world-europe-67890",                        false},
			{"https://docs.python.org/3/library/os.html",                            false},
			{"https://github.com/anthropics/claude-code",                            false},
			{"https://blog.cloudflare.com/post-about-security/",                     false},
			{"https://www.python.org/",                                              false},
			{"https://en.wikipedia.org/wiki/Portable_Document_Format",               false}, // mentions PDF but URL has no .pdf
			{"https://arxiv.org/list/cs.LG/2024",                                    false},
			{"https://example.com/article-about-javascript-frameworks",              false}, // "javascript" in path, not scheme
			{"https://example.com/gallery",                                          false},

			// --- binary / image / media files that MUST be blocked ---
			{"https://upload.wikimedia.org/wikipedia/commons/cat.jpg",               true},
			{"https://example.com/photo.jpeg",                                       true},
			{"https://example.com/logo.png",                                         true},
			{"https://example.com/icon.svg",                                         true},
			{"https://example.com/animation.gif",                                    true},
			{"https://example.com/photo.webp",                                       true},
			{"https://arxiv.org/pdf/2301.00000.pdf",                                 true},
			{"https://example.com/report.doc",                                       true},
			{"https://example.com/spreadsheet.xlsx",                                 true},
			{"https://example.com/slides.pptx",                                      true},
			{"https://example.com/song.mp3",                                         true},
			{"https://example.com/podcast.ogg",                                      true},
			{"https://example.com/video.mp4",                                        true},
			{"https://example.com/clip.webm",                                        true},
			{"https://example.com/download.zip",                                     true},
			{"https://example.com/archive.tar.gz",                                   true},
			{"https://example.com/installer.exe",                                    true},
			{"https://example.com/package.deb",                                      true},
			{"https://example.com/font.woff2",                                       true},

			// --- code/asset files that MUST be blocked ---
			{"https://code.jquery.com/jquery-3.6.0.min.js",                          true},
			{"https://cdnjs.cloudflare.com/ajax/libs/lib/lib.js",                    true}, // *.js AND *.cloudflare...no actually no *.cloudflare pattern, just cloudfront. So this matches *.js
			{"https://example.com/styles.css",                                       true},
			{"https://example.com/app.tsx",                                          true},
			{"https://example.com/sourcemap.map",                                    true},
			{"https://example.com/data.json",                                        true},
			{"https://example.com/config.yaml",                                      true},

			// --- social / video platforms that MUST be blocked ---
			{"https://www.facebook.com/zuck",                                        true},
			{"https://facebook.com/pages/about",                                     true},
			{"https://www.instagram.com/user",                                       true},
			{"https://twitter.com/elonmusk",                                         true},
			{"https://x.com/some_user",                                              true},
			{"https://www.reddit.com/r/programming",                                 true},
			{"https://reddit.com/r/java/comments/abc",                               true},
			{"https://www.linkedin.com/in/someone",                                  true},
			{"https://www.youtube.com/watch?v=dQw4w9WgXcQ",                          true},
			{"https://youtu.be/dQw4w9WgXcQ",                                         true},
			{"https://www.tiktok.com/@user/video/123",                               true},
			{"https://medium.com/@author/article-slug",                              true},
			{"https://www.quora.com/What-is-X",                                      true},

			// --- e-commerce / search-engine / CDN blocked ---
			{"https://www.amazon.com/dp/B07PVCVBN7",                                 true},
			{"https://www.ebay.com/itm/12345",                                       true},
			{"https://www.walmart.com/ip/product/789",                               true},
			{"https://www.google.com/search?q=test",                                 true},
			{"https://www.bing.com/search?q=test",                                   true},
			{"https://duckduckgo.com/?q=test",                                       true},
			{"https://cdn.example.com/lib.v2",                                       true}, // *://cdn.*
			{"https://d111111abcdef8.cloudfront.net/image",                          true}, // *.cloudfront.net/*
			{"https://s3.amazonaws.com/mybucket/file",                               true},
			{"https://storage.googleapis.com/bucket/obj",                            true},

			// --- login / auth pages blocked ---
			{"https://site.com/login",                                               true},
			{"https://site.com/user/signin?next=home",                               true},
			{"https://site.com/auth/callback",                                       true},
			{"https://site.com/account/settings",                                    true},
			{"https://site.com/oauth/authorize",                                     true},

			// --- URL-trap / infinite-space patterns blocked ---
			{"https://site.com/calendar?month=5&year=2025",                          true},
			{"https://site.com/forum?sessionid=abc123",                              true},
			{"https://site.com/shop?PHPSESSID=xyz",                                  true},
			{"https://blog.example.com/post?utm_source=twitter&utm_medium=social",   true},
			{"https://wiki.example.com/article?action=edit&page=Main",               true},
			{"https://wiki.example.com/article?oldid=99999",                         true},
			{"https://site.com/page?ref=partner",                                    true},

			// --- non-HTTP scheme mangling blocked ---
			{"http://host.com/base/mailto:someone@example.com",                      true},
			{"http://host.com/base/javascript:void(0)",                              true},
			{"http://host.com/base/tel:+15551234",                                   true},

			// --- ad / tracking domains blocked ---
			{"https://doubleclick.net/pixel",                                        true},
			{"https://googleadservices.com/pagead/click",                            true},
			{"https://analytics.example.com/collect",                                true},

			// --- tricky negatives: URLs that look suspicious but should be allowed ---
			{"https://en.wikipedia.org/wiki/JavaScript",                             false}, // "javascript" in path, not scheme
			{"https://en.wikipedia.org/wiki/Mailto",                                 false}, // wiki article about mailto
			{"https://blog.example.com/why-pdfs-are-hard",                           false}, // "pdf" in slug, no extension
			{"https://news.com/story-about-login-security",                          false}, // "login" in slug but no /login segment - hmm wait, pattern is */login*
		};

		for (Object[] row : urls) {
			String url = (String) row[0];
			boolean expectBlocked = (Boolean) row[1];
			String matchedBy = findMatch(url, rawPatterns, compiled);
			boolean actuallyBlocked = matchedBy != null;
			boolean ok = actuallyBlocked == expectBlocked;
			record(ok, String.format("%s | %-60s %s",
				expectBlocked ? "BLOCK" : "ALLOW",
				truncate(url, 60),
				ok ? (actuallyBlocked ? "(matched \"" + matchedBy + "\")" : "") :
					"UNEXPECTED: " + (actuallyBlocked ? "blocked by \"" + matchedBy + "\"" : "not blocked")));
		}
	}

	static String findMatch(String url, List<String> raw, List<Pattern> compiled) {
		for (int i = 0; i < compiled.size(); i++) {
			if (compiled.get(i).matcher(url).matches()) return raw.get(i);
		}
		return null;
	}

	static List<String> loadBlacklist(String path) throws Exception {
		List<String> out = new ArrayList<>();
		for (String line : Files.readAllLines(Paths.get(path))) {
			String t = line.trim();
			if (t.isEmpty() || t.startsWith("#")) continue;
			out.add(t);
		}
		return out;
	}

	static void record(boolean ok, String line) {
		if (ok) { pass++; System.out.println("PASS | " + line); }
		else   { fail++; System.out.println("FAIL | " + line); failures.add(line); }
	}

	static String truncate(String s, int n) {
		return s.length() <= n ? String.format("%-" + n + "s", s) : s.substring(0, n - 1) + "…";
	}
}
