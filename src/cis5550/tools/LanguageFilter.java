package cis5550.tools;

import java.util.HashSet;
import java.util.Set;

public class LanguageFilter {

	private static final int HTML_SCAN_PREFIX = 4096;
	private static final int HEURISTIC_PREFIX = 8192;
	private static final int MIN_WORDS_FOR_HEURISTIC = 20;
	private static final int MIN_STOPWORD_HITS = 3;
	private static final double MIN_STOPWORD_RATIO = 0.02;

	private static final Set<String> STOPWORDS = new HashSet<>();
	static {
		String[] words = {
			"the", "and", "of", "to", "a", "in", "is", "it", "that", "for",
			"on", "with", "as", "are", "be", "this", "was", "at", "by", "an",
			"have", "from", "or", "but", "not", "you", "we", "they", "he", "she"
		};
		for (String w : words) STOPWORDS.add(w);
	}

	public static class Decision {
		public final boolean accept;
		public final String lang;
		public Decision(boolean accept, String lang) {
			this.accept = accept;
			this.lang = lang;
		}
	}

	public static String parseContentLanguage(String headerValue) {
		if (headerValue == null) return null;
		String trimmed = headerValue.trim();
		if (trimmed.isEmpty()) return "";
		String[] parts = trimmed.split(",");
		String firstNonEmpty = null;
		for (String p : parts) {
			String tag = p.trim();
			// Strip quality value if present (e.g. "en;q=0.8")
			int semi = tag.indexOf(';');
			if (semi >= 0) tag = tag.substring(0, semi).trim();
			if (tag.isEmpty()) continue;
			if (firstNonEmpty == null) firstNonEmpty = tag;
			if (isEnglishTag(tag)) return tag;
		}
		return firstNonEmpty == null ? "" : firstNonEmpty;
	}

	public static String parseHtmlLang(String htmlPrefix) {
		if (htmlPrefix == null) return null;
		String src = htmlPrefix.length() > HTML_SCAN_PREFIX
			? htmlPrefix.substring(0, HTML_SCAN_PREFIX) : htmlPrefix;
		String lower = src.toLowerCase();
		int idx = 0;
		while (idx < lower.length()) {
			int htmlStart = lower.indexOf("<html", idx);
			if (htmlStart == -1) return null;
			int after = htmlStart + 5;
			if (after >= lower.length()) return null;
			char c = lower.charAt(after);
			if (c != ' ' && c != '\t' && c != '\n' && c != '\r' && c != '>') {
				idx = after;
				continue;
			}
			int tagEnd = lower.indexOf('>', htmlStart);
			if (tagEnd == -1) return null;
			String tagLower = lower.substring(htmlStart, tagEnd);
			String tagOrig = src.substring(htmlStart, tagEnd);
			String val = extractAttr(tagLower, tagOrig, "lang");
			if (val == null) val = extractAttr(tagLower, tagOrig, "xml:lang");
			return val;
		}
		return null;
	}

	private static String extractAttr(String tagLower, String tagOrig, String attr) {
		int pos = 0;
		while (pos < tagLower.length()) {
			int at = tagLower.indexOf(attr, pos);
			if (at == -1) return null;
			// Must be preceded by whitespace (attribute boundary) to avoid matching
			// substrings like "xml:lang" when searching for "lang", or "language" for "lang".
			// at == 0 is also rejected: a real attribute must be preceded by whitespace.
			if (at == 0) {
				pos = at + attr.length();
				continue;
			}
			char prev = tagLower.charAt(at - 1);
			if (prev != ' ' && prev != '\t' && prev != '\n' && prev != '\r') {
				pos = at + attr.length();
				continue;
			}
			int eq = at + attr.length();
			while (eq < tagLower.length() && (tagLower.charAt(eq) == ' ' || tagLower.charAt(eq) == '\t')) eq++;
			if (eq >= tagLower.length() || tagLower.charAt(eq) != '=') {
				pos = at + attr.length();
				continue;
			}
			int valStart = eq + 1;
			while (valStart < tagLower.length() && (tagLower.charAt(valStart) == ' ' || tagLower.charAt(valStart) == '\t')) valStart++;
			if (valStart >= tagLower.length()) return null;
			char quote = tagLower.charAt(valStart);
			if (quote == '"' || quote == '\'') {
				int valEnd = tagLower.indexOf(quote, valStart + 1);
				if (valEnd == -1) return null;
				return tagOrig.substring(valStart + 1, valEnd);
			}
			int valEnd = valStart;
			while (valEnd < tagLower.length()
				&& tagLower.charAt(valEnd) != ' '
				&& tagLower.charAt(valEnd) != '\t'
				&& tagLower.charAt(valEnd) != '>') {
				valEnd++;
			}
			return tagOrig.substring(valStart, valEnd);
		}
		return null;
	}

	public static boolean isEnglishTag(String langTag) {
		if (langTag == null) return false;
		String t = langTag.trim().toLowerCase();
		if (t.isEmpty()) return false;
		int cut = t.length();
		for (int i = 0; i < t.length(); i++) {
			char c = t.charAt(i);
			if (c == '-' || c == '_') { cut = i; break; }
		}
		return t.substring(0, cut).equals("en");
	}

	public static boolean passesEnglishStopwordHeuristic(String htmlPrefix) {
		if (htmlPrefix == null || htmlPrefix.isEmpty()) return true;
		String prefix = htmlPrefix.length() > HEURISTIC_PREFIX
			? htmlPrefix.substring(0, HEURISTIC_PREFIX) : htmlPrefix;
		String stripped = prefix.replaceAll("<[^>]*>", " ").toLowerCase();
		int totalWords = 0;
		int hits = 0;
		int n = stripped.length();
		int i = 0;
		while (i < n) {
			while (i < n && !isAsciiLetter(stripped.charAt(i))) i++;
			int start = i;
			while (i < n && isAsciiLetter(stripped.charAt(i))) i++;
			if (i > start) {
				totalWords++;
				String word = stripped.substring(start, i);
				if (STOPWORDS.contains(word)) hits++;
			}
		}
		if (totalWords < MIN_WORDS_FOR_HEURISTIC) return true;
		if (hits < MIN_STOPWORD_HITS) return false;
		double ratio = (double) hits / (double) totalWords;
		return ratio >= MIN_STOPWORD_RATIO;
	}

	private static boolean isAsciiLetter(char c) {
		return c >= 'a' && c <= 'z';
	}

	public static Decision decide(String contentLanguageHeader, String htmlBody) {
		String headerLang = parseContentLanguage(contentLanguageHeader);
		if (headerLang != null && !headerLang.isEmpty()) {
			if (isEnglishTag(headerLang)) return new Decision(true, headerLang);
			return new Decision(false, headerLang);
		}
		String htmlLang = parseHtmlLang(htmlBody);
		if (htmlLang != null && !htmlLang.isEmpty()) {
			if (isEnglishTag(htmlLang)) return new Decision(true, htmlLang);
			return new Decision(false, htmlLang);
		}
		if (passesEnglishStopwordHeuristic(htmlBody)) {
			return new Decision(true, "unknown");
		}
		return new Decision(false, "non-en");
	}

	public static void main(String[] args) {
		check("parseContentLanguage en-US", "en-US", parseContentLanguage("en-US"));
		check("isEnglishTag en-US", true, isEnglishTag("en-US"));
		check("parseContentLanguage de, fr", "de", parseContentLanguage("de, fr"));
		check("parseContentLanguage en, de picks en", true, isEnglishTag(parseContentLanguage("en, de")));
		check("parseContentLanguage empty", "", parseContentLanguage(""));
		check("parseContentLanguage null", null, parseContentLanguage(null));
		check("parseContentLanguage with q", true, isEnglishTag(parseContentLanguage("en;q=0.9, de;q=0.8")));

		check("isEnglishTag EN", true, isEnglishTag("EN"));
		check("isEnglishTag en_GB", true, isEnglishTag("en_GB"));
		check("isEnglishTag de", false, isEnglishTag("de"));
		check("isEnglishTag empty", false, isEnglishTag(""));
		check("isEnglishTag null", false, isEnglishTag(null));

		check("parseHtmlLang de", "de", parseHtmlLang("<!DOCTYPE html><html lang=\"de\"><body>hi</body></html>"));
		check("parseHtmlLang xml:lang en-GB", "en-GB", parseHtmlLang("<html xml:lang='en-GB'><body>hi</body></html>"));
		check("parseHtmlLang missing", null, parseHtmlLang("<html><body>hi</body></html>"));
		check("parseHtmlLang unquoted", "en", parseHtmlLang("<html lang=en><body>hi</body></html>"));
		check("parseHtmlLang null", null, parseHtmlLang(null));
		// lang takes priority over xml:lang (HTML5 semantics)
		check("parseHtmlLang lang wins over xml:lang", "en", parseHtmlLang("<html lang=\"en\" xml:lang=\"de\"><body>hi</body></html>"));
		// xml:lang used as fallback when lang absent
		check("parseHtmlLang xml:lang fallback", "de", parseHtmlLang("<html xml:lang=\"de\"><body>hi</body></html>"));
		// substring inside attribute value should not match
		check("parseHtmlLang no match in value", "en", parseHtmlLang("<html class=\"language-nav\" lang=\"en\"><body>hi</body></html>"));

		String englishPara = "<p>The quick brown fox jumps over the lazy dog. "
			+ "It is a well known sentence that contains many of the letters in the English alphabet. "
			+ "This is the kind of text you would expect to see on a page written for English readers.</p>";
		check("stopword heuristic english", true, passesEnglishStopwordHeuristic(englishPara));

		String germanPara = "<p>Der schnelle braune Fuchs springt ueber den faulen Hund. "
			+ "Dies ist ein bekannter Satz, der viele Buchstaben des deutschen Alphabets enthaelt. "
			+ "Eine solche Seite ist fuer deutsche Leser gedacht und enthaelt keine englischen Worte.</p>";
		check("stopword heuristic german", false, passesEnglishStopwordHeuristic(germanPara));

		check("decide short accepts", true, decide(null, "<html><body>short</body></html>").accept);
		check("decide header de rejects", false, decide("de", englishPara).accept);
		check("decide header en accepts", true, decide("en", germanPara).accept);
		check("decide html lang de rejects", false, decide(null, "<html lang=de><body>" + englishPara + "</body></html>").accept);
		check("decide heuristic english accepts", true, decide(null, englishPara).accept);
		check("decide heuristic german rejects", false, decide(null, germanPara).accept);

		System.out.println("LanguageFilter smoke tests complete.");
	}

	private static void check(String label, Object expected, Object actual) {
		boolean ok = (expected == null) ? (actual == null) : expected.equals(actual);
		System.out.println((ok ? "PASS " : "FAIL ") + label + "  expected=" + expected + "  actual=" + actual);
	}
}
