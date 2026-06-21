# Cerebro Agent — Technical Build Plan

A research agent layer on top of the Cerebro search engine (CIS5550 final project).
Goal: question → decompose → search Cerebro → read pages → synthesize a cited answer →
retry if insufficient. Built and evaluated over 15–20 days.

This document is meant to be worked through session-by-session with Claude. Each phase
has a fixed scope and a "done" check — don't move to the next phase until the check passes.
This mirrors the lesson your own project report already learned the hard way: validate small,
end-to-end, before adding features.

---

## 0. Architecture (read this once, refer back to it)

Two systems, talking over plain HTTP. **Cerebro is not modified** except for one new
read-only endpoint. Everything agentic lives in a new, separate service.

```
Browser (Ask mode)
      │  HTTP
      ▼
Agent Service (new, Python, FastAPI)
      │  HTTP — calls Cerebro exactly like any external client would
      ▼
Cerebro Frontend (Java, unchanged except /api/page)
  GET /api/search?q=...     [EXISTS]
  GET /api/page?url=...     [NEW — Phase 1]
      │
      ▼
KVS  (pt-index, pt-crawl, pt-pageranks)   ← untouched
```

Why a separate service instead of bolting this into `Frontend.java`:
- Keeps the CIS5550 submission boundary clean — Cerebro stays a complete, gradeable,
  unmodified artifact in its own right.
- Python is much faster to iterate on for prompt engineering than Java.
- It's an honest architectural story: "I built a search engine, then built an agent
  that uses it as a retrieval backend" — two distinct, defensible pieces of work.

The agent service talks to Cerebro's HTTP API the same way `curl` would. No shared code,
no shared process, no imports across the boundary.

---

## 1. The five things you're actually building

1. **One new Cerebro endpoint**: `GET /api/page?url=...` → full page text as JSON.
2. **Query decomposer**: LLM call that turns a question into 2–4 Cerebro-shaped sub-queries.
3. **Retrieval + read loop**: plain HTTP code — call `/api/search`, then `/api/page` for top-k.
4. **Synthesis + sufficiency check**: LLM call that answers from retrieved text only, cites by
   URL, and can say "INSUFFICIENT" to trigger one more retrieval round.
5. **Eval harness**: ~20–30 questions against your real corpus, scored for retrieval hit-rate
   and citation-support rate.

Everything else (UI, streaming, deployment) wraps around these five things. Build them in
this order. Do not start the UI before Phase 4's loop works from the command line.

---

## 2. Phase-by-phase plan

### Phase 1 — Days 1–2: The one Cerebro change

**What:** Add `GET /api/page?url=...` to `Frontend.java`, returning:
```json
{"url": "...", "title": "...", "text": "..."}
```

**How:** You already have the exact pattern three times in `Frontend.java` — `/cache`,
the debug page, and inside `doSearch`'s per-result metadata fetch (`kvs.get("pt-crawl",
Hasher.hash(url), "page")`). Reuse that lookup, strip HTML tags to plain text (a simple
regex strip is fine — you don't need a real HTML parser for this), and return JSON using
the same `jsonEsc` pattern already used in `toJson(List<Result>)`.

**Why this endpoint and not reusing `/cache`:** `/cache` returns raw HTML for rendering in
a browser tab with a banner injected. The agent needs clean extracted text, not raw markup,
and JSON is easier to parse downstream than HTML-scraping your own cache page.

**Decisions to make:**
- Cap response size (e.g. first ~8,000 characters of body text) so one page can't blow up
  your LLM context budget later. Truncate, don't reject — agent should still get partial text.
- Return 404 JSON (`{"error": "not found"}`) if the URL isn't in `pt-crawl`, so the agent
  service can handle misses gracefully instead of crashing on a parse error.

**Done check:** `curl "http://<host>/api/page?url=<some indexed URL>"` returns clean JSON
with non-empty `text`. Test against 3–4 URLs you know are indexed (pull them from a
`/debug?q=` result page).

---

### Phase 2 — Days 3–5: Agent service skeleton + retrieval (no LLM yet)

**What:** A new directory, e.g. `agent/`, sitting alongside (not inside) the Cerebro repo,
or as a sibling top-level folder if you want one repo. FastAPI app with:
- `POST /ask {"question": "..."}` — the only external endpoint, for now just a stub
- A `cerebro_client.py` module: `search(query) -> list[dict]`, `fetch_page(url) -> dict`,
  thin wrappers over `requests.get` to your two Cerebro endpoints.

**Why build retrieval before any LLM call:** You want to be 100% sure Cerebro round-trips
correctly — real results, real page text, sane latency — before adding LLM cost and
non-determinism on top. Debugging "is it the retrieval or the LLM" is much harder once
both are in the loop together. This is the same "smaller-scale validation pipeline" lesson
from your own report (§4), applied one layer up.

**Done check:** A script that calls `search("some test query")`, prints top-5 URLs, then
calls `fetch_page` on each and prints text length. No LLM involved. Confirms the HTTP
plumbing end-to-end.

---

### Phase 3 — Days 6–8: Query decomposition (LLM call #1)

**What:** Given a user question, call Claude to produce 2–4 sub-queries in Cerebro's
"shape" — short, keyword-style phrases, not full sentences. Cerebro's index is
stemmed-token TF/IDF, not semantic search, so a sub-query like "what programming
language is used for the crawler" will match worse than "crawler Java implementation".

**Prompt design constraints to bake in:**
- Force structured output (JSON list of strings) — see Anthropic's docs on structured
  outputs / forcing JSON, referenced in your system's product-knowledge tooling if you
  need exact current API syntax.
- Tell the model explicitly that the search index is keyword/TF-IDF based, not semantic,
  so it generates queries suited to lexical matching, not natural-language questions.
- Cap at 4 sub-queries — more doesn't help and burns retrieval + token budget.

**Done check:** Feed 5 varied test questions, manually eyeball whether the generated
sub-queries look like things that would actually hit relevant pages in your corpus
(spot-check by running them through Phase 2's retrieval script).

---

### Phase 4 — Days 9–12: Synthesis + sufficiency check (LLM call #2/#3) — the core loop

**What:** This is the actual "agent" part. Pseudocode:

```
def answer(question):
    subqueries = decompose(question)                      # Phase 3
    pages = retrieve_and_read(subqueries)                  # Phase 2, looped over subqueries
    result = synthesize(question, pages)                   # LLM call, forced citations
    if result.sufficient:
        return result
    followups = generate_followups(question, result.gaps)  # 1 more LLM call
    more_pages = retrieve_and_read(followups)
    result2 = synthesize(question, pages + more_pages)      # second and FINAL attempt
    return result2
```

**Hard cap at 2 rounds.** No open-ended looping — bound it explicitly so a bad
sufficiency judgment can't spiral into runaway LLM calls. This is the agent-equivalent of
your crawler's frontier cap (per-page caps, hard cap on frontier size) — same instinct,
applied to LLM calls instead of URLs.

**Synthesis prompt constraints:**
- Answer **only** from the provided page texts — no outside knowledge. State this
  explicitly and test that it's actually followed (this is the single most common failure
  mode in RAG systems: the model answers from its training data instead of the retrieved
  context, and looks correct while being unverifiable).
- Every factual claim must carry an inline citation tag to the source URL.
- If the provided pages don't actually answer the question, the model must say so
  explicitly (`"sufficient": false` in structured output) rather than answering anyway.

**Done check:** Run 10 hand-picked questions you already know the answer to (because you
know what's in your corpus) end-to-end from the command line. For each: does the answer
look right, and does every citation actually point to a page that supports the claim?
Don't move to Phase 5 until this passes on most of the 10 — this is the iteration loop
where almost all of your real debugging time will go.

---

### Phase 5 — Days 13–15: Evaluation harness

**Why this phase matters more than it looks like it should:** this is what separates "I
wrapped an LLM around our search engine" from "I built and measured a retrieval-augmented
agent." It's also the one part of this plan that maps directly onto research-engineering
skills (calibration, eval design) rather than just software engineering — worth treating
as seriously as the loop itself.

**What:** A fixed test set of 20–30 questions against your actual crawled corpus (not
synthetic/generic questions — pull from what's actually indexed, the same way you built
~30 sample queries by hand to tune Cerebro's ranking per the report). For each question,
hand-record:
- Which URLs *should* plausibly appear in a good answer (your judgment, written down
  before running the agent — don't post-hoc rationalize)
- A short note on what a correct answer should contain

**Metrics to compute automatically:**
1. **Retrieval hit-rate** — did any of the "should appear" URLs show up in retrieval?
2. **Citation-support rate** — for each citation in the final answer, does the cited
   page's text actually contain/support the claim attached to it? (This needs a small
   second LLM call per citation: "does this page support this claim, yes/no" — slow but
   this is the metric that actually catches hallucinated citations, which is the
   single biggest credibility risk in a RAG system.)
3. **Sufficiency-trigger accuracy** — on questions you deliberately picked because the
   corpus *doesn't* cover them well, does the agent correctly flag insufficiency rather
   than confabulating an answer?

**Done check:** A script (`eval.py`) that runs all 20–30 questions and prints a table:
question, retrieval hit-rate, citation-support rate, sufficiency flag correctness. This
table is the artifact you'll actually show people — screenshot it for your portfolio.

---

### Phase 6 — Days 16–18: UI integration

**What:** Add an "Ask" mode to Cerebro's existing frontend — same visual language you
already built (dark theme, instant-answer card pattern in `instantAnswerCard`), not a
new design. A toggle or second input mode next to the search box that POSTs to the agent
service's `/ask` and renders the synthesized answer with clickable citation links back to
`/cache?url=...` (which already exists).

**Decision point:** does the agent service get its own subdomain/port, with the Cerebro
frontend JS calling it directly (simplest), or does `Frontend.java` proxy the request to
the agent service server-side (cleaner single-origin story, slightly more Java work)?
Recommend the direct-call approach first — proxying is a Phase 7 polish item if you have
time left, not a blocker.

**Done check:** Typing a question into Ask mode on the actual deployed Cerebro UI returns
a rendered, cited answer in the browser.

---

### Phase 7 — Days 19–20: Deploy + write-up

**What:**
- Deploy the agent service on the same EC2 cluster (a small adjacent instance, or
  colocated with the coordinator if resources allow) so the whole thing is live under
  your existing `xxx.cis5550.net` domain.
- Write a short README section (or standalone doc) covering: architecture diagram, the
  eval methodology, the eval results table, and 2–3 sentences on what you'd do
  differently — mirroring the structure your team's actual project report already uses
  for Cerebro itself (features → challenges → what we'd change).

**Done check:** A stranger could read the write-up and understand what was built, how
it was measured, and where it's weak — without needing you in the room.

---

## 3. Things deliberately left out of scope

- **No new crawling.** The agent runs against whatever corpus Cerebro already has.
  Re-crawling toward a sharper topic is a future improvement, not part of this build.
- **No fallback to live web search.** If Cerebro's corpus doesn't cover a question, the
  agent says so (Phase 4/5's sufficiency logic) rather than silently going to the open
  web. This keeps the system's claims honest — every answer is traceable to something
  your own crawler actually indexed.
- **No multi-agent framework (LangGraph/CrewAI/AutoGen).** The loop in Phase 4 is maybe
  150 lines of plain Python. A framework would add indirection without adding capability
  at this scale — and writing the loop by hand means you can actually explain every part
  of it in an interview, which is the entire point of the project.
- **No persistent agent memory across questions.** Each `/ask` call is stateless. Memory
  across a session is a reasonable v2 feature, not part of the core build.

---

## 4. Session-by-session checklist (tear-off version)

- [ ] Phase 1: `/api/page` endpoint live, tested against 3+ real URLs
- [ ] Phase 2: retrieval script round-trips search + fetch with no LLM involved
- [ ] Phase 3: decomposition produces sane Cerebro-shaped sub-queries for 5 test questions
- [ ] Phase 4: full loop answers 10 hand-picked questions correctly from the CLI
- [ ] Phase 5: eval harness runs 20–30 questions, prints hit-rate / citation-support table
- [ ] Phase 6: "Ask" mode live in the actual Cerebro UI
- [ ] Phase 7: deployed on EC2, write-up done
