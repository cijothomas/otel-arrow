#!/usr/bin/env node
// Perf Regression Watcher
//
// Compares the latest two entries of every docs/benchmarks/**/data.js file
// on the benchmarks branch and reports metrics whose |delta| exceeds a
// configurable threshold by opening or commenting on a single shared issue
// labelled "perf-regression".
//
// Inputs (env):
//   GH_TOKEN              - token with issues:write on REPO
//   REPO                  - "owner/name" where the issue is filed (and labels live)
//   BENCHMARKS_REPO       - "owner/name" of the repo hosting the benchmarks branch
//   BENCHMARKS_BRANCH     - branch holding the data.js files (e.g. "benchmarks")
//   DEFAULT_THRESHOLD_PCT - fallback threshold if config has none / file missing
//   DRY_RUN               - "true" prints findings without touching issues
//   WORKFLOW_RUN_URL      - link to the triggering workflow run (for issue body)
//   CONFIG_JSON           - perf-regression config as JSON (already YAML-parsed)
//
// Exits 0 on success even if regressions are found. Exits non-zero only on
// unexpected errors (network, parse). This keeps the workflow green so the
// signal channel is "did an issue get opened/commented", not "did CI fail".

const {
  GH_TOKEN,
  REPO,
  BENCHMARKS_REPO,
  BENCHMARKS_BRANCH,
  DEFAULT_THRESHOLD_PCT = "10",
  DRY_RUN = "false",
  WORKFLOW_RUN_URL = "",
  CONFIG_JSON = "{}",
  PATH_FILTER = "",
} = process.env;

const LABEL = "perf-regression";
const ISSUE_TITLE = "Nightly perf/size regression watcher: investigation needed";
const DEFAULT_THRESHOLD = Number(DEFAULT_THRESHOLD_PCT);
const DRY = DRY_RUN === "true";

if (!GH_TOKEN || !REPO || !BENCHMARKS_REPO || !BENCHMARKS_BRANCH) {
  console.error("Missing required env vars.");
  process.exit(2);
}

const config = (() => {
  try {
    const c = JSON.parse(CONFIG_JSON);
    return {
      defaultThreshold: Number(c.default_threshold_pct ?? DEFAULT_THRESHOLD),
      overrides: c.metric_overrides ?? [],
      ignored: new Set(c.ignored_paths ?? []),
    };
  } catch (e) {
    console.warn(`Config parse failed (${e.message}); using defaults.`);
    return { defaultThreshold: DEFAULT_THRESHOLD, overrides: [], ignored: new Set() };
  }
})();

async function ghApi(path, init = {}) {
  const res = await fetch(`https://api.github.com${path}`, {
    ...init,
    headers: {
      "Accept": "application/vnd.github+json",
      "Authorization": `Bearer ${GH_TOKEN}`,
      "User-Agent": "perf-regression-watcher",
      "X-GitHub-Api-Version": "2022-11-28",
      ...(init.headers || {}),
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`GitHub API ${init.method || "GET"} ${path} -> ${res.status}: ${text}`);
  }
  return res.json();
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": "perf-regression-watcher" },
  });
  if (!res.ok) throw new Error(`fetch ${url} -> ${res.status}`);
  return res.text();
}

// Parse a github-action-benchmark data.js file:
//   window.BENCHMARK_DATA = { ... };
function parseDataJs(text) {
  const m = text.match(/=\s*({[\s\S]*})\s*;?\s*$/);
  if (!m) throw new Error("data.js: no JSON object found");
  return JSON.parse(m[1]);
}

function thresholdFor(path, metricName) {
  for (const o of config.overrides) {
    if (o.path !== path) continue;
    if (!o.metrics || o.metrics.includes(metricName)) {
      return Number(o.threshold_pct);
    }
  }
  return config.defaultThreshold;
}

// Returns array of finding objects for one data.js file.
function diffEntries(path, data) {
  const findings = [];
  for (const [suite, runs] of Object.entries(data.entries || {})) {
    if (runs.length < 2) continue;
    const curr = runs[runs.length - 1];
    const prev = runs[runs.length - 2];

    // The github-action-benchmark schema allows duplicate bench names within a
    // single run; some publishers (e.g. continuous-idle-state) emit the same
    // metric name multiple times. Collapse to first-occurrence on both sides so
    // we don't emit N duplicate findings for the same name.
    const firstByName = (benches) => {
      const out = {};
      for (const b of benches) if (!(b.name in out)) out[b.name] = b;
      return out;
    };
    const currByName = firstByName(curr.benches);
    const prevByName = firstByName(prev.benches);
    if (Object.keys(currByName).length !== curr.benches.length) {
      console.warn(
        `  ${path}: ${curr.benches.length - Object.keys(currByName).length} duplicate bench name(s) in latest run; using first occurrence`,
      );
    }

    for (const c of Object.values(currByName)) {
      const p = prevByName[c.name];
      if (!p || p.value === 0) continue;
      const deltaPct = ((c.value - p.value) / p.value) * 100;
      const th = thresholdFor(path, c.name);
      if (Math.abs(deltaPct) < th) continue;
      findings.push({
        path,
        suite,
        metric: c.name,
        unit: c.unit || "",
        prevValue: p.value,
        currValue: c.value,
        prevCommit: prev.commit?.id,
        currCommit: curr.commit?.id,
        currCommitUrl: curr.commit?.url,
        deltaPct,
        threshold: th,
        arrow: deltaPct > 0 ? "📈" : "📉",
      });
    }
  }
  return findings;
}

function renderBody(findings) {
  const lines = [];
  lines.push(
    `<!-- watcher:auto-generated. Do not edit the table headers; the watcher uses them to dedup. -->`,
  );
  lines.push("");
  lines.push(
    `The nightly perf-regression watcher flagged the following benchmark deltas against the previous nightly. Investigate whether each change is intentional and either close this issue or open follow-up issues per metric.`,
  );
  lines.push("");
  if (WORKFLOW_RUN_URL) lines.push(`Watcher run: ${WORKFLOW_RUN_URL}`);
  lines.push(
    `Dashboards: https://${BENCHMARKS_REPO.split("/")[0]}.github.io/${BENCHMARKS_REPO.split("/")[1]}/benchmarks/`,
  );
  lines.push("");
  lines.push("| | Metric | Previous | Current | Δ | Threshold | Path |");
  lines.push("|---|---|---|---|---|---|---|");
  for (const f of findings) {
    const unit = f.unit ? ` ${f.unit}` : "";
    const delta = `${f.deltaPct >= 0 ? "+" : ""}${f.deltaPct.toFixed(2)}%`;
    lines.push(
      `| ${f.arrow} | \`${f.metric}\` | ${f.prevValue}${unit} | ${f.currValue}${unit} | ${delta} | ±${f.threshold}% | ${f.path} |`,
    );
  }
  lines.push("");
  const fingerprints = findings
    .map((f) => `${f.path}::${f.metric}::${f.currCommit ?? "?"}`)
    .sort();
  lines.push(`<!-- fingerprints:${fingerprints.join(",")} -->`);
  return lines.join("\n");
}

async function findOpenIssue() {
  const issues = await ghApi(
    `/repos/${REPO}/issues?state=open&labels=${encodeURIComponent(LABEL)}&per_page=10`,
  );
  return issues[0] || null;
}

function bodyFingerprints(body) {
  const m = (body || "").match(/<!-- fingerprints:([^>]*)-->/);
  if (!m) return new Set();
  return new Set(
    m[1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
  );
}

async function getRecentFingerprints(issueNumber) {
  const fingerprints = new Set();
  const issue = await ghApi(`/repos/${REPO}/issues/${issueNumber}`);
  for (const fp of bodyFingerprints(issue.body)) fingerprints.add(fp);
  const comments = await ghApi(
    `/repos/${REPO}/issues/${issueNumber}/comments?per_page=100`,
  );
  for (const c of comments) {
    for (const fp of bodyFingerprints(c.body)) fingerprints.add(fp);
  }
  return fingerprints;
}

async function ensureLabel() {
  try {
    await ghApi(`/repos/${REPO}/labels/${encodeURIComponent(LABEL)}`);
  } catch {
    if (DRY) return;
    await ghApi(`/repos/${REPO}/labels`, {
      method: "POST",
      body: JSON.stringify({
        name: LABEL,
        color: "d93f0b",
        description: "Nightly perf/size benchmark regressed beyond configured threshold",
      }),
    });
  }
}

async function main() {
  // 1. Discover data.js files under docs/benchmarks/ on the benchmarks branch.
  const tree = await ghApi(
    `/repos/${BENCHMARKS_REPO}/git/trees/${BENCHMARKS_BRANCH}?recursive=1`,
  );
  const dataPaths = tree.tree
    .filter((n) => n.type === "blob")
    .map((n) => n.path)
    .filter(
      (p) =>
        p.startsWith("docs/benchmarks/") &&
        p.endsWith("/data.js") &&
        !config.ignored.has(p) &&
        (!PATH_FILTER || p.includes(PATH_FILTER)),
    );
  console.log(`Discovered ${dataPaths.length} data.js files on ${BENCHMARKS_REPO}@${BENCHMARKS_BRANCH}${PATH_FILTER ? ` (filter: "${PATH_FILTER}")` : ""}`);

  // 2. Fetch + diff each.
  const allFindings = [];
  for (const path of dataPaths) {
    try {
      const raw = await fetchText(
        `https://raw.githubusercontent.com/${BENCHMARKS_REPO}/${BENCHMARKS_BRANCH}/${path}`,
      );
      const data = parseDataJs(raw);
      const f = diffEntries(path, data);
      console.log(`  ${path}: ${f.length} finding(s)`);
      allFindings.push(...f);
    } catch (e) {
      console.warn(`  ${path}: skipped (${e.message})`);
    }
  }

  if (allFindings.length === 0) {
    console.log("No regressions above threshold. Done.");
    return;
  }

  console.log(`\nTotal findings: ${allFindings.length}`);
  const body = renderBody(allFindings);
  console.log("\n--- Proposed issue body ---");
  console.log(body);
  console.log("--- End body ---\n");

  if (DRY) {
    console.log("DRY_RUN=true; not opening/commenting an issue.");
    return;
  }

  await ensureLabel();

  // 3. Find existing open issue.
  const existing = await findOpenIssue();
  const newFingerprints = new Set(
    allFindings.map((f) => `${f.path}::${f.metric}::${f.currCommit ?? "?"}`),
  );

  if (existing) {
    const seen = await getRecentFingerprints(existing.number);
    const novel = [...newFingerprints].filter((fp) => !seen.has(fp));
    if (novel.length === 0) {
      console.log(`Issue #${existing.number} already covers all current findings; skipping comment.`);
      return;
    }
    console.log(`Commenting on existing issue #${existing.number} with ${novel.length} new finding(s).`);
    const novelFindings = allFindings.filter((f) =>
      novel.includes(`${f.path}::${f.metric}::${f.currCommit ?? "?"}`),
    );
    await ghApi(`/repos/${REPO}/issues/${existing.number}/comments`, {
      method: "POST",
      body: JSON.stringify({ body: renderBody(novelFindings) }),
    });
    return;
  }

  console.log("Opening new issue.");
  const issue = await ghApi(`/repos/${REPO}/issues`, {
    method: "POST",
    body: JSON.stringify({
      title: ISSUE_TITLE,
      body,
      labels: [LABEL],
    }),
  });
  console.log(`Opened: ${issue.html_url}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
