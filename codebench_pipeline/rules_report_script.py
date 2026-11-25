SCRIPT = r'''"use strict";

const rows = JSON.parse(document.getElementById("measurements").textContent);
const metric = document.getElementById("metric");
const cohort = document.getElementById("cohort");
const category = document.getElementById("category");
const breakdown = document.getElementById("breakdown");
const cohorts = ["overall", "uncontaminated", "trained", "unseen"];
const axes = ["category", "risk", "severity", "action"];
const outcomes = ["correct", "wrong", "abstain", "error"];
const metrics = [
  "recall", "precision", "coverage", "fpr", "support", "correct", "wrong",
  "abstain", "error", "predicted", "negative_support", "true_positive", "false_positive",
];
const ratios = new Set(["precision", "coverage", "recall", "fpr"]);
const names = new Map(Object.entries({
  overall: "Overall",
  uncontaminated: "Uncontaminated",
  trained: "Trained categories",
  unseen: "Unseen categories",
  category: "Category",
  risk: "Risk",
  severity: "Severity",
  action: "Action",
  fpr: "False positive rate",
  support: "Support",
  correct: "Correct",
  wrong: "Wrong",
  abstain: "Abstentions",
  error: "Errors",
  rows: "Overlapping test rows",
  unique_commands: "Distinct overlapping commands",
}));
const definitions = new Map(Object.entries({
  recall: "Correct predictions divided by the number of expected examples.",
  precision: "Correct predictions divided by the number of predictions made.",
  coverage: "Examples that received a prediction divided by the number of expected examples.",
  fpr: "False positives divided by the number of examples outside the selected class.",
  support: "Number of expected examples.",
  correct: "Number of correct complete labels.",
  wrong: "Number of incorrect complete labels.",
  abstain: "Number of examples on which the rules abstained.",
  error: "Number of examples that could not be evaluated.",
  predicted: "Number of predictions for the class.",
  negative_support: "Number of examples outside the class.",
  true_positive: "Number of correct predictions for the class.",
  false_positive: "Number of incorrect predictions for the class.",
}));
const count = new Intl.NumberFormat("en", { maximumFractionDigits: 3 });
const percent = new Intl.NumberFormat("en", { style: "percent", maximumFractionDigits: 1 });
const collator = new Intl.Collator("en", { numeric: true });
const svg = "http://www.w3.org/2000/svg";

function title(key) {
  return names.get(key) ?? (key.charAt(0).toUpperCase() + key.slice(1).replaceAll("_", " "));
}

function ordered(values, preferred) {
  const unique = new Set(values);
  const first = preferred.filter((value) => unique.delete(value));
  return first.concat([...unique].sort(collator.compare));
}

function options(select, values, { all, unavailable = "Not available" } = {}) {
  const previous = select.value;
  const choices = values.map((value) => new Option(title(value), value));
  if (values.length && all) choices.unshift(new Option(all, ""));
  if (!choices.length) choices.push(new Option(unavailable, ""));
  select.replaceChildren(...choices);
  if (choices.some((option) => option.value === previous)) select.value = previous;
  select.disabled = !values.length;
}

function node(tag, text, style) {
  const element = document.createElement(tag);
  if (text) element.textContent = text;
  if (style) element.className = style;
  return element;
}

function format(value, ratio) {
  if (value === null) return "Undefined";
  if (!ratio) return value > 0 && value < 0.001 ? "<0.001" : count.format(value);
  if (value > 0 && value < 0.001) return "<0.1%";
  if (value > 0.999 && value < 1) return ">99.9%";
  return percent.format(value);
}

function graphicTrack(className) {
  const graphic = document.createElementNS(svg, "svg");
  graphic.setAttribute("class", className);
  graphic.setAttribute("aria-hidden", "true");
  graphic.setAttribute("viewBox", "0 0 1000 10");
  graphic.setAttribute("preserveAspectRatio", "none");
  return graphic;
}

function segment(start, end) {
  const fill = document.createElementNS(svg, "rect");
  fill.setAttribute("x", String(start * 1000));
  fill.setAttribute("width", String((end - start) * 1000));
  fill.setAttribute("height", "10");
  return fill;
}

function bar(value, maximum) {
  const graphic = graphicTrack("bar");
  graphic.append(segment(0, value / maximum));
  return graphic;
}

function chart(id, { visible, describe, empty, ratio, maximum }) {
  const region = document.getElementById(id);
  if (!visible.length) {
    region.replaceChildren(node("p", empty, "empty"));
    return;
  }
  const hasDefined = visible.some((row) => row.value !== null);
  const showBars = hasDefined && maximum > 0;
  const scale = node("div", "", "scale");
  if (showBars) {
    const ticks = node("div", "", "scale-ticks");
    for (const value of ratio ? [0, 0.5, 1] : [0, maximum]) ticks.append(node("span", format(value, ratio)));
    scale.append(node("p", ratio ? "Rate scale" : "Shared count scale"), ticks);
  } else {
    scale.append(node("p", hasDefined ? "All measured values are zero." : "No defined values to plot."));
  }
  const list = node("ul", "", "bars");
  list.setAttribute("role", "list");
  for (const row of visible) {
    const item = node("li");
    const reading = node("div", "", "reading");
    reading.append(
      node("span", describe(row)),
      node("span", format(row.value, ratio), row.value === null ? "value undefined" : "value"),
    );
    item.append(reading);
    if (showBars && row.value !== null) item.append(bar(row.value, maximum));
    list.append(item);
  }
  region.replaceChildren(scale, list);
}

function selectedCohort(row) {
  return !cohort.value || row.cohort === cohort.value;
}

function distributionRow(name, measurements) {
  const value = (key) => measurements.get(key);
  const text = (key) => measurements.has(key) ? format(value(key), false) : "Missing";
  const support = value("support");
  const values = outcomes.map(value);
  const item = node("li");
  const reading = node("div", "", "reading");
  reading.append(node("strong", title(name)), node("span", `Support: ${text("support")}`, "value"));
  item.append(reading);
  let note;
  if (![support, ...values].every(Number.isSafeInteger)) {
    note = "Cannot plot: support or some outcome counts are unavailable.";
  } else if (values.reduce((total, count) => total + count, 0) !== support) {
    note = "Cannot plot: outcome counts do not add up to support.";
  } else if (support === 0) {
    note = "No evaluated rows in this cohort.";
  } else {
    const stack = graphicTrack("outcome-stack");
    let offset = 0;
    outcomes.forEach((key, index) => {
      const amount = values[index];
      if (amount === 0) return;
      const fill = segment(offset / support, (offset + amount) / support);
      fill.setAttribute("class", `outcome-${key}`);
      stack.append(fill);
      offset += amount;
    });
    item.append(stack);
  }
  if (note) item.append(node("p", note, "note"));
  const counts = node("dl", "", "outcome-counts");
  for (const key of outcomes) {
    const pair = node("div", "", `outcome-${key}`);
    pair.append(node("dt", title(key)), node("dd", text(key)));
    counts.append(pair);
  }
  item.append(counts);
  return item;
}

function renderDistribution(cohortOrder) {
  const region = document.getElementById("distribution");
  const visible = cohortOrder.filter((name) => selectedCohort({ cohort: name }) && outcomeGroups.has(name));
  if (!visible.length) {
    region.replaceChildren(node("p", "No outcome counts for this selection.", "note"));
    return;
  }
  const list = node("ul", "", "bars");
  list.setAttribute("role", "list");
  for (const name of visible) list.append(distributionRow(name, outcomeGroups.get(name)));
  region.replaceChildren(list);
}

function categoryOf(row) {
  return row.axis === "risk" ? row.label.split(".")[0] : row.label;
}

function overlapReport(overlap) {
  if (!overlap.length) return;
  const list = document.getElementById("overlap-values");
  for (const row of overlap) {
    const pair = node("div");
    pair.append(node("dt", title(row.metric)), node("dd", format(row.value, false)));
    list.append(pair);
  }
  document.getElementById("overlap").hidden = false;
}

const explorerRows = rows.filter((row) => row.axis !== "overlap");
const overlapRows = rows.filter((row) => row.axis === "overlap");
const outcomeGroups = new Map();
for (const row of explorerRows) {
  if (row.axis !== "outcome") continue;
  if (!outcomeGroups.has(row.cohort)) outcomeGroups.set(row.cohort, new Map());
  outcomeGroups.get(row.cohort).set(row.metric, row.value);
}

function render() {
  document.getElementById("metric-note").textContent = definitions.get(metric.value) ?? "";
  const measurements = explorerRows.filter((row) => row.metric === metric.value);
  const ratio = ratios.has(metric.value);
  const maximum = ratio ? 1 : measurements.reduce((max, row) => Math.max(max, row.value ?? 0), 0);
  const cohortOrder = ordered([...outcomeGroups.keys(), ...measurements.map((row) => row.cohort)], cohorts);
  options(cohort, cohortOrder, { all: "All cohorts" });
  renderDistribution(cohortOrder);
  const scopedClasses = measurements.filter((row) => row.axis !== "outcome" && selectedCohort(row));
  options(breakdown, ordered(scopedClasses.map((row) => row.axis), axes));
  const outcomeRows = measurements.filter((row) => row.axis === "outcome")
    .sort((a, b) => cohortOrder.indexOf(a.cohort) - cohortOrder.indexOf(b.cohort));
  const byClass = measurements.filter((row) => row.axis === breakdown.value)
    .sort((a, b) => collator.compare(a.label, b.label)
      || cohortOrder.indexOf(a.cohort) - cohortOrder.indexOf(b.cohort));
  const scopedBreakdown = byClass.filter(selectedCohort);
  const hasCategories = ["category", "risk"].includes(breakdown.value);
  options(category, hasCategories ? ordered(scopedBreakdown.map(categoryOf), []) : [],
    { all: "All categories", unavailable: "Not applicable" });
  const visibleOutcomes = outcomeRows.filter(selectedCohort);
  const visibleClasses = scopedBreakdown.filter((row) => !category.value || categoryOf(row) === category.value);
  const classTitle = breakdown.value ? `${title(breakdown.value)} breakdown` : "Label breakdown";
  document.getElementById("class-title").textContent = classTitle;
  document.getElementById("class-note").textContent = hasCategories
    ? "Categories shown are available for the selected metric, cohort and breakdown."
    : "The category filter applies only to category and risk breakdowns.";
  chart("outcomes", {
    visible: visibleOutcomes,
    describe: (row) => title(row.cohort),
    empty: outcomeRows.length ? "No complete-label results for this cohort." : "Not measured for complete labels.",
    ratio,
    maximum,
  });
  chart("classes", {
    visible: visibleClasses,
    describe: (row) => !cohort.value ? `${title(row.label)} · ${title(row.cohort)}` : title(row.label),
    empty: "No breakdown results for this metric and cohort.",
    ratio,
    maximum,
  });
  document.getElementById("status").textContent =
    `${title(metric.value)}: ${visibleOutcomes.length} cohort results, ${visibleClasses.length} breakdown results.`;
}

overlapReport(overlapRows);
if (explorerRows.length) {
  const metricOrder = ordered(explorerRows.map((row) => row.metric), metrics);
  const positive = new Set(explorerRows.filter((row) => row.value > 0).map((row) => row.metric));
  const defined = new Set(explorerRows.filter((row) => row.value !== null).map((row) => row.metric));
  const defaults = ordered(metricOrder, ["recall", "precision", "coverage", "support"]);
  options(metric, metricOrder);
  metric.value = defaults.find((value) => positive.has(value))
    ?? metricOrder.find((value) => defined.has(value))
    ?? metricOrder[0];
  for (const select of [metric, cohort, category, breakdown]) select.addEventListener("change", render);
  render();
  document.getElementById("explorer").hidden = false;
} else {
  const notice = document.getElementById("notice");
  notice.textContent = overlapRows.length
    ? "No cohort or class metrics are available in this report."
    : "No evaluation metrics are available in this report.";
  notice.hidden = false;
}
'''
