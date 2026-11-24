SCRIPT = r'''"use strict";

const rows = JSON.parse(document.getElementById("measurements").textContent);
const metric = document.getElementById("metric");
const cohort = document.getElementById("cohort");
const category = document.getElementById("category");
const breakdown = document.getElementById("breakdown");
const cohorts = ["overall", "uncontaminated", "trained", "unseen"];
const axes = ["category", "risk", "severity", "action"];
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

function options(select, values, { all, label = title } = {}) {
  if (all) select.add(new Option(all, ""));
  for (const value of values) select.add(new Option(label(value), value));
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

function bar(value, maximum) {
  const graphic = document.createElementNS(svg, "svg");
  graphic.setAttribute("class", "bar");
  graphic.setAttribute("aria-hidden", "true");
  graphic.setAttribute("viewBox", "0 0 1000 10");
  graphic.setAttribute("preserveAspectRatio", "none");
  const fill = document.createElementNS(svg, "rect");
  fill.setAttribute("width", String(maximum > 0 ? value / maximum * 1000 : 0));
  fill.setAttribute("height", "10");
  graphic.append(fill);
  return graphic;
}

function chart(id, axisRows, { visible, describe }) {
  const region = document.getElementById(id);
  if (!visible.length) {
    region.replaceChildren(node("p", "No values for this selection. Try another filter.", "empty"));
    return;
  }
  const ratio = ratios.has(metric.value);
  const maximum = axisRows.reduce((max, row) => Math.max(max, row.value ?? 0), ratio ? 1 : 0);
  const scale = maximum
    ? node("p", `Bar scale: ${format(0, ratio)} to ${format(maximum, ratio)}`, "scale")
    : null;
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
    if (row.value !== null) item.append(bar(row.value, maximum));
    list.append(item);
  }
  region.replaceChildren(...(scale ? [scale, list] : [list]));
}

function selectedCohort(row) {
  return !cohort.value || row.cohort === cohort.value;
}

function selectedCategory(row) {
  return category.disabled || !category.value || row.label === category.value
    || (row.axis === "risk" && row.label.startsWith(`${category.value}.`));
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
const classRows = explorerRows.filter((row) => row.axis !== "outcome");
const cohortOrder = ordered(explorerRows.map((row) => row.cohort), cohorts);
const axisOrder = ordered(classRows.map((row) => row.axis), axes);

function render() {
  category.disabled = !category.options[1] || !["category", "risk"].includes(breakdown.value);
  document.getElementById("metric-note").textContent = definitions.get(metric.value) ?? "";
  const measurements = explorerRows.filter((row) => row.metric === metric.value);
  const outcomeRows = measurements.filter((row) => row.axis === "outcome")
    .sort((a, b) => cohortOrder.indexOf(a.cohort) - cohortOrder.indexOf(b.cohort));
  const byClass = measurements.filter((row) => row.axis === breakdown.value)
    .sort((a, b) => collator.compare(a.label, b.label)
      || cohortOrder.indexOf(a.cohort) - cohortOrder.indexOf(b.cohort));
  const visibleOutcomes = outcomeRows.filter(selectedCohort);
  const visibleClasses = byClass.filter((row) => selectedCohort(row) && selectedCategory(row));
  const classTitle = breakdown.value ? `${title(breakdown.value)} breakdown` : "Class breakdown";
  document.getElementById("class-title").textContent = classTitle;
  document.getElementById("class-note").textContent = category.disabled
    ? "This breakdown is not filtered by category."
    : "Based on the selected label dimension, not the complete label.";
  chart("outcomes", outcomeRows, { visible: visibleOutcomes, describe: (row) => title(row.cohort) });
  chart("classes", byClass, {
    visible: visibleClasses,
    describe: (row) => !cohort.value ? `${title(row.label)} · ${title(row.cohort)}` : title(row.label),
  });
  document.getElementById("status").textContent =
    `${title(metric.value)}: ${visibleOutcomes.length} cohort results, ${visibleClasses.length} breakdown results.`;
}

overlapReport(overlapRows);
if (explorerRows.length) {
  options(metric, ordered(explorerRows.map((row) => row.metric), metrics));
  options(cohort, cohortOrder, {
    all: "All cohorts",
    label: (value) => value === "trained" || value === "unseen"
      ? title(value).split(" ")[0]
      : title(value),
  });
  options(category, ordered(classRows.flatMap((row) => {
    if (row.axis === "category") return [row.label];
    if (row.axis === "risk") return [row.label.split(".")[0]];
    return [];
  }), []), { all: "All categories" });
  options(breakdown, axisOrder);
  breakdown.disabled = !axisOrder.length;
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
