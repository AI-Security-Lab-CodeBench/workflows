STYLES = """* { box-sizing: border-box; }
[hidden] { display: none !important; }
:root {
  background: var(--page);
  color: var(--ink);
  font: var(--body)/1.55 var(--sans);
  scrollbar-color: var(--edge) var(--page);
}
::selection { background: var(--wash); color: var(--ink); }
body { margin: 0; background: var(--page); font-size: 1rem; }
main {
  width: min(100%, var(--shell));
  margin-inline: auto;
  padding: var(--s8) clamp(var(--s4), 4vw, var(--s7));
}
header {
  padding-block-end: var(--s5);
  border-block-end: var(--stroke) solid var(--edge);
}
header p { margin-block: var(--s3) 0; }
h1, h2, h3, p { margin: 0; overflow-wrap: anywhere; }
h1, h2, h3 { line-height: 1.12; }
h1 { font: 400 var(--display)/1.06 var(--serif); letter-spacing: -0.035em; }
h2 { font-size: var(--heading); }
h3 { font-size: var(--subheading); font-weight: 700; }
p { max-inline-size: var(--measure); text-wrap: pretty; }
main > section, .notice { margin-block-start: var(--s6); }
.filters {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: var(--s3);
  margin-block: var(--s4) var(--s3);
}
label { display: grid; gap: var(--s1); font-weight: 650; min-inline-size: 0; }
select {
  min-block-size: var(--hit);
  inline-size: 100%;
  min-inline-size: 0;
  padding: var(--s2);
  border: var(--stroke) solid var(--edge);
  border-radius: var(--radius);
  background: var(--paper);
  color: var(--ink);
  font: inherit;
}
select:hover { background: var(--wash); }
select:active { border-color: var(--accent); }
select:disabled { color: var(--muted); background: var(--track); }
select.primary { color: var(--accent-ink); background: var(--accent); }
select.primary:hover { background: var(--accent-hover); }
select.primary:active { background: var(--accent-pressed); }
select option { color: var(--ink); background: var(--paper); }
select:focus-visible { outline: var(--focus-width) solid var(--focus); outline-offset: var(--focus-width); }
.note, .scale, .undefined { color: var(--muted); }
.note { font-size: var(--small); }
.comparisons {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  gap: var(--s5);
  align-items: start;
  margin-block-start: var(--s5);
}
.panel {
  min-inline-size: 0;
  background: var(--paper);
  border: var(--stroke) solid var(--line);
  border-radius: var(--radius);
  padding: clamp(var(--s4), 3vw, var(--s6));
}
.panel > .note, #overlap > .note { margin-block-start: var(--s2); }
.distribution { margin-block-start: var(--s5); }
#distribution { margin-block-start: var(--s3); }
.outcome-stack {
  display: block;
  inline-size: 100%;
  block-size: var(--s5);
  border-radius: var(--s1);
  overflow: hidden;
}
.outcome-correct { --outcome-color: var(--outcome-correct); }
.outcome-wrong { --outcome-color: var(--outcome-wrong); }
.outcome-abstain { --outcome-color: var(--outcome-abstain); }
.outcome-error { --outcome-color: var(--outcome-error); }
.outcome-stack rect { fill: var(--outcome-color); }
.outcome-counts {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: var(--s2) var(--s4);
  margin: var(--s2) 0 0;
  font-size: var(--small);
}
.outcome-counts dt::before {
  content: "";
  display: inline-block;
  inline-size: var(--s2);
  block-size: var(--s2);
  margin-inline-end: var(--s1);
  border-radius: var(--s1);
  background: var(--outcome-color);
}
.scale, .empty { margin-block-start: var(--s4); }
.scale {
  font-size: var(--small);
  border-block-end: var(--stroke) solid var(--edge);
  padding-block-end: var(--s2);
}
.scale-ticks {
  display: flex;
  justify-content: space-between;
  gap: var(--s2);
  margin-block-start: var(--s1);
  overflow-wrap: anywhere;
}
.bars { list-style: none; padding: 0; margin: 0; }
.bars li {
  display: grid;
  gap: var(--s1);
  padding-block: var(--s3);
  border-block-end: var(--stroke) solid var(--line);
}
.bars li:last-child { border-block-end: 0; padding-block-end: 0; }
.reading {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: var(--s2) var(--s4);
  flex-wrap: wrap;
  overflow-wrap: anywhere;
}
.value { font-weight: 700; }
.value, .scale, dd { font-variant-numeric: tabular-nums; }
.undefined { font-weight: 550; }
.bar {
  display: block;
  inline-size: 100%;
  block-size: var(--s3);
  color: var(--accent);
  background: var(--track);
  border-radius: var(--s1);
  overflow: hidden;
}
.bar rect { fill: currentColor; }
.notice, #overlap { padding-block-start: var(--s4); border-block-start: var(--stroke) solid var(--edge); }
dl { display: flex; flex-wrap: wrap; gap: var(--s4) var(--s7); margin: var(--s3) 0 0; }
dl > div { min-inline-size: 0; max-inline-size: 100%; overflow-wrap: anywhere; }
dt { font-weight: 650; }
dd { margin: 0; }
.visually-hidden {
  position: absolute;
  inline-size: 1px;
  block-size: 1px;
  overflow: hidden;
  clip-path: inset(50%);
  white-space: nowrap;
}
@media (max-width: 46rem) {
  main { padding-block: var(--s6); }
  .filters { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .comparisons { grid-template-columns: minmax(0, 1fr); gap: var(--s4); }
}
@media (max-width: 30rem) {
  .filter-wide { grid-column: 1 / -1; }
  .outcome-counts { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 22rem) {
  .filters { grid-template-columns: minmax(0, 1fr); }
}
@media (forced-colors: active) {
  .bar { border: var(--stroke) solid CanvasText; }
  .bar rect { fill: Highlight; }
}
"""
