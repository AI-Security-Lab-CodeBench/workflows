from string import Template

MARKUP = Template("""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="referrer" content="no-referrer">
  <meta name="description" content="Held-out Bashguard rules evaluation and category metrics.">
  <meta http-equiv="Content-Security-Policy" content="$policy">
  <title>Rules evaluation</title>
  <style>$styles</style>
</head>
<body>
  <main>
    <header>
      <h1>Rules evaluation</h1>
      <p>Compare Bashguard’s exact-label outcomes across test cohorts and examine performance by category.</p>
    </header>
    <section id="explorer" aria-labelledby="explorer-title" hidden>
      <h2 id="explorer-title">Explore metrics</h2>
      <div class="filters">
        <label for="metric">Metric<select id="metric" class="primary"></select></label>
        <label for="cohort">Cohort<select id="cohort"></select></label>
        <label for="category">Category<select id="category"></select></label>
        <label for="breakdown">Breakdown<select id="breakdown"></select></label>
      </div>
      <p id="metric-note" class="note"></p>
      <div id="status" class="visually-hidden" role="status"></div>
      <div class="comparisons">
        <section class="panel" aria-labelledby="outcome-title">
          <h3 id="outcome-title">Cohort outcomes</h3>
          <p class="note">Exact matches on the complete label, regardless of category filter.</p>
          <div id="outcomes"></div>
        </section>
        <section class="panel" aria-labelledby="class-title">
          <h3 id="class-title">Category breakdown</h3>
          <p class="note" id="class-note"></p>
          <div id="classes"></div>
        </section>
      </div>
    </section>
    <p id="notice" class="notice" hidden></p>
    <section id="overlap" aria-labelledby="overlap-title" hidden>
      <h2 id="overlap-title">Train–test overlap</h2>
      <p class="note">Exact command matches between the two splits. The uncontaminated cohort excludes overlapping test rows.</p>
      <dl id="overlap-values"></dl>
    </section>
    <noscript><p class="notice">Enable JavaScript to view this offline report.</p></noscript>
  </main>
  <script id="measurements" type="application/json">$rows</script>
  <script>$script</script>
</body>
</html>
""")
