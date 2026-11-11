# Native asset data and review

The design system is the pinned Dagster ui-components package. Button, Table,
TextInput, TextArea, Dialog, Radio, Box and Text retain their native anatomy,
typography, focus, disabled and theme states. No second shell or palette.

Spacing uses Dagster's 4, 8, 12, 16, 20 and 24 pixel scale. CSS tokens below
name only extensions needed for this surface: 44 pixel touch targets, a 720
pixel narrow navigation threshold, and a 320 pixel intrinsic pane floor.
Colors and fonts come from Dagster CSS variables in both themes.

Layout: Data and Review - query and inspect persisted rows -> wrapping toolbar,
table reel and intrinsic list/detail switcher; scroll owner: the extension
section vertically, table reel horizontally, and bounded value cells
horizontally for their own unwrapped text. Do not let long cell contents
size the page or place actions outside the available content width.

Data priority: category/query, filtered and total counts, complete row inspection.
Review priority: pending counts and reviewer, selected record and evidence,
explicit decision save. Complete review and publish is a separate final action.
No decorative tiles, headings, animation, or product chrome.

Saved decision correction stays in the selected-record inspector as a secondary
Retract decision action. It explains the pending state and invalidation of
dependent results. The primary completed-stage action is the descriptor's
continuation label when advertised; run navigation does not require a CSV.
Stale tables keep their rows with a clear historical/not-currently-approved
warning. All controls reuse Dagster Button and existing stack/toolbar/status
classes, without new layout, colors, spacing or typography.

Controls wrap rather than shrink beyond usable widths. The review inspector
stacks below the table when two 320 pixel panes do not fit. Full text uses
pre-wrap and arbitrary-word wrapping in inspectors, never HTML interpretation
or truncation. Table values preserve whitespace with `pre` inside bounded,
keyboard-focusable scroll regions and support normal text selection/copy.
Selecting a row still opens every complete cell.

Narrow navigation collapses once on entering a narrow viewport through the native
context. Explicit user reopening is respected until the viewport leaves and
reenters the narrow range. A native-context toggle remains available in the view.

Verification ledger: build, strict typecheck, focused tests, desktop/mobile,
light/dark, keyboard and real-endpoint QA are assigned to the integration lead.
This frontend unit must not start builds, tests, servers or browser sessions.
