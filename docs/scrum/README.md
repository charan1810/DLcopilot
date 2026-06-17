# Data Lifecycle Copilot Scrum Sheets

This folder contains ready-to-import CSV files for Google Sheets.

## Files

- `DLcopilot_Scrum_Master.csv`
  - Full roadmap from project foundation to planned future development.
  - Includes done, in progress, and planned tasks.
  - Best for leadership and sprint planning.

- `DLcopilot_Individual_Sprint_Template.csv`
  - Individual-level sprint tracking template.
  - Best for daily scrum updates and assignment ownership.

## Import into Google Sheets

1. Open Google Sheets.
2. Select File -> Import -> Upload.
3. Upload one CSV file.
4. Choose Insert new sheet(s).
5. Repeat for the second CSV so each becomes its own tab.

## Recommended tab names

- `Master Roadmap`
- `Individual Sprint Board`

## Status standard

Use only these values for consistency:

- `Done`
- `In Progress`
- `Planned`
- `Blocked`

## Daily scrum update format

For each assigned row in the individual sprint tab:

- Update `Daily_Update` with what changed today.
- Update `Blockers` if anything is blocked.
- Keep `Definition_of_Done` unchanged unless scope changed.

## Suggested ownership model

If you do not have named assignees yet, assign by interest first:

- Backend and API
- Frontend and UX
- Data Modeling and Analytics
- QA and Automation
- DevOps and Platform
- Security and Governance
