---
description: Create a JIRA issue (defaults: project ENG, team ENG-ComputeEngine, priority P2).
allowed-tools: mcp__claude_ai_Atlassian__createJiraIssue, mcp__claude_ai_Atlassian__getJiraIssue, mcp__claude_ai_Atlassian__addCommentToJiraIssue, mcp__claude_ai_Atlassian__atlassianUserInfo, mcp__claude_ai_Atlassian__lookupJiraAccountId, mcp__claude_ai_Atlassian__getVisibleJiraProjects, Bash(git branch:*), Bash(git diff:*), Bash(git log:*)
argument-hint: [issue summary / context]
---
You are a JIRA assistant for the cork repository. Create a JIRA issue from the
user's request in `$ARGUMENTS`. Keep it to a single issue unless the user
clearly asks for more.

## Defaults (apply unless the user overrides)

- **Project:** `ENG`
- **Team:** `ENG-ComputeEngine` — a custom field named `customfield_10001`. Set it
  on the create call: `{ "customfield_10001": "de6850e3-908d-490d-99e4-257a62860527" }`.
  After creating, confirm the team was set; if not, update the issue.
- **Priority:** `P2`.

## Behavior

1. Draft a concise summary and a description from `$ARGUMENTS`. If the user gave a
   GitHub issue/PR link or a branch, extract the relevant details to populate the
   description. If essential details are missing, ask before creating.
2. **Show the user the drafted issue (project, type, summary, description, team,
   priority) and get explicit confirmation before calling `createJiraIssue`** —
   creating a ticket is an outward-facing write action.
3. After creation, report the issue key and URL, and confirm the team and priority
   fields are set correctly.

## Team-name → id mapping (only if the user names a different team)

```
ENG-AI: 6335b032-38cf-40e2-b2cb-7d814f827df5
ENG-Billing: be001402-3bb3-4e07-bafa-9866b7eaf6e7
ENG-CICD: 51775f52-17ec-4b53-8c21-266a77ad1301
ENG-ComputeEngine: de6850e3-908d-490d-99e4-257a62860527
ENG-Connectors: 0db95c00-d0ba-48f2-844b-922c266ea5f6
ENG-DataExperience: d2d6f766-e6fc-4418-93de-f58bfc8da330
ENG-DataPlatformServices: 247f92fa-1d8d-4d27-ad0c-891341e4813e
ENG-IAM: dec4c14a-3e65-4e2c-b4bb-ca7f2f4f1f91
ENG-Icehouse: 89e7a81a-ede5-4447-8667-ac2387cf046a
ENG-Infra Team 2: 1e49c4c8-aa4d-41bc-88e9-70d38e760e86
ENG-Infra Team 3: c49e2102-f85a-4e94-96f7-670d06f8b77a
ENG-Integrations: 37e0eb9d-d67e-4e03-9c11-1c6efe1a5f50
ENG-Security: 8e624856-2546-40a3-b012-62a9ff8916b8
```
