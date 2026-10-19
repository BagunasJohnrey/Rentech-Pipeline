# Extraction Log Retention and Access

| Item | Rule |
|---|---|
| Retention period | At least 12 months for logs, raw folders, checkpoints, and quarantine files. Covers one school year, seasonal peaks, and defense re-checks. |
| Storage location | `data/logs/`, `data/raw/`, `data/state/`, `data/quarantine/`. Zip a backup of `data/` before submission or defense. |
| Access permissions | Team members and the panel can read. Only the runner can write. Passwords never appear in logs; `.env` is never shared or committed. |
| Archiving | Logs older than 90 days zipped monthly to `data/logs/archive/pipeline_YYYY-MM.zip`; keep originals until the 12-month mark. |
| Deletion rules | Not deleted before the retention period or while needed for troubleshooting, auditing, or recovery. Deletion needs lead approval and proof that final reports still exist. |
| Operational vs audit | Daily run logs are operational. Dated raw folders and checkpoints are audit records and follow the same 12-month minimum. |
