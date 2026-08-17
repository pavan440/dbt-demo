import argparse
from pathlib import Path


SOURCE_YAML = """version: 2

sources:
  - name: wayplorer_bronze
    database: "{{ target.database }}"
    schema: "{{ var('wayplorer_bronze_schema', 'BRONZE') }}"
    description: Raw Wayplorer CRM entities landed before Silver normalization.
    tables:
      - name: account_raw
        identifier: ACCOUNT_RAW
        description: Raw account CDC records.
        columns:
          - name: applicationId
            tests:
              - not_null
          - name: accountId
            tests:
              - not_null
          - name: clientId
            tests:
              - not_null
          - name: cdc_timestamp
            tests:
              - not_null
      - name: lead_raw
        identifier: LEAD_RAW
        description: Raw lead CDC records.
        columns:
          - name: applicationId
            tests:
              - not_null
          - name: leadId
            tests:
              - not_null
          - name: clientId
            tests:
              - not_null
          - name: cdc_timestamp
            tests:
              - not_null
      - name: opportunity_raw
        identifier: OPPORTUNITY_RAW
        description: Raw opportunity CDC records.
        columns:
          - name: applicationId
            tests:
              - not_null
          - name: opportunityId
            tests:
              - not_null
          - name: clientId
            tests:
              - not_null
          - name: cdc_timestamp
            tests:
              - not_null
      - name: contract_raw
        identifier: CONTRACT_RAW
        description: Raw contract CDC records.
        columns:
          - name: applicationId
            tests:
              - not_null
          - name: contractId
            tests:
              - not_null
          - name: clientId
            tests:
              - not_null
          - name: cdc_timestamp
            tests:
              - not_null
      - name: call_raw
        identifier: CALL_RAW
        description: Raw call activity CDC records.
        columns:
          - name: applicationId
            tests:
              - not_null
          - name: callId
            tests:
              - not_null
          - name: clientId
            tests:
              - not_null
          - name: cdc_timestamp
            tests:
              - not_null
      - name: email_raw
        identifier: EMAIL_RAW
        description: Raw email activity CDC records.
        columns:
          - name: applicationId
            tests:
              - not_null
          - name: emailId
            tests:
              - not_null
          - name: clientId
            tests:
              - not_null
          - name: cdc_timestamp
            tests:
              - not_null
      - name: task_raw
        identifier: TASK_RAW
        description: Raw task activity CDC records.
        columns:
          - name: applicationId
            tests:
              - not_null
          - name: taskId
            tests:
              - not_null
          - name: clientId
            tests:
              - not_null
          - name: cdc_timestamp
            tests:
              - not_null
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Write the Wayplorer bronze dbt source YAML.")
    parser.add_argument(
        "--output",
        default="models/sources/wayplorer_bronze.yml",
        help="Target dbt source YAML path.",
    )
    args = parser.parse_args()

    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(SOURCE_YAML, encoding="utf-8")
    print(f"Wrote bronze source YAML to {target}")


if __name__ == "__main__":
    main()