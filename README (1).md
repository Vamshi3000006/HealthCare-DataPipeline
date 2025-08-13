
# 🏥 Healthcare Data Engineering Project (GCP + Delta Lake + PySpark)

This project demonstrates a full-scale healthcare data pipeline built using modern data engineering tools on Google Cloud Platform (GCP). It processes healthcare data from HL7, CSV claims, and OpenFDA APIs, applying strong data governance, standardization, and security practices.

---

## 🧱 Project Architecture Overview

**Ingestion → Bronze → Silver → Silver Unified → Silver Secure → Gold → Analytics**

```
           +-------------------------+
           |     HL7 / CSV / API     |
           +-------------------------+
                      |
               [Kafka / GCS]
                      |
            ┌───────────────────────┐
            │ Bronze Delta Lake     │
            └───────────────────────┘
                      ↓
            ┌───────────────────────┐
            │ Silver Delta Lake     │
            └───────────────────────┘
                      ↓
        ┌───────────────────────────────┐
        │ Silver Unified Standardized  │
        └───────────────────────────────┘
                      ↓
        ┌───────────────────────────────┐
        │ Silver Unified Secure (PII)   │
        └───────────────────────────────┘
                      ↓
        ┌───────────────────────────────┐
        │ Gold Zone (Star Schema)       │
        └───────────────────────────────┘
                      ↓
            ┌───────────────────────┐
            │ Dashboards / Analytics│
            └───────────────────────┘
```

---

## 📁 Repository Structure

| Folder | Description |
|--------|-------------|
| `01-ingestion-pipeline/` | Scripts to ingest HL7 (Kafka), CSV (CMS Claims), and OpenFDA API |
| `02-Standardization_and_data_profiling/` | Great Expectations, data validation & profiling |
| `03-processing-delta-spark/` | Spark jobs for Bronze → Silver → Unified → Secure layers |
| `04-dbt-models-star-schema/` | DBT models for transforming Silver into Gold zone |
| `05-analytics-dashboard/` | BigQuery SQL + Looker Studio dashboard files |
| `06-orchestration-automation/` | Airflow DAGs, NiFi templates, decorators |
| `07-governance-lineage-security/` | Cloud DLP, tokenization, Data Catalog, Dataplex |
| `terraform/` | Infra-as-code for provisioning GCS, BQ, and Delta infra |
| `utils/` | Helper decorators (`@timer`, `@retry`, `@log_inputs`) |

---

## 🔧 Key Tools & Technologies

- **GCP**: GCS, BigQuery, Dataflow, Cloud Functions, Dataproc, Cloud Monitoring, IAM
- **Apache Kafka**: For HL7 real-time ingestion
- **Apache Spark (PySpark)**: For batch/stream processing and transformation
- **Delta Lake**: For ACID storage, checkpointing, deduplication
- **Great Expectations**: Validation of patient_id, dob, age, etc.
- **Google Cloud DLP**: For PII detection and masking (e.g., patient_name, SSN)
- **Terraform**: Infra-as-code for secured data warehouse & resource provisioning
- **Apache NiFi on GKE**: For orchestration and data routing (Helm-based deployment)
- **Google Data Catalog & Dataplex**: For metadata, PII tagging, and data lineage
- **dbt**: Star schema creation and business-friendly Gold layer
- **Looker Studio**: Final analytics dashboards

---

## 🔐 Data Governance & Security Highlights

- Cloud DLP masking/tokenization for fields like patient_id, dob, SSN
- Role-based access to buckets and BQ datasets via IAM
- Tags and classifications using Data Catalog + Dataplex
- Dead-letter handling for malformed HL7/CSV/API rows
- Exactly-once processing: deduplication + checkpointing in Spark
- Atomic writes via Delta Lake
- Alerting via Cloud Monitoring (e.g., failed jobs, query spikes)
- Terraform-secured BigQuery setup with VPC-SC, CMEK support

---

## 🚀 Pipeline Flow (Example: HL7)

1. HL7 Messages → Kafka → PySpark parses segments (MSH, PID, PV1...)
2. Bronze: Raw HL7 → `bronze/hl7`
3. Silver: Structured HL7 → `silver/hl7`
4. Unified: Mapped schema via `unify-config.json` → `silver_unified/hl7`
5. Secure: Tokenized and masked PII fields → `silver_unified_secure/hl7`
6. GE Validations → Gold Zone (via dbt)
7. Visualized in BigQuery + Looker

---

## ✅ Key Features Already Implemented

- [x] Multi-source ingestion (CSV, HL7, API)
- [x] Dynamic schema mapping via config
- [x] Great Expectations validations
- [x] PII masking + tokenization
- [x] Deduplication and exactly-once writes
- [x] Data Catalog tagging and Dataplex integration
- [x] Terraform secured infra for GCS & BigQuery
- [x] Orchestration via NiFi (Kubernetes-deployed)
- [x] Cloud Monitoring integration for pipeline observability

---

## 📊 Dashboards

- Looker Studio: Claims summary, diagnosis trends, patient demographics
- BigQuery Views: Gold-layer curated tables for downstream analytics

---

## 🤝 Contributions

Feel free to fork, raise issues, or suggest enhancements via pull request.


---

**Built by Vamshi Krishna — Cloud & Data Engineer**

Connect on https://www.linkedin.com/in/vamshi-krishna-musham/