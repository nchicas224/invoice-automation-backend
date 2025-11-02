# ⚙️ Invoice Automation Backend

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python&logoColor=white)
![Azure Functions](https://img.shields.io/badge/Azure-Functions-0078D4?logo=azure-functions)
![Cosmos DB](https://img.shields.io/badge/Azure-Cosmos%20DB-1E90FF?logo=azurecosmosdb)
![Microsoft Graph](https://img.shields.io/badge/Microsoft-Graph-2B579A?logo=microsoft)
![License](https://img.shields.io/badge/License-All%20Rights%20Reserved-red)

The **Invoice Automation Backend** is a cloud‑native serverless API built with **Azure Functions (Python v2)**.  
It orchestrates invoice ingestion, AI‑powered document processing, metadata persistence, and secure approval workflows for the Invoice Automation App.

---

## 📦 Overview

This backend integrates multiple Azure services to automate the lifecycle of invoices:
- Receives new invoice notifications from Microsoft Graph webhooks  
- Stores invoice files and extracted metadata in **Azure Blob Storage** and **Cosmos DB**  
- Analyzes invoices via **Azure Document Intelligence**  
- Generates and sends check request payloads for approval  
- Supports continuous renewal of webhook subscriptions via a **Timer Trigger**

---

## 🧩 Architecture

**Core Components:**
| Function | Description |
|-----------|--------------|
| `NotifyNewMail (HTTP trigger)` | Entry point for Graph webhook notifications. Validates JWT and initiates orchestration. |
| `SubscriptionRenewalTimer (Timer trigger)` | Automatically renews Graph webhook subscriptions before expiry. |
| `StoreInvoiceBlob` | Saves invoice attachments to Blob Storage. |
| `UpsertMetadata` | Inserts or updates invoice metadata in Cosmos DB. |
| `AnalyzeInvoiceActivity` | Uses Document Intelligence to parse invoices into structured JSON. |
| `GenerateAndSendCheckRequest` | Composes adaptive card payloads and dispatches to approvers via Teams/Graph. |

**Shared Utilities:**
Located in the `/shared` directory:
- `graph_client.py` → Microsoft Graph API calls  
- `database_client.py` → Cosmos DB client & vector search  
- `keyvault_client.py` → Secure secret retrieval  
- `di_process.py` & `process_ai_results.py` → Document Intelligence parsing  
- `duplicate_invoice_check.py` → Duplicate detection using embeddings  
- `azure_monitor.py` → Custom telemetry for Application Insights  

---

## 🗂️ Project Structure

```
invoice-automation-backend/
├── function_app.py               # Main entrypoint (Python v2 decorator model)
├── shared/                       # Shared modules (Graph, Cosmos, Key Vault, AI, etc.)
│   ├── graph_client.py
│   ├── database_client.py
│   ├── keyvault_client.py
│   ├── process_ai_results.py
│   └── duplicate_invoice_check.py
├── requirements.txt              # Python dependencies
├── host.json                     # Function host configuration
├── .funcignore                   # Exclusion rules for function packaging
├── .github/workflows/            # GitHub Actions CI/CD workflow
│   └── dev_invoice-automation-app(staging).yml
└── .vscode/                      # Local debugging configuration
```

---

## ⚙️ Local Development

### 🧰 Prerequisites
- **Python 3.12+**
- **Azure Functions Core Tools v4**
- **Azure CLI**
- **VS Code** with Azure Functions extension (recommended)

### 🧩 Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### ▶️ Run Locally

```bash
func start
```

---

## ☁️ Deployment (CI/CD)

This backend uses **GitHub Actions** for automated deployment to Azure.

### 🔁 Workflow Summary
- Triggered on `push` to `main` or `feature/*`
- Builds a deployment ZIP (`release.zip`)
- Publishes to the corresponding **Function App slot** (dev or prod)
- Verifies Function discovery and syncs host keys

Workflow file:
```
.github/workflows/dev_invoice-automation-app(staging).yml
```

### 🪣 Azure Resources

| Resource | Purpose |
|-----------|----------|
| **Function App** | Executes orchestrator and activity functions |
| **Blob Storage** | Stores invoice files and metadata snapshots |
| **Cosmos DB** | Stores structured invoice data and embeddings |
| **Application Insights** | Centralized logging and telemetry |
| **Key Vault** | Secrets for JWT signing, API keys, and connection strings |

---

## 🔐 Environment Variables

Environment settings are configured in **Azure Function App → Configuration**, or locally via `local.settings.json`.

| Key | Description |
|-----|-------------|
| `AZURE_STORAGE_CONNECTION_STRING` | Blob Storage connection |
| `COSMOS_ENDPOINT` / `COSMOS_KEY` | Cosmos DB credentials |
| `GRAPH_TENANT_ID` | Tenant ID for Microsoft Graph |
| `GRAPH_CLIENT_STATE_SECRET` | Secret for validating Graph webhook |
| `DOCUMENT_INTELLIGENCE_ENDPOINT` / `DOCUMENT_INTELLIGENCE_KEY` | AI service credentials |
| `KEYVAULT_URI` | Azure Key Vault instance URI |
| `APPINSIGHTS_CONNECTION_STRING` | Application Insights telemetry endpoint |

---

## 🧠 Monitoring & Logging

Telemetry is routed to **Application Insights** for:
- Function performance metrics  
- Error traces  
- Dependency calls (Graph, Cosmos, Blob)  
- Custom events from `azure_monitor.py`

Logs can also be viewed locally via:
```bash
func logs
```

---

## 🧑‍💻 Contributing

This repository is proprietary and not open to public contributions.  
Internal forks for staging or testing are permitted.

---

## 🪪 License

**All Rights Reserved © 2025 Nelson Chicas**

The source code in this repository is proprietary and may not be redistributed, modified, or used commercially without written permission.

---

*Part of the full Invoice Automation System — integrating Azure Functions, Cosmos DB, Microsoft Graph, and AI‑driven document intelligence to automate invoice workflows.*
