# Import Project from GitHub to Databricks Workspace

## Complete Step-by-Step Guide

This guide shows you how to import your `insurance-data-ai` project from GitHub into your Databricks workspace using Databricks Repos.

---

## Method 1: Using Databricks Repos (RECOMMENDED) 🌟

### Prerequisites
- ✅ GitHub repository: `https://github.com/Siddhartha-data-ai/insurance-data-ai`
- ✅ Access to a Databricks workspace
- ✅ Permission to create repos in Databricks

### Step 1: Navigate to Repos

1. **Open your Databricks workspace** in a web browser
   - URL format: `https://<your-workspace>.cloud.databricks.com`

2. **Click on "Repos"** in the left sidebar
   - If you don't see it, click the workspace icon (folder icon) first

### Step 2: Create New Repo

1. **Click "Add Repo"** button (top right corner)
   - Or click "Create Repo" if this is your first repo

2. **Fill in the Repo Details**:

   | Field | Value |
   |-------|-------|
   | **Git repository URL** | `https://github.com/Siddhartha-data-ai/insurance-data-ai` |
   | **Git provider** | `GitHub` (auto-detected) |
   | **Repository name** | `insurance-data-ai` (auto-filled) |

3. **Click "Create Repo"**

### Step 3: Wait for Import

- Databricks will clone your repository
- This takes **30-60 seconds** depending on project size
- You'll see a progress indicator

### Step 4: Verify Import

Once imported, you should see this folder structure in Databricks:

```
/Repos/<your-username>/insurance-data-ai/
├── config/
│   └── template.json
├── resources/
│   ├── grants/
│   ├── jobs/
│   ├── pipelines/
│   └── schemas/
├── src/
│   ├── analytics/
│   ├── bronze/
│   ├── chatbot/
│   ├── gold/
│   ├── ml/
│   ├── pipelines/          ← NEW: 5 DLT notebooks
│   │   ├── bronze_to_silver_customers.py
│   │   ├── bronze_to_silver_policies.py
│   │   ├── bronze_to_silver_claims.py
│   │   ├── bronze_to_silver_agents.py
│   │   └── bronze_to_silver_payments.py
│   ├── setup/
│   ├── silver/
│   └── transformations/
├── databricks.yml
├── README.md
├── DLT_DEPLOYMENT_GUIDE.md  ← NEW
└── [all other .md files]
```

### Step 5: Verify DLT Notebooks

1. **Navigate to**: `/Repos/<your-username>/insurance-data-ai/src/pipelines/`
2. **Open any notebook** (e.g., `bronze_to_silver_customers.py`)
3. **Verify it displays correctly** with all cells

✅ **Success!** Your project is now in Databricks!

---

## Method 2: Using Databricks CLI

### Prerequisites

```bash
# Install Databricks CLI
brew tap databricks/tap
brew install databricks

# Verify installation
databricks --version
# Should show: Databricks CLI v0.200.0+
```

### Step 1: Configure Authentication

```bash
# Start configuration
databricks configure --token

# You'll be prompted for:
# 1. Databricks Host: https://<your-workspace>.cloud.databricks.com
# 2. Token: <your-personal-access-token>
```

**To get your Personal Access Token (PAT):**
1. In Databricks workspace → Click your username (top right)
2. Click **User Settings**
3. Go to **Developer** → **Access tokens**
4. Click **Generate new token**
5. Name: "CLI Access"
6. Lifetime: 90 days (or as needed)
7. Click **Generate**
8. **Copy the token** (you won't see it again!)

### Step 2: Deploy Using Databricks Asset Bundles

```bash
# Navigate to project directory
cd /Users/kanikamondal/Databricks/insurance-data-ai

# Validate the bundle configuration
databricks bundle validate -e dev

# Deploy everything to Databricks
databricks bundle deploy -e dev
```

**What this does:**
- ✅ Creates Unity Catalog catalogs (bronze, silver, gold)
- ✅ Creates all schemas
- ✅ Uploads all notebooks to workspace
- ✅ Creates Delta Live Tables pipeline
- ✅ Creates orchestration jobs
- ✅ Applies security grants

### Step 3: Verify Deployment

```bash
# List deployed resources
databricks bundle summary -e dev

# Check if pipelines were created
databricks pipelines list | grep insurance

# Check if jobs were created
databricks jobs list | grep insurance
```

---

## Method 3: Manual Upload (Not Recommended)

If you can't use Repos or CLI, here's the manual approach:

### Step 1: Create Folder Structure

In Databricks Workspace:
1. Navigate to **Workspace → Shared**
2. Right-click → **Create → Folder**
3. Name it: `insurance-data-ai`

### Step 2: Upload Notebooks

1. **Open the GitHub repo** in your browser:
   ```
   https://github.com/Siddhartha-data-ai/insurance-data-ai
   ```

2. **For each Python notebook** in `src/` folders:
   - Open the file on GitHub
   - Click **Raw** button
   - Copy the entire content
   - In Databricks: Right-click folder → **Create → Notebook**
   - Paste the content
   - Save with same name

3. **Repeat for all folders**:
   - `src/analytics/` (4 notebooks)
   - `src/bronze/` (3 notebooks)
   - `src/chatbot/` (3 notebooks)
   - `src/gold/` (2 notebooks)
   - `src/ml/` (9 notebooks)
   - `src/pipelines/` (5 DLT notebooks) ← **Important!**
   - `src/transformations/` (1 notebook)

⚠️ **This is tedious!** Use Method 1 (Repos) instead.

---

## After Import: Setting Up the Pipeline

### Option A: Using Databricks Repos (Already Set Up)

If you used Method 1 (Repos), you now need to:

#### 1. Create Unity Catalog Structure

```sql
-- Run in Databricks SQL Editor or Notebook

-- Create catalogs
CREATE CATALOG IF NOT EXISTS insurance_dev_bronze;
CREATE CATALOG IF NOT EXISTS insurance_dev_silver;
CREATE CATALOG IF NOT EXISTS insurance_dev_gold;

-- Create schemas (run the setup scripts from your repo)
```

Or run the SQL files from your repo:
- `/Repos/<your-username>/insurance-data-ai/src/setup/00_create_catalog.sql`
- `/Repos/<your-username>/insurance-data-ai/src/setup/01_create_bronze_tables.sql`
- `/Repos/<your-username>/insurance-data-ai/src/setup/02_create_silver_tables.sql`

#### 2. Create Delta Live Tables Pipeline

1. **Go to**: Workflows → Delta Live Tables
2. **Click**: "Create Pipeline"
3. **Configure**:

   | Setting | Value |
   |---------|-------|
   | **Pipeline Name** | `insurance_dev_bronze_to_silver_pipeline` |
   | **Product Edition** | Advanced or Core |
   | **Target** | `insurance_dev_silver` |
   | **Storage Location** | (optional, or specify path) |

4. **Add Notebooks** (click "+ Add notebook library"):
   - `/Repos/<your-username>/insurance-data-ai/src/pipelines/bronze_to_silver_customers.py`
   - `/Repos/<your-username>/insurance-data-ai/src/pipelines/bronze_to_silver_policies.py`
   - `/Repos/<your-username>/insurance-data-ai/src/pipelines/bronze_to_silver_claims.py`
   - `/Repos/<your-username>/insurance-data-ai/src/pipelines/bronze_to_silver_agents.py`
   - `/Repos/<your-username>/insurance-data-ai/src/pipelines/bronze_to_silver_payments.py`

5. **Add Configuration** (Key-Value pairs):
   ```
   bronze_catalog = insurance_dev_bronze
   silver_catalog = insurance_dev_silver
   pipeline.environment = dev
   ```

6. **Cluster Settings** (optional):
   - Node type: `i3.xlarge` or similar
   - Min workers: 1
   - Max workers: 2
   - Enable Autoscaling: Yes
   - Enable Photon: Yes (if available)

7. **Click**: "Create"

#### 3. Generate Bronze Data

Run these notebooks in order:
1. `/Repos/<your-username>/insurance-data-ai/src/bronze/generate_customers_data.py`
2. `/Repos/<your-username>/insurance-data-ai/src/bronze/generate_policies_data.py`
3. `/Repos/<your-username>/insurance-data-ai/src/bronze/generate_claims_data.py`

#### 4. Run DLT Pipeline

1. Go to your DLT pipeline
2. Click **Start**
3. Wait for completion (15-20 minutes)

#### 5. Build Gold Layer

Run these notebooks:
1. `/Repos/<your-username>/insurance-data-ai/src/gold/build_customer_360.py`
2. `/Repos/<your-username>/insurance-data-ai/src/gold/build_fraud_detection.py`

### Option B: Using Databricks CLI (Automated)

If you used Method 2 (CLI), the pipeline is already created! Just run:

```bash
# Run the full ETL process
databricks bundle run insurance_etl_full_refresh -e dev
```

This executes everything automatically:
1. ✅ Generates bronze data
2. ✅ Runs DLT pipeline
3. ✅ Applies security rules
4. ✅ Builds gold layer

---

## Updating Your Code

### If You Used Repos (Method 1)

When you push updates to GitHub:

1. **Push changes locally**:
   ```bash
   cd /Users/kanikamondal/Databricks/insurance-data-ai
   git add .
   git commit -m "Your changes"
   git push origin main
   ```

2. **Pull in Databricks**:
   - Go to your repo in Databricks
   - Click the **branch dropdown** (shows "main")
   - Click **"Pull"** or refresh icon
   - Changes sync automatically!

### If You Used CLI (Method 2)

Simply redeploy:
```bash
databricks bundle deploy -e dev
```

---

## Troubleshooting

### Issue 1: "Repository not found" or "Access Denied"

**Solution:**
- Ensure your repository is **public**, OR
- Add your Databricks account to GitHub repository collaborators, OR
- Configure GitHub authentication in Databricks:
  1. User Settings → Git Integration
  2. Add GitHub Personal Access Token with `repo` scope

### Issue 2: "Cannot create repo in workspace"

**Solution:**
- You need **Can Manage** permission on the workspace
- Contact your workspace admin
- Or use a different location: `/Users/<your-username>/insurance-data-ai`

### Issue 3: DLT Pipeline Fails with "Table not found"

**Solution:**
- Ensure Bronze tables exist first
- Run setup scripts: `src/setup/01_create_bronze_tables.sql`
- Generate bronze data first

### Issue 4: "Insufficient Unity Catalog Permissions"

**Solution:**
```sql
-- Ask workspace admin to grant:
GRANT CREATE CATALOG ON METASTORE TO `<your-email>`;
GRANT USE CATALOG ON insurance_dev_bronze TO `<your-email>`;
GRANT USE CATALOG ON insurance_dev_silver TO `<your-email>`;
```

### Issue 5: Notebooks Show as Text Files

**Solution:**
- Databricks Repos correctly identifies `.py` files as notebooks if they have:
  - `# Databricks notebook source` header
  - `# MAGIC` commands
- Your files already have these! If not rendering, try:
  - Right-click → Open as → Notebook

---

## Quick Reference Commands

### Using Git Locally
```bash
# Check status
cd /Users/kanikamondal/Databricks/insurance-data-ai
git status

# Push updates
git add .
git commit -m "Update message"
git push origin main
```

### Using Databricks CLI
```bash
# Validate configuration
databricks bundle validate -e dev

# Deploy changes
databricks bundle deploy -e dev

# Run jobs
databricks bundle run insurance_etl_full_refresh -e dev

# Check pipeline status
databricks pipelines list | grep insurance
```

### Using Databricks SQL
```sql
-- Verify catalogs
SHOW CATALOGS LIKE 'insurance_dev%';

-- Check tables
SHOW TABLES IN insurance_dev_silver.customers;

-- Verify data
SELECT COUNT(*) FROM insurance_dev_bronze.customers.customer_raw;
```

---

## Next Steps After Import

1. ✅ **Import repository** (Method 1 recommended)
2. ✅ **Run setup scripts** to create catalogs/schemas
3. ✅ **Create DLT pipeline** with the 5 notebooks
4. ✅ **Generate bronze data**
5. ✅ **Run DLT pipeline**
6. ✅ **Build gold layer**
7. ✅ **Run ML models** (optional)
8. ✅ **Launch chatbot** (optional)

---

## Summary: Recommended Path

### 🌟 Best Approach: Databricks Repos

1. **Databricks Workspace** → **Repos** → **Add Repo**
2. **URL**: `https://github.com/Siddhartha-data-ai/insurance-data-ai`
3. **Create Repo** → Wait 60 seconds → ✅ Done!

**Advantages:**
- ⚡ Fastest (< 2 minutes)
- 🔄 Easy updates (just pull from Git)
- 📂 Maintains folder structure
- 🎯 No CLI installation needed
- ✅ Works with all Databricks features

---

**Repository URL**: https://github.com/Siddhartha-data-ai/insurance-data-ai  
**Last Updated**: October 15, 2025  
**Status**: ✅ Ready for import

