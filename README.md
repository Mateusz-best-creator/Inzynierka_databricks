Follow these steps to set up and deploy the entire project:

## Step 1: Authenticate and Configure Azure Storage Provider

1. Run Azure CLI login:

```bash
az login
```

2. If you encounter an error stating that the Microsoft Storage provider is not registered, run the following commands:

```bash
az provider register --namespace 'Microsoft.Storage'
az provider show -n Microsoft.Storage --query "registrationState" -o tsv
```

## Step 2: Deploy State Storage Infrastructure via Terraform

1. Navigate to the state storage directory:

```bash
cd terraform/state_storage
```

2. Initialize Terraform:

```bash
terraform init
```

3. Generate the Terraform plan:

```bash
terraform plan -out terraform.plan
```

4. Apply the Terraform plan:

```bash
terraform apply terraform.plan
```

## Step 3: Configure Infrastructure and Environment Variables

1. Navigate to the infrastructure directory:

```bash
cd terraform/infrastructure
```

2. Copy the Resource Group name and Storage Account name from the previous Terraform output, and update them in the `providers.tf` file inside the infrastructure folder.

3. Navigate to `ecommerce/table_properties/dev/bronze` and update the `storage_account_name` value to match your newly created storage account.

4. Open the `nb_file_ingestion` notebook and update the `storage_account` variable with the latest storage account name.

5. Open `global_variables.yml` and update `notification_email` with your personal email address to receive notifications about main pipeline runs.

## Step 4: Apply Infrastructure Terraform Configuration

1. Initialize Terraform again:

```bash
terraform init
```

2. Generate the Terraform plan:

```bash
terraform plan -out terraform.plan
```

3. Apply the Terraform plan:

```bash
terraform apply terraform.plan
```

## Step 5: Configure Azure Key Vault and Secrets

1. Navigate to your newly created storage account in the Azure Portal, go to Access keys, and copy one of the access keys.

2. Go to your Azure Key Vault service and create a new secret:

- Secret Name: storageaccountaccesskey

- Secret Value: Paste the copied access key.

- Leave all other options as default.

3. Still in the Azure Key Vault service, go to the Access Policies section and click Create:

- Under Secret permissions, select Get and List.

- Under Principal, select your Databricks service principal.

4. Copy your Databricks instance URL and open this link in your browser: `https://<databricks-instance>#secrets/createScope`.

5. Create a new secret scope named `secret-keys`. Copy the Vault URI and Resource ID from the Key Vault properties and paste them into the Databricks interface.

## Step 6: Set Up Databricks Catalog, Schemas, and Data Generation

1. In the Databricks catalog, create a new catalog named `databricks_dev_domain`.

2. Within this catalog, create three schemas:

- ecommerce_bronze

- ecommerce_silver

- ecommerce_gold

3. Execute the data generation script located at `ecommerce/data_generation/main.py`. Upload the generated files into the appropriate virtual folders in the rawdatastorage container based on the bronze table configuration.

## Step 7: Deploy via Databricks Asset Bundles (DAB)

1. Install the Databricks CLI if you haven't already.

2. Open the `dev.yml` file and update the workspace_id with your deployed Databricks workspace ID.

3. Log in to Databricks CLI:

```bash
databricks auth login --host <databricks_url>
```

4. Deploy the Databricks bundle for the development environment:

```bash
databricks bundle deploy -t dev
```

Once completed, the pipeline will be successfully deployed and ready to run!
