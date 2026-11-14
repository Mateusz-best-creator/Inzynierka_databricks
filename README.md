Steps to setup the entire project:

1. az login
1.2. If you see error that microsoft storage is not registered then run below query:
1.3. az provider register --namespace 'Microsoft.Storage'
1.4. az provider show -n Microsoft.Storage --query "registrationState" -o tsv
4. go to terraform/state_storage directory
5. terraform init
6. terraform plan -out terraform.plan
7. terraform apply terraform.plan
8. go to terraform/infrastructure directory
8.2 Copy the resource group name and storage account name from terminal output and change it in the providers.tf ile in infrastructure folder
8.3 Go to ecommerce\table_properties\dev\bronze directory and update storage_account_name value for the newly created storage account
8.4. Then go to nb_file_ingestion notebook and update storage_account variable to have the latest value
8.5. Then go to global_variables.yml and update notification_email to reveive notifations about main pipeline runs into you personal email.
9. terraform init
10. terraform plan -out terraform.plan
11. terraform apply terraform.plan
12. Go to created storage account then go to access keys and copy one of the access keys
13. Then go to azure key vault and create a secret: paste value, select name as : storageaccountaccesskey, leave other options as default
14. Then still in azure key vault service go to "Access Policies" section, click create, in secret permissions select Get, List and as principal select databricks
15. Then copy your databricks instance url and paste this into your browser: "https://<databricks-instance>#secrets/createScope", create given scope: "secret-keys" and copy Vault URI and Resource ID from properties in key vault and paste it into databricks interface.
16. In databricks catalog create given catalog: "databricks_dev_domain" and within this catalog create 3 schemas: "ecommerce_bronze", "ecommerce_silver", "ecommerce_gold".
17. Execute data_generation script that is located here: "ecommerce\data_generation\main.py" and paste files into appropriate virtual folders based on configuration in bronze table properties in created storage account in this container: "rawdatastorage".

18. Install databricks cli
18.2. Go to dev.yml file and update workspace id, copy workspace id from the deployed databrick workspace
19. databricks auth login --host <databricks_url>
20. databricks bundle deploy -t dev

The pipeline should be deployed then and you should be able to run the entire pipeline.