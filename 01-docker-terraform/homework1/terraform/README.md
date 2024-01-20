# Terraform Homework: Google Cloud Resources

## Goal

The goal of this Terraform homework is to create Google Cloud Storage resources using Terraform. Additionally, there is an advanced goal which involves creating both Google Cloud Storage and BigQuery resources using Terraform, utilizing variables for better configurability.

## Prerequisites

Before starting with the homework, ensure the following prerequisites are met:

1. **Google Cloud Account:**
    - You need to have access to a Google Cloud account.

2. **Service Account:**
    - Create a service account with the following roles:
        - Storage Admin
        - BigQuery Admin

3. **JSON Key:**
    - Download the JSON key for the service account.

4. **Create 'keys' Folder:**
    - Create a folder named 'keys' in the Terraform directory.

5. **Place JSON Key:**
    - Put the downloaded service account JSON key file inside the 'keys' folder.

6. **Set Google Application Credentials:**
    - Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your service account JSON key file:
        ```bash
        export GOOGLE_APPLICATION_CREDENTIALS=path_to_repo_on_your_macbook/datacamp2024/01-docker-terraform/homework1/terraform/keys/your-key-file-name.json
        ```

## Basic Goal

To achieve the first goal (creating Google Cloud Storage), follow these steps:

1. **Navigate to Terraform Basic Directory:**
    ```bash
    cd terraform_basic
    ```

2. **Initialize Terraform:**
    ```bash
    terraform init
    ```

3. **Plan Terraform Changes:**
    ```bash
    terraform plan
    ```

4. **Apply Terraform Changes:**
    ```bash
    terraform apply
    ```

5. **Destroy Resources:**
    ```bash
    terraform destroy
    ```

## Advanced Goal (With Variables)

To achieve the second goal (creating Google Cloud Storage and BigQuery with variables), follow these steps:

1. **Navigate to Terraform with Variables Directory:**
    ```bash
    cd terraform_with_variables
    ```

2. **Initialize Terraform:**
    ```bash
    terraform init
    ```

3. **Plan Terraform Changes:**
    ```bash
    terraform plan
    ```

4. **Apply Terraform Changes:**
    ```bash
    terraform apply
    ```

5. **Destroy Resources:**
    ```bash
    terraform destroy
    ```

By following these steps, you can create and manage Google Cloud resources using Terraform for both basic and advanced goals.
