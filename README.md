# AWS Data Lakehouse Automation

This project automates the management of data in an AWS Lakehouse. It includes Terraform scripts to set up and manage resources on AWS, along with Glue jobs and Python scripts for data processing and transformation.

## Description

The project orchestrates a series of AWS services, including S3, Glue, IAM, and Step Functions, to facilitate the handling of data within a Lakehouse architecture. The main components are:

- Terraform scripts for setting up AWS resources.
- AWS Glue jobs for data transformation and loading.
- Python scripts for Glue jobs.
- A Step Function state machine for workflow management.

### Terraform Setup

Terraform scripts are provided to manage AWS resources. It includes configuration for:

- S3 buckets for raw and processed data storage.
- IAM roles and policies for secure access.
- Glue jobs and crawlers for data processing.
- Step Functions for orchestrating the data pipeline.

### Data Processing

AWS Glue is used for data processing. This includes:

- Python scripts to transform and load data.
- Glue jobs configuration for different data sources.
- Glue crawlers to update data catalog.

### Workflow Management

An AWS Step Functions state machine is set up to manage the workflow:

- It orchestrates Glue jobs and crawlers.
- Manages dependencies between different tasks.
- Ensures efficient and error-free execution.

## Usage

To use this project:

1. **Set up AWS CLI**: Ensure AWS CLI is configured with appropriate credentials.

2. **Initialize Terraform**:
   ```
   terraform init
   ```

3. **Apply Terraform Plan**
    ```
    terraform apply
    ```

4. **Run Step Function**: Trigger the state machine to start the data processing workflow.

5. **Monitor AWS Glue Jobs**: Check the AWS Glue console for job status and logs.

6. **Validate Data**: Ensure the data is correctly processed and stored in S3 buckets.

