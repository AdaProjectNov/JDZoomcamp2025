# Data Ingestion and Terraform Setup

This repository contains Python scripts for data ingestion and Terraform configurations to set up the necessary infrastructure. It is organized as follows:

## Table of Contents

- [Data Ingestion Scripts](#data-ingestion-scripts)
  - [Green Trip Data Ingestion](#green-trip-data-ingestion)
  - [Zone Lookup Data Ingestion](#zone-lookup-data-ingestion)
- [Terraform Configuration Files](#terraform-configuration-files)
- [Security Considerations](#security-considerations)
- [Evidence Screenshots](#evidence-screenshots)

---

## Data Ingestion Scripts

### Green Trip Data Ingestion

This Python script is responsible for ingesting and processing **Green Trip** data. It extracts, transforms, and loads data into the appropriate format for downstream processing.

### Zone Lookup Data Ingestion

This Python script handles the ingestion of **Zone Lookup** data, which is critical for the lookup of geographical zones related to the Green Trip data.

---

## Terraform Configuration Files

The repository includes the following Terraform configuration files:

- **main.tf**: The main Terraform configuration file defining the infrastructure resources to be created.
- **variables.tf**: Contains the variable definitions used within the Terraform configuration.

> **Important Note**: Sensitive files have been excluded from the repository for security reasons:
> - **Credentials JSON files** have been **removed**.
> - **Terraform state files** (`terraform.tfstate` and `terraform.tfstate.backup`) are **excluded** from the repository to prevent sharing of sensitive state information.

---

## Security Considerations

To ensure that no sensitive information is exposed:
- All **credentials** (including JSON files) have been deliberately removed.
- Both **terraform.tfstate** and **terraform.tfstate.backup** files have been omitted from the repository.

---

## Evidence Screenshots

Several **screenshots** have been provided as evidence of the setup and data processing stages. These screenshots capture the results and progress made throughout the implementation.

---

## Conclusion

This repository demonstrates the integration of data ingestion scripts for Green Trip and Zone Lookup data, as well as the setup of Terraform infrastructure. All sensitive data has been removed or excluded for security purposes.

Feel free to explore the scripts and configurations, and refer to the screenshots for additional context on the work done.
