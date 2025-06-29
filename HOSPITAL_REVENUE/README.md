# 🏥 Healthcare Billing Analytics using AWS Glue and PySpark

This project demonstrates how to build a serverless data processing pipeline using **AWS Glue Notebooks** and **PySpark** to analyze healthcare data. The pipeline processes and transforms structured medical data stored in S3 and creates insightful reports such as **monthly revenue trends**, **missed appointments**, **treatment costs**, and more.

---


## 📌 Project Objective

To extract meaningful insights from hospital data including billing, appointments, patients, doctors, and treatments by leveraging the serverless capabilities of AWS Glue and store the analytical outputs in S3 for further use (e.g., visualization, reporting, dashboards).

---

![OList Logical Architecture](https://github.com/vegetariancoder/byte-building/blob/main/HOSPITAL_REVENUE/img/flow.png)

## 📊 Use Cases Covered

| # | Use Case Description | Output Folder |
|---|-----------------------|----------------|
| 1 | **Monthly Revenue Summary** - Total billing amount aggregated per month | `outputs/monthly_revenue/` |
| 2 | **Appointments per Doctor** - Count of appointments handled by each doctor | `outputs/appointments_per_doctor/` |
| 3 | **Missed Appointments** - Identify patients who missed their appointments | `outputs/missed_appointments/` |
| 4 | **Unpaid Bills** - Find patients with pending bills | `outputs/unpaid_bills/` |
| 5 | **Top 5 Expensive Treatments** - Highest cost treatments provided | `outputs/top_expensive_treatments/` |
| 6 | **Patient Age Distribution** - Calculate age from date of birth | `outputs/patient_age_distribution/` |
| 7 | **Insurance Provider Usage** - Group patients by their insurance provider | `outputs/insurance_provider_usage/` |
| 8 | **Treatment Cost per Appointment** - Aggregate treatment cost linked to each appointment | `outputs/treatment_cost_per_appointment/` |
| 9 | **Doctor Experience vs Billing** - Correlation between doctor experience and total revenue | `outputs/doctor_experience_vs_billing/` |
|10 | **Daily Appointment Volume** - Track appointment traffic by date | `outputs/daily_appointment_volume/` |

---

## 🧰 Technologies Used

- **AWS Glue Studio Notebook** (PySpark)
- **AWS Glue Catalog**
- **AWS S3** (input & output storage)
- **IAM Roles & Policies**
- **PySpark (DataFrame API)**
- **AWS CloudWatch (optional for logging)**

---

## 🔗 Data Tables Used

| Table | Description |
|-------|-------------|
| `appointments` | Details of patient-doctor appointments |
| `billing` | Billing info including payment status and methods |
| `doctors` | Doctor's specialization, experience, and contact |
| `patients` | Demographic and insurance information |
| `treatments` | Medical treatments linked to appointments |

---

## 🧑‍💻 How to Run

1. **Create a Glue Crawler** to catalog your tables in S3.
2. **Create a Glue Notebook** and select the IAM role with S3 + Glue access.
3. Use the provided PySpark scripts (e.g., `monthly_revenue`) to analyze data.
4. Results will be written back to your S3 bucket in `parquet` format.

---

## 🔐 IAM Role Setup

Ensure your Glue Notebook IAM role includes:
- `AWSGlueServiceRole`
- `AmazonS3FullAccess` (or bucket-scoped access)
- `logs:*` permissions (optional for debugging)
- Trust relationship with Glue:
```json
{
  "Effect": "Allow",
  "Principal": { "Service": "glue.amazonaws.com" },
  "Action": "sts:AssumeRole"
}
