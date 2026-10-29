# HCL Technical Assessment 2 — Solutions and Implementation Guide

**Candidate:** CVV ANAND  
**Assessment:** HCL Assessment  
**Focus areas:** Advanced SQL, Linux, PySpark Data Engineering, AWS Cloud Services, AWS Step Functions, and Microsoft Azure

## 📄 Assessment PDF

[View HCL Assessment PDF](./HCL%20Assessment.pdf)

---

## Table of Contents

- [Overview](#overview)
- [Assessment Sections](#assessment-sections)
- [Prerequisites](#prerequisites)
- [Suggested Repository Structure](#suggested-repository-structure)
- [Section A — Advanced SQL](#section-a--advanced-sql)
- [Section B — Linux](#section-b--linux)
- [Section C — PySpark Data Engineering](#section-c--pyspark-data-engineering)
- [Section D — AWS Cloud Services](#section-d--aws-cloud-services)
- [Section E — AWS Step Functions](#section-e--aws-step-functions)
- [Section F — Microsoft Azure](#section-f--microsoft-azure)
- [Execution and Validation Checklist](#execution-and-validation-checklist)
- [Security Best Practices](#security-best-practices)
- [Troubleshooting](#troubleshooting)
- [Learning Outcomes](#learning-outcomes)

---

## Overview

This repository documents solutions and implementation examples for the HCL technical assessment. It demonstrates relational database design, query optimization, Linux log analysis and permissions, Bash automation, Spark DataFrame transformations, cloud storage security, event-driven processing, workflow orchestration, and Azure data integration.

The examples are intended for learning, assessment preparation, and demonstration. Adapt file paths, database dialects, cloud resource names, regions, and IAM permissions to your environment before execution.

> **Important:** Do not commit passwords, access keys, account IDs, private data, or production resource identifiers to a public repository. Use placeholders in documentation and configure credentials through the relevant cloud CLI or secret-management mechanism.

## Assessment Sections

| Section | Area | Main topics |
|---|---|---|
| A | Advanced SQL | Database design, joins, subqueries, window functions, indexes, execution plans |
| B | Linux | Log analysis, file permissions, process and resource monitoring, Bash backups |
| C | PySpark Data Engineering | DataFrames, schemas, cleaning, aggregation, joins, performance |
| D | AWS Cloud Services | S3, IAM, EC2, CloudWatch, Lambda, event-driven processing |
| E | AWS Step Functions | State machines, task sequencing, retry, catch, execution history |
| F | Microsoft Azure | ADLS Gen2, Entra ID, RBAC, VM networking, Data Factory |

## Prerequisites

Install or configure the tools needed for the section you want to run.

- **SQL:** A SQL database engine such as MySQL, PostgreSQL, or SQL Server. Check dialect differences before running the examples.
- **Linux:** A Linux shell or a Linux virtual machine with permission to read the target logs and run administrative commands.
- **Python/PySpark:** Python 3 and a compatible Java runtime for the installed Spark version; install `pyspark`.
- **AWS:** An AWS account, AWS Console or CLI, appropriate IAM permissions, and a configured region.
- **Azure:** An Azure subscription, Azure Portal or CLI, and appropriate resource permissions.

Example Python environment:

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
# .venv\Scripts\Activate.ps1

pip install pyspark
```

Verify the installed tools:

```bash
python --version
java -version
aws --version
az version
```

Only run commands relevant to the section you are using. Cloud CLI commands may require installation and authentication first.

## Suggested Repository Structure

```text
hcl-technical-assessment/
├── README.md
├── sql/
│   ├── enterprise_db.sql
│   └── sales_queries.sql
├── linux/
│   ├── log_analysis.sh
│   └── backup_check.sh
├── pyspark/
│   ├── dataframe_operations.py
│   ├── data_cleaning_aggregation.py
│   └── joins_performance.py
├── data/
│   ├── transactions_sample.csv
│   ├── customers_sample.csv
│   └── orders_sample.csv
├── aws/
│   ├── iam-s3-read-policy.json
│   ├── step-functions-state-machine.json
│   └── cloudwatch-alarm-notes.md
└── azure/
    └── data-factory-pipeline-notes.md
```

This is a suggested layout; create only the files that you actually implement and test.

---

## Section A — Advanced SQL

### Question 1: Database Design and Advanced Queries

Create an `EnterpriseDB` database with department and employee tables. The design should include primary keys, a foreign key from employees to departments, and a self-referencing manager relationship.

Example schema (works with common SQL dialects with minor adjustments):

```sql
CREATE DATABASE EnterpriseDB;
-- Connect to EnterpriseDB before running the following statements.

CREATE TABLE Departments (
    DepartmentID   VARCHAR(10) PRIMARY KEY,
    DepartmentName VARCHAR(100) NOT NULL,
    Location       VARCHAR(100)
);

CREATE TABLE Employees (
    EmployeeID   INT PRIMARY KEY,
    EmployeeName VARCHAR(100) NOT NULL,
    DepartmentID VARCHAR(10) NOT NULL,
    Salary       DECIMAL(12, 2) NOT NULL CHECK (Salary >= 0),
    JoiningDate  DATE,
    ManagerID    INT NULL,
    CONSTRAINT fk_employee_department
        FOREIGN KEY (DepartmentID)
        REFERENCES Departments(DepartmentID),
    CONSTRAINT fk_employee_manager
        FOREIGN KEY (ManagerID)
        REFERENCES Employees(EmployeeID)
);
```

Insert at least five departments and 15 employees, including varied salaries, departments, and managers. Ensure at least one department has no employees. Insert manager rows before employees who reference those managers, or use a database-appropriate deferred/self-reference strategy.

#### A. Display employee, department, and manager names

```sql
SELECT
    e.EmployeeName,
    d.DepartmentName,
    m.EmployeeName AS ManagerName
FROM Employees AS e
JOIN Departments AS d
    ON d.DepartmentID = e.DepartmentID
LEFT JOIN Employees AS m
    ON m.EmployeeID = e.ManagerID;
```

The `LEFT JOIN` to `Employees` allows employees without an assigned manager to appear.

#### B. Find employees earning more than their department's average salary

```sql
SELECT
    e.EmployeeName,
    e.DepartmentID,
    e.Salary
FROM Employees AS e
WHERE e.Salary > (
    SELECT AVG(e2.Salary)
    FROM Employees AS e2
    WHERE e2.DepartmentID = e.DepartmentID
);
```

The correlated subquery calculates the average salary for the employee's own department.

#### C. Find the second-highest distinct salary without `LIMIT`, `TOP`, or `FETCH FIRST`

```sql
SELECT MAX(Salary) AS SecondHighestSalary
FROM Employees
WHERE Salary < (
    SELECT MAX(Salary)
    FROM Employees
);
```

This returns the second-highest **distinct** salary. If the table contains fewer than two distinct salaries, the result is `NULL`.

### Question 2: Sales and Revenue Queries

Create a `Sales` table with sale ID, customer ID, sale date, product, quantity, and unit price.

```sql
CREATE TABLE Sales (
    SaleID      INT PRIMARY KEY,
    CustomerID  INT NOT NULL,
    SaleDate    DATE NOT NULL,
    ProductName VARCHAR(100) NOT NULL,
    Quantity    INT NOT NULL CHECK (Quantity >= 0),
    UnitPrice   DECIMAL(12, 2) NOT NULL CHECK (UnitPrice >= 0)
);
```

Insert at least 15 sample rows across multiple customers, products, and dates.

#### A. Calculate total revenue for each product

```sql
SELECT
    ProductName,
    SUM(Quantity * UnitPrice) AS TotalRevenue
FROM Sales
GROUP BY ProductName
ORDER BY TotalRevenue DESC;
```

#### B. Calculate monthly revenue and month-over-month change

The following example uses PostgreSQL-style date formatting. Adapt the month expression for your database engine.

```sql
WITH MonthlyRevenue AS (
    SELECT
        DATE_TRUNC('month', SaleDate) AS SalesMonth,
        SUM(Quantity * UnitPrice) AS Revenue
    FROM Sales
    GROUP BY DATE_TRUNC('month', SaleDate)
)
SELECT
    SalesMonth,
    Revenue,
    LAG(Revenue) OVER (ORDER BY SalesMonth) AS PreviousMonthRevenue,
    Revenue - LAG(Revenue) OVER (ORDER BY SalesMonth) AS MoMChange
FROM MonthlyRevenue
ORDER BY SalesMonth;
```

`LAG()` retrieves the previous available month in the aggregated result. If a calendar month has no sales, generate a calendar-month series first if you need that month represented as zero.

#### C. Rewrite a date filter to improve index usage

Avoid wrapping an indexed date column in a function:

```sql
-- Less index-friendly in many database engines
SELECT *
FROM Employees
WHERE YEAR(JoiningDate) = 2025;
```

Use a range predicate instead:

```sql
SELECT *
FROM Employees
WHERE JoiningDate >= '2025-01-01'
  AND JoiningDate <  '2026-01-01';
```

The range predicate can allow an index seek/range scan on `JoiningDate`, depending on the database, index, statistics, and query plan.

### Question 3: SQL Troubleshooting

#### A. Execution plans

An execution plan shows how the database intends to run—or has run—a query. Inspect it for full table scans, expensive joins, sorts, large row estimates, and missing or ineffective indexes. Use the engine's plan tool, such as `EXPLAIN` or its graphical equivalent, then validate changes with representative data.

#### B. Composite index for department and salary filters

```sql
CREATE INDEX idx_employees_department_salary
ON Employees (DepartmentID, Salary);
```

This column order is a reasonable starting point for queries that filter by `DepartmentID` and then apply a salary condition. Index design should reflect actual query patterns, selectivity, sorting needs, and write overhead. Confirm its benefit using the execution plan; an index is not automatically faster for every query.

#### C. Find duplicate customer email addresses

Assuming a `Customers` table with an `Email` column:

```sql
SELECT
    Email,
    COUNT(*) AS Occurrences
FROM Customers
WHERE Email IS NOT NULL
GROUP BY Email
HAVING COUNT(*) > 1;
```

If email comparison should be case-insensitive, normalize it according to the database's collation or use an appropriate expression.

---

## Section B — Linux

### Question 4: Log Analysis

Assume logs are stored in `/var/log/application.log` and each line contains a timestamp, severity, service, and message.

```bash
# Find lines containing ERROR
grep 'ERROR' /var/log/application.log

# Display the 20 most recent matching error lines in file order
grep 'ERROR' /var/log/application.log | tail -n 20

# Count matching error lines
grep -c 'ERROR' /var/log/application.log

# Extract errors for a specified date
grep '^2026-10-28 .*ERROR' /var/log/application.log > errors_2026-10-28.log
```

For large files, `grep` streams through the file rather than loading the whole file into memory. The “most recent” command above means the last 20 matching lines in file order; it assumes the log is ordered chronologically.

#### Count errors by hour

For the provided timestamp format (`YYYY-MM-DD HH:MM:SS ERROR ...`):

```bash
grep 'ERROR' /var/log/application.log \
  | awk '{print substr($2,1,2)}' \
  | sort \
  | uniq -c
```

This counts errors by hour-of-day across all dates. To count each date and hour separately:

```bash
grep 'ERROR' /var/log/application.log \
  | awk '{print $1, substr($2,1,2)}' \
  | sort \
  | uniq -c
```

### Question 5: Permissions and System Troubleshooting

```bash
# Create the finance group (requires suitable privileges)
sudo groupadd finance

# Create a report file
sudo touch /tmp/financial_report.txt

# Set the group owner
sudo chown :"finance" /tmp/financial_report.txt

# Owner: read/write; group: read; others: no access
sudo chmod 640 /tmp/financial_report.txt
```

`640` means owner permissions `rw-`, group permissions `r--`, and other permissions `---`. If the file must be owned by a specific application user, set that owner explicitly with `chown`.

Resource diagnostics:

```bash
# Interactive process and CPU view
top

# Processes sorted by CPU usage
ps -eo pid,comm,%cpu,%mem --sort=-%cpu | head

# Memory usage
free -h

# Filesystem disk usage
df -h

# Find files larger than 100 MB under an application directory
find /path/to/application -type f -size +100M -print
```

Replace `/path/to/application` with the actual directory. Access may be restricted by file permissions.

### Question 6: Bash Backup Script

Save the following as `linux/backup_check.sh`.

```bash
#!/usr/bin/env bash
set -u

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <source_directory> <destination_directory>" >&2
    exit 1
fi

SOURCE_DIR=$1
DEST_DIR=$2

if [[ ! -d "$SOURCE_DIR" ]]; then
    echo "Error: source directory does not exist: $SOURCE_DIR" >&2
    exit 1
fi

if ! mkdir -p -- "$DEST_DIR"; then
    echo "Error: could not create destination directory: $DEST_DIR" >&2
    exit 1
fi

TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
ARCHIVE="$DEST_DIR/backup_${TIMESTAMP}.tar.gz"

if tar -czf "$ARCHIVE" -C "$(dirname "$SOURCE_DIR")" "$(basename "$SOURCE_DIR")"; then
    echo "Backup successful: $ARCHIVE"
else
    echo "Error: backup failed." >&2
    rm -f -- "$ARCHIVE"
    exit 1
fi
```

Make the script executable and test valid and invalid inputs:

```bash
chmod +x linux/backup_check.sh

mkdir -p source_data
echo "Sample backup data" > source_data/data.txt

# Valid run
./linux/backup_check.sh source_data backup_folder

# Missing arguments: should print usage and exit with a non-zero status
./linux/backup_check.sh

# Missing source directory: should print an error
./linux/backup_check.sh missing_folder backup_folder
```

The archive is compressed with gzip and contains a timestamp in its name. For production use, also consider backup retention, file ownership, permissions, and verification by listing or restoring an archive.

---

## Section C — PySpark Data Engineering

### Shared PySpark setup

The examples below use the DataFrame API. Run each program in an environment with PySpark and a compatible Java runtime.

```python
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, round, sum as spark_sum,
    to_date, coalesce, try_to_timestamp, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, DateType
)

spark = (
    SparkSession.builder
    .appName("HCLAssessment")
    .master("local[*]")
    .getOrCreate()
)
```

Use the shared imports and session setup as needed. If your Spark version does not support a particular function such as `try_to_timestamp`, use a version-compatible parsing strategy and test invalid-date behavior.

### Question 7: DataFrame Operations

Create a DataFrame with employee ID, name, department, monthly salary, and joining date. Example data:

```python
from datetime import date

employee_data = [
    (101, "Anand", "IT", 75000.0, date(2023, 1, 15)),
    (102, "Rahul", "HR", 45000.0, date(2022, 3, 16)),
    (103, "Priya", "Finance", 65000.0, date(2021, 7, 20)),
    (104, "Srikya", "IT", 55000.0, date(2023, 9, 5)),
    (105, "Kiran", "Sales", 55000.0, date(2023, 6, 12)),
    (106, "Arjun", "IT", 95000.0, date(2019, 3, 18)),
    (107, "Divya", "HR", 60000.0, date(2020, 10, 22)),
    (108, "Ravi", "Finance", 72000.0, date(2021, 4, 14)),
    (109, "Meera", "Sales", 48000.0, date(2023, 5, 30)),
    (110, "Pooja", "HR", 58000.0, date(2022, 12, 2)),
    (111, "Suresh", "Finance", 88000.0, date(2018, 8, 13)),
    (112, "Neha", "IT", 52000.0, date(2024, 1, 10)),
    (113, "Ajay", "Sales", 62000.0, date(2024, 2, 13)),
    (114, "Lakshmi", "Finance", 78000.0, date(2020, 7, 9)),
    (115, "Vijay", "IT", 68000.0, date(2022, 11, 30)),
]

employee_schema = StructType([
    StructField("EmployeeID", IntegerType(), False),
    StructField("EmployeeName", StringType(), False),
    StructField("Department", StringType(), False),
    StructField("Salary", DoubleType(), False),
    StructField("JoiningDate", DateType(), True),
])

df = spark.createDataFrame(employee_data, employee_schema)
```

#### A. Display schema and first five records

```python
df.printSchema()
df.show(5, truncate=False)
```

#### B. Filter monthly salaries greater than ₹60,000

```python
df.filter(col("Salary") > 60000).show(truncate=False)
```

#### C. Add annual salary

```python
df = df.withColumn("AnnualSalary", col("Salary") * 12)
df.show(truncate=False)
```

#### D. Average salary and employee count by department

```python
department_summary = (
    df.groupBy("Department")
      .agg(
          round(avg("Salary"), 2).alias("AverageSalary"),
          count("EmployeeID").alias("EmployeeCount")
      )
      .orderBy("Department")
)

department_summary.show(truncate=False)
```

#### E. Five highest-paid employees

```python
df.orderBy(col("Salary").desc()).limit(5).show(truncate=False)
```

Stop the Spark session after all operations:

```python
spark.stop()
```

### Question 8: Data Cleaning and Aggregation

Assume `transactions.csv` has the columns `TransactionID`, `CustomerID`, `TransactionDate`, `Product`, `Quantity`, and `UnitPrice`.

#### A. Read the CSV with a schema

For robust cleaning, read source values as strings first so malformed values can be inspected before conversion.

```python
from pyspark.sql.types import StructType, StructField, StringType

raw_schema = StructType([
    StructField("TransactionID", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("TransactionDate", StringType(), True),
    StructField("Product", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("UnitPrice", StringType(), True),
])

raw_df = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(raw_schema)
    .csv("data/transactions.csv")
)
```

#### B. Remove duplicates using a business key

If `TransactionID` uniquely identifies a transaction:

```python
df = raw_df.dropDuplicates(["TransactionID"])
```

Choose the deduplication key based on the real business definition. If duplicate IDs can represent different legitimate records, use a more appropriate composite key or a deterministic latest-record rule.

#### C. Handle missing required values and convert numeric fields

```python
from pyspark.sql.functions import trim, when

df = (
    df.withColumn("TransactionID", trim(col("TransactionID")))
      .withColumn("CustomerID", trim(col("CustomerID")))
      .withColumn("Product", trim(col("Product")))
      .withColumn("Quantity", col("Quantity").cast("double"))
      .withColumn("UnitPrice", col("UnitPrice").cast("double"))
)

required_columns = [
    "TransactionID", "CustomerID", "TransactionDate",
    "Product", "Quantity", "UnitPrice"
]

for name in required_columns:
    df = df.withColumn(
        name,
        when(col(name).isNull() | (trim(col(name).cast("string")) == ""), None)
        .otherwise(col(name))
    )

df = df.dropna(subset=required_columns)
```

Dropping rows is appropriate when required fields are essential to a valid transaction and cannot be reliably recovered. For optional fields, consider a documented default or a separate quarantine dataset instead. In a production pipeline, retain rejected records and rejection reasons for auditing.

#### D. Parse dates and handle invalid values

Example formats are ISO (`yyyy-MM-dd`) and day-first (`dd-MM-yyyy`):

```python
iso_date = to_date(col("TransactionDate"), "yyyy-MM-dd")
day_first_date = to_date(col("TransactionDate"), "dd-MM-yyyy")

df = (
    df.withColumn(
        "ParsedDate",
        coalesce(iso_date, day_first_date)
    )
)

# Invalid dates produce null in ParsedDate; inspect or quarantine them.
invalid_dates = df.filter(col("ParsedDate").isNull())
valid_df = df.filter(col("ParsedDate").isNotNull())
```

For strict data-quality handling, write `invalid_dates` to a separate error location before proceeding.

#### E. Calculate revenue

```python
valid_df = valid_df.withColumn(
    "Quantity", col("Quantity").cast("double")
).withColumn(
    "UnitPrice", col("UnitPrice").cast("double")
).withColumn(
    "Revenue", col("Quantity") * col("UnitPrice")
)
```

Rows with failed numeric casts should be validated before calculating revenue.

#### F. Total revenue by product in descending order

```python
product_revenue = (
    valid_df.groupBy("Product")
    .agg(spark_sum("Revenue").alias("TotalRevenue"))
    .orderBy(col("TotalRevenue").desc())
)

product_revenue.show(truncate=False)
```

### Question 9: PySpark Joins and Performance

Assume customer data contains `CustomerID`, `CustomerName`, and `City`; orders data contains `OrderID`, `CustomerID`, `OrderAmount`, and `OrderDate`.

#### A. Load both CSV files

```python
customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/customers_sample.csv")
)

orders = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/orders_sample.csv")
)
```

For production, prefer explicit schemas over `inferSchema` when data types and data quality matter.

#### B. Join orders with customer details

```python
customer_orders = (
    orders.join(customers, on="CustomerID", how="inner")
    .select(
        "OrderID", "CustomerID", "CustomerName",
        "City", "OrderAmount", "OrderDate"
    )
)

customer_orders.show(truncate=False)
```

#### C. Identify customers who have never placed an order

```python
customers_without_orders = customers.join(
    orders.select("CustomerID").distinct(),
    on="CustomerID",
    how="left_anti"
)

customers_without_orders.show(truncate=False)
```

#### D. Total order value by city

```python
city_totals = (
    customer_orders.groupBy("City")
    .agg(spark_sum("OrderAmount").alias("TotalOrderValue"))
    .orderBy(col("TotalOrderValue").desc())
)

city_totals.show(truncate=False)
```

#### E. When to use a broadcast join

Broadcast joins are useful when one DataFrame is small enough to fit comfortably in each executor's memory. Broadcasting can reduce shuffle traffic. Do not broadcast a large dataset; check the Spark plan and memory limits.

```python
broadcast_result = orders.join(
    broadcast(customers),
    on="CustomerID",
    how="inner"
)
```

#### F. Transformation versus action

- **Transformation:** Defines a new DataFrame from an existing one; example: `filter()`. Transformations are generally evaluated lazily.
- **Action:** Triggers computation or returns a result; examples: `show()` and `count()`.

---

## Section D — AWS Cloud Services

> The following steps describe the expected configuration. Actual AWS resource names, ARNs, region, and permissions must be taken from your account. Use least-privilege permissions.

### Question 10: S3 and IAM Security

1. Create an S3 bucket with a globally unique name.
2. Keep **Block Public Access** enabled.
3. Upload a sample report.
4. Use IAM to grant only the required users or roles access.
5. Verify that an unauthorized identity cannot read the object.

Example read-only IAM policy for one designated bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListOnlyDesignatedBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME"
    },
    {
      "Sid": "ReadObjectsOnlyInDesignatedBucket",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::YOUR_BUCKET_NAME/*"
    }
  ]
}
```

Replace `YOUR_BUCKET_NAME`. If users need only to retrieve a known object and do not need to list the bucket, omit `s3:ListBucket`. This identity policy does not override explicit denies, bucket policies, organization policies, or other applicable controls.

**Encryption at rest:** S3 server-side encryption protects stored object data by encrypting it. Configure the required encryption method and key policy for the environment.

**Authorization test:** Sign in or assume a role with no permission to the object and attempt a read. A denied request confirms the tested identity cannot read it under the tested configuration. Do not use a privileged administrator identity for this test.

### Question 11: EC2 Troubleshooting and CloudWatch

#### A. Check instance health

In the EC2 console, inspect instance state, system status checks, and instance status checks. A running instance can still have operating-system or reachability problems.

#### B. Check connectivity

- Security groups control allowed inbound and outbound traffic.
- Network ACLs may also allow or deny traffic at the subnet boundary.
- Route tables determine the path traffic takes.
- Check application listening ports, OS firewall rules, DNS, and the client network.

Avoid opening SSH or RDP to the whole internet as a troubleshooting shortcut.

#### C. Configure a CPU utilization alarm

In CloudWatch, choose the EC2 `CPUUtilization` metric, create an alarm with a threshold of **greater than 85%**, set the period and evaluation datapoints appropriate to the workload, and configure an SNS notification action. The alarm fires when the configured threshold and evaluation conditions are met.

#### D. Investigate high CPU

Review CloudWatch metrics and logs, then inspect the operating system's processes using `top` or `ps` if secure access is available. Correlate spikes with application logs, deployments, and traffic patterns.

### Question 12: Event-Driven Processing

A common design is:

1. An input CSV file is uploaded to Amazon S3.
2. An S3 `ObjectCreated` event notification invokes a Lambda function.
3. The Lambda function validates and processes the file.
4. Processed output is written to a separate S3 bucket or a designated output prefix.
5. CloudWatch Logs captures execution logs; CloudWatch alarms notify operators of failures.

The Lambda execution role should receive only the required permissions, such as:

- `s3:GetObject` on the input object path.
- `s3:PutObject` on the output object path.
- Required CloudWatch Logs permissions.

For reliability, make processing idempotent—for example, track processed object keys and versions—and configure retry/failure handling. Prevent recursive invocations by separating input and output buckets or filtering notifications to a specific input prefix and `.csv` suffix.

---

## Section E — AWS Step Functions

### Question 13: State Machine Design

A typical sales-processing workflow can use these states:

1. **Validate** — checks the input file.
2. **CheckValidation** — routes valid input to processing and invalid input to failure.
3. **Transform** — cleans and transforms the data.
4. **CalculateSales** — aggregates sales by product.
5. **StoreResults** — writes results to S3.
6. **NotifyCompletion** — sends a completion notification.
7. **Fail / Succeed** — records the terminal outcome.

A `Choice` state evaluates a field such as `isValid`. `Retry` handles retryable transient failures. `Catch` routes an exhausted or otherwise handled failure to a failure-handling state. Task states invoke Lambda or supported AWS service integrations using the correct resource ARN and execution role permissions.

Execution history shows state input/output, status, timing, and error details. Use it to identify the failing state and the reason.

### Question 14: Step Functions Implementation

The following Amazon States Language (ASL) JSON illustrates three sequential Lambda tasks. Replace the placeholders with real Lambda ARNs and validate the definition in the Step Functions console before use.

```json
{
  "Comment": "HCL assessment data processing workflow",
  "StartAt": "ValidateFile",
  "States": {
    "ValidateFile": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:REGION:ACCOUNT_ID:function:ValidateFile",
      "Next": "CheckValidation"
    },
    "CheckValidation": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.isValid",
          "BooleanEquals": true,
          "Next": "TransformData"
        }
      ],
      "Default": "ValidationFailed"
    },
    "ValidationFailed": {
      "Type": "Fail",
      "Error": "ValidationError",
      "Cause": "Input file validation failed"
    },
    "TransformData": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:REGION:ACCOUNT_ID:function:TransformData",
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.TooManyRequestsException",
            "States.Timeout"
          ],
          "IntervalSeconds": 3,
          "MaxAttempts": 3,
          "BackoffRate": 2
        }
      ],
      "Next": "LoadResults"
    },
    "LoadResults": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:REGION:ACCOUNT_ID:function:LoadResults",
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.error",
          "Next": "HandleLoadFailure"
        }
      ],
      "Next": "WorkflowSucceeded"
    },
    "HandleLoadFailure": {
      "Type": "Pass",
      "Parameters": {
        "status": "FAILED",
        "message": "Loading failed; inspect the error details.",
        "error.$": "$.error"
      },
      "End": true
    },
    "WorkflowSucceeded": {
      "Type": "Succeed"
    }
  }
}
```

**Important integration note:** The validation Lambda must return a Boolean `isValid` field in the state input at the path checked by the `Choice` state. Ensure the actual Lambda response shape matches the JSON paths. Lambda service integration output may wrap the function result; adjust the paths or use `ResultSelector`, `ResultPath`, or an appropriate integration pattern when necessary.

#### Retry policy

- `ErrorEquals` lists errors that should be retried.
- `IntervalSeconds` sets the initial wait.
- `MaxAttempts` sets the number of retries after the initial attempt.
- `BackoffRate` increases the wait between retries.

The example uses a three-second initial interval, three retries, and a backoff rate of two. Tune these values to the task and service limits.

#### Catch policy

`LoadResults` catches errors and sends execution to `HandleLoadFailure`, where the workflow records a failure result. In production, consider emitting a structured log or notifying an operator, while avoiding sensitive data in error messages.

#### Passing data between tasks

The state output is normally passed to the next state as its input. `InputPath`, `Parameters`, `ResultSelector`, `ResultPath`, and `OutputPath` can filter or reshape this data. Validate the actual response shape from each Lambda function.

#### Testing

- **Success:** Use valid input and confirm validation, transformation, and loading succeed in sequence.
- **Validation failure:** Return `isValid: false` and confirm the state machine enters `ValidationFailed`.
- **Transient transformation failure:** Simulate a retryable error, then confirm retries occur and the task eventually succeeds or fails after the configured attempts.
- **Loading failure:** Simulate an error and confirm the execution reaches `HandleLoadFailure`.
- Inspect execution history and CloudWatch Logs for input/output, retry behavior, and errors.

---

## Section F — Microsoft Azure

### Question 15: Azure Data Lake Storage and Security

#### A. Configure ADLS Gen2

Create an Azure Storage account and enable the **hierarchical namespace** option to support Azure Data Lake Storage Gen2 capabilities.

#### B. Create a filesystem and upload a dataset

Create a filesystem/container in the storage account and upload a sample CSV file to it. Organize data into meaningful directories.

#### C. Microsoft Entra ID and RBAC

Microsoft Entra ID authenticates identities. Azure role-based access control (RBAC) assigns roles to users, groups, or service principals at an appropriate scope.

Examples:

- **Storage Blob Data Reader:** Read access to blob data at the assigned scope.
- **Storage Blob Data Contributor:** Read, write, and delete access to blob data at the assigned scope.

Assign only the role required for the task and scope it to the storage account, container/filesystem, or other appropriate resource where possible. For ADLS Gen2, filesystem ACLs may also affect data access; validate both RBAC and ACL requirements for the chosen access path.

#### D. Verify unauthorized access is denied

Use a dedicated test identity that has no applicable data permissions. Attempt to list or download the dataset. An authorization failure (often HTTP 403) indicates the request was denied in that test context. Confirm that the test identity does not inherit permissions through another group or role.

### Question 16: Azure VM Troubleshooting

#### A. Verify VM state

In the Azure Portal, inspect provisioning state, power state, boot diagnostics, and VM health.

#### B. Review Network Security Group rules

NSG rules evaluate source/destination, ports, protocols, and priority. A deny rule or missing allow rule can block SSH, RDP, or application traffic.

#### C. Check subnet and routing

Verify subnet configuration, effective routes, network interfaces, public/private addressing, DNS, and any VPN or peering path. Incorrect routes can send traffic to the wrong destination or prevent it from reaching the VM.

#### D. Troubleshoot securely

Use **Azure Bastion** where available to connect through the Azure Portal without exposing SSH/RDP directly to the public internet. Use **Network Watcher** and connection troubleshooting tools to investigate routes and NSG behavior. Avoid broad inbound rules such as allowing management ports from `0.0.0.0/0`.

### Question 17: Azure Data Integration

#### A. Orchestration

Use **Azure Data Factory (ADF)** to orchestrate copying and preparing daily customer transaction CSV files.

#### B. Copy CSV to ADLS Gen2

1. Create linked services for the source and ADLS Gen2 destination.
2. Create source and sink datasets.
3. Add a **Copy Data** activity to an ADF pipeline.
4. Configure file paths, schema/mapping, and authentication.
5. Run the pipeline and inspect the destination.

#### C. Organize by ingestion date

A simple folder structure is:

```text
transactions/
└── 2026-10-28/
    └── transactions.csv
```

For dynamic pipelines, generate the date folder from the pipeline's ingestion date/time expression. Use a consistent timezone and naming convention.

#### D. Handle malformed rows and missing values

Use ADF Mapping Data Flows or Azure Databricks to validate and clean records. Fill missing values only when a documented business default is appropriate; otherwise reject or quarantine incomplete rows. Write malformed records to a separate error location for investigation.

#### E. Monitor and validate completeness

Use ADF's **Monitor** section to review pipeline status, duration, and activity failures. Compare source and destination row counts, check required columns, and detect missing or duplicate records. Re-run failed activities after correcting the underlying issue.

---

## Execution and Validation Checklist

Use this checklist to document what you actually ran.

- [ ] SQL database and tables created successfully.
- [ ] At least five departments and 15 employees inserted; one department has no employees.
- [ ] SQL joins, salary subqueries, second-highest salary, sales revenue, and duplicate-email queries tested.
- [ ] Date-range query reviewed with an execution plan.
- [ ] Linux log commands tested against a sample log file.
- [ ] Finance group and `640` permissions verified.
- [ ] Backup script tested with valid arguments, missing arguments, and a missing source directory.
- [ ] PySpark schema, filters, annual salary, department summary, and top-five results checked.
- [ ] Transaction cleaning tested with duplicate IDs, missing fields, invalid dates, and invalid numeric values.
- [ ] Customer/order join, left-anti join, city aggregation, and broadcast join plan reviewed.
- [ ] S3 Block Public Access and least-privilege IAM permissions verified.
- [ ] EC2 status checks, security rules, and CloudWatch alarm configuration reviewed.
- [ ] S3-to-Lambda trigger tested without recursive output triggering.
- [ ] Step Functions success, validation failure, transformation retry, and load failure tested.
- [ ] ADLS Gen2 hierarchical namespace, RBAC/ACLs, and unauthorized access test verified.
- [ ] Azure VM connectivity and secure troubleshooting path reviewed.
- [ ] ADF pipeline run, output location, malformed-record handling, and row counts validated.

## Security Best Practices

- Apply least privilege to IAM policies and Azure RBAC assignments.
- Keep S3 Block Public Access enabled unless a carefully reviewed use case requires otherwise.
- Use encryption at rest and secure transport.
- Do not hard-code access keys, passwords, tokens, or secrets in scripts or notebooks.
- Use separate development/test resources when possible.
- Restrict SSH/RDP and prefer controlled access paths such as Azure Bastion.
- Keep logs useful for debugging without exposing sensitive customer or employee data.
- Make event-driven jobs idempotent and design retries to avoid duplicate side effects.
- Validate input files and quarantine malformed data rather than silently losing records.
- Review cloud costs and delete temporary resources after completing practice exercises.

## Troubleshooting

| Symptom | Possible cause | What to check |
|---|---|---|
| SQL foreign-key insert fails | Referenced department or manager does not exist | Insert parent/manager records first and check IDs |
| SQL date query is slow | Function applied to indexed date column | Use a half-open date range and inspect the plan |
| `Permission denied` in Linux | User/group or mode is incorrect | Check `ls -l`, group membership, parent directory permissions |
| Backup script reports missing source | Incorrect relative path or working directory | Run `pwd` and check the source path |
| PySpark cannot start | Java/Python compatibility or environment issue | Check `java -version`, Python version, and PySpark version |
| CSV fields become null | Schema mismatch or malformed source values | Inspect raw strings, header names, delimiters, and casting |
| S3 returns `AccessDenied` | Missing IAM, bucket policy, KMS, or organization permission | Review the execution identity and all applicable policies |
| S3 returns `NoSuchBucket` or `NoSuchKey` | Incorrect bucket, region, or object key | Verify exact names and path spelling |
| Lambda does not trigger | Event notification, permissions, or suffix/prefix filter is wrong | Check S3 event configuration and Lambda resource permissions |
| Step Functions task fails | Invalid ARN, IAM permission, input/output path, or Lambda error | Inspect execution history and CloudWatch Logs |
| Azure data access fails | RBAC/ACL assignment or scope is incorrect | Check identity, role scope, filesystem ACLs, and propagation |
| Azure VM is unreachable | NSG, route, DNS, IP, or guest OS issue | Use Network Watcher, boot diagnostics, and secure connection methods |
| ADF pipeline fails | Linked service credentials, dataset path, mapping, or source data | Inspect activity output and the Monitor pane |

## Learning Outcomes

By completing the assessment examples, a learner can demonstrate:

- Relational schema design, constraints, joins, correlated subqueries, and window functions.
- Query performance reasoning through execution plans and indexes.
- Linux log inspection, permission management, process monitoring, and compressed backups.
- PySpark DataFrame creation, transformation, cleaning, aggregation, and joins.
- Secure S3 access, IAM policy design, EC2 monitoring, and event-driven Lambda processing.
- Workflow orchestration with Step Functions, including branching, retries, and failure handling.
- Azure data-lake security, VM network troubleshooting, and ADF ingestion pipelines.

---

## Notes

This README is a consolidated guide based on the assessment topics. Code and configuration examples should be tested in the target SQL engine, Spark version, AWS account, or Azure subscription. Cloud screenshots and successful execution evidence should be added only after the corresponding steps have been performed in your own environment.
