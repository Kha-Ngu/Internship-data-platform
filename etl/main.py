# Import Polars, which we use for fast DataFrame processing.
import polars as pl

# Import psycopg2, which allows Python to connect to PostgreSQL.
import psycopg2

# Import os so we can read environment variables.
import os

# Import load_dotenv so we can load variables from a .env file during local development.
from dotenv import load_dotenv


# Load environment variables from a .env file if one exists.
load_dotenv()


# Define the path to our raw CSV file.
CSV_PATH = "../data/raw_jobs.csv"


# Read the raw CSV file into a Polars DataFrame.
df = pl.read_csv(CSV_PATH)


# Print the original data so we can see what we started with.
print("Raw data:")
print(df)


# Clean and transform the data.
cleaned_df = (
    df

    # Remove rows where company is missing.
    .filter(pl.col("company").is_not_null())

    # Remove rows where role is missing.
    .filter(pl.col("role").is_not_null())

    # Create new cleaned columns.
    .with_columns([

        # Remove extra spaces from company names.
        pl.col("company").str.strip_chars().alias("company"),

        # Remove extra spaces from role names.
        pl.col("role").str.strip_chars().alias("role"),

        # Convert salary_per_hour into a number.
        pl.col("salary_per_hour").cast(pl.Float64).alias("salary_per_hour"),

        # Convert posted_date from text into an actual Date type.
        pl.col("posted_date").str.strptime(pl.Date, "%Y-%m-%d").alias("posted_date"),

        # Split skills text into a list.
        pl.col("skills").str.split(";").alias("skills_list")
    ])

    # Add a yearly salary estimate.
    .with_columns([

        # Estimate yearly salary assuming 40 hours/week and 52 weeks/year.
        (pl.col("salary_per_hour") * 40 * 52).alias("estimated_yearly_salary")
    ])
)


# Print the cleaned data so we can confirm our transformation worked.
print("Cleaned data:")
print(cleaned_df)


# Create an analytics table by grouping jobs by company.
company_summary = (
    cleaned_df

    # Group all rows by company.
    .group_by("company")

    # Calculate useful summary statistics.
    .agg([

        # Count how many jobs each company has.
        pl.len().alias("job_count"),

        # Calculate average hourly salary.
        pl.col("salary_per_hour").mean().alias("avg_salary_per_hour"),

        # Calculate max hourly salary.
        pl.col("salary_per_hour").max().alias("max_salary_per_hour")
    ])

    # Sort companies from highest average salary to lowest.
    .sort("avg_salary_per_hour", descending=True)
)


# Print the company-level analytics.
print("Company summary:")
print(company_summary)


# Read the PostgreSQL connection values from environment variables.
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "internship_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


# Connect to the PostgreSQL database.
connection = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)


# Create a cursor, which lets us send SQL commands to the database.
cursor = connection.cursor()


# Create the jobs table if it does not already exist.
cursor.execute("""
CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    company TEXT NOT NULL,
    role TEXT NOT NULL,
    location TEXT,
    salary_per_hour NUMERIC,
    skills TEXT,
    posted_date DATE,
    estimated_yearly_salary NUMERIC
);
""")


# Delete old rows so our demo does not duplicate data every time we run it.
cursor.execute("DELETE FROM jobs;")


# Loop through every row in the cleaned DataFrame.
for row in cleaned_df.iter_rows(named=True):

    # Insert one cleaned job row into PostgreSQL.
    cursor.execute("""
    INSERT INTO jobs (
        company,
        role,
        location,
        salary_per_hour,
        skills,
        posted_date,
        estimated_yearly_salary
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (
        row["company"],
        row["role"],
        row["location"],
        row["salary_per_hour"],
        ";".join(row["skills_list"]),
        row["posted_date"],
        row["estimated_yearly_salary"]
    ))


# Save all database changes.
connection.commit()


# Close the cursor.
cursor.close()


# Close the database connection.
connection.close()


# Print a success message.
print("ETL pipeline completed successfully.")