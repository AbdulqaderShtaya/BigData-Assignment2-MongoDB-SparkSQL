# Big Data Homework 2 — Student Records with MongoDB & Spark SQL

A reproducible two-stage data pipeline for analysing **100 student records**.

- **Stage 1 (MongoDB):** parse a CSV containing array-valued fields, insert typed BSON documents, build indexes, and run interactive course/GPA queries.
- **Stage 2 (Apache Spark SQL):** analyse a JSON Lines export using Spark SQL, including arrays, aggregation, window functions, joins, and Parquet output.

> **[View the complete demo: SQL queries, actual outputs, and findings](DEMO.md)**

## Pipeline

```text
hw2-data.csv
     |
     v
import_students.py
     |----> MongoDB: university.students
     |         |-- _id key
     |         |-- courseIds multikey index
     |         |-- gpa index
     |         `-- interactive Python queries
     |
     `----> students.json (JSON Lines)
                   |
                   v
       Apache Spark: StudentAnalysis.scala
                   |
                   v
       SQL temp views: students, enrollments
                   |
                   v
        output/course_summary_parquet/
                   |
                   v
        Spark reads Parquet to verify output
```

## Verified results

| Check | Observed result |
|---|---:|
| MongoDB records imported | 100 |
| Students enrolled in course `103` | 36 |
| Students with `GPA > 3.5` | 49 |
| Course `103` AND `GPA > 3.5` | 9 |
| Student–course enrollment rows after `explode` | 218 |
| Distinct courses in the per-course summary | 5 |
| Students in exactly 1 / 2 / 3 courses | 8 / 66 / 26 |
| Pairs of students sharing the same full name | 7 |
| Students with GPA above the average of **every** enrolled course | 50 |
| Rows read back from saved Parquet | 5 |

The first two Spark filters reproduced the respective MongoDB result counts (**36** and **49**).

## Project structure

```text
BigData_Assignment2/
├── build.sbt
├── project/                         # sbt project configuration
├── src/main/scala/
│   └── StudentAnalysis.scala        # Spark SQL queries + Parquet write/read
├── import_students.py               # CSV -> MongoDB + JSON Lines export
├── query_by_course.py               # Interactive course query + explain plan
├── query_by_gpa.py                  # Interactive GPA threshold query
├── query_combined.py                # Course and GPA combined query
├── hw2-data.csv                     # Original dataset
├── students.json                    # JSON Lines export consumed by Spark
├── output/course_summary_parquet/   # Spark's written Parquet output
├── README.md
├── DEMO.md
├── requirements.txt
└── .gitignore
```

## Technologies

- Python 3, `pymongo`
- MongoDB Community Server (tested with 8.0)
- Apache Spark **4.2.0**, Spark SQL
- Scala **2.13.18** and Java **21**
- sbt (build configuration in `build.sbt`)
- Parquet with Snappy compression

## How to run

### 1. MongoDB stage

Start your local MongoDB service, then install the Python dependency:

```powershell
python -m pip install -r requirements.txt
```

Import and export the data:

```powershell
python import_students.py
```

This uses `mongodb://localhost:27017` and database `university`, collection `students` in the tested local setup. The import script is designed to be safe to rerun (it recreates the collection).

Run the interactive query programs:

```powershell
python query_by_course.py
python query_by_gpa.py
python query_combined.py
```

Suggested test inputs: course **103**, GPA threshold **3.5**, or both together. For the course query, the MongoDB explanation showed **IXSCAN** for the `courseIds` index.

### 2. Spark SQL stage

In **IntelliJ IDEA → sbt shell**, run:

```text
compile
package
```

Then from a PowerShell terminal **in this project directory**, execute Spark Submit (adjust the Spark install location if different):

```powershell
& "C:\spark\spark-4.2.0-bin-hadoop3\bin\spark-submit.cmd" --class StudentAnalysis --master "local[*]" "target\scala-2.13\bigdata_assignment2_2.13-1.0.jar" "students.json"
```

The program prints schema, filter results, analytics, and verifies the Parquet round trip. The Parquet path is `output/course_summary_parquet` relative to the working directory.

> **Windows note:** a shutdown warning about failing to delete a temporary Spark JAR was observed after the application had already completed with `exitCode 0`. It did **not** affect the query results or Parquet output.

## Data model

Each MongoDB document includes:

- `_id` — integer
- `firstName`, `lastName` — strings
- `dateOfBirth` — BSON date
- `courseIds` — array of integers
- `gpa` — double

The JSON Lines export is read by Spark, where `dateOfBirth` is converted to timestamp and `courseIds` to an array of integers before registering the `students` view.

## What the two stages demonstrate

**MongoDB** supports indexed, interactive lookups on a document field or array element, using `IXSCAN` on an appropriate index. **Spark SQL** supports analytical scans, `explode`, grouping, ranking within partitions, self-joins, and persistent Parquet output.

## Assignment coverage

- Part 1 — import, types, indexes, JSON export
- Part 2 — course filter, validated input, sorted output, index explanation
- Part 3 — GPA threshold and combined course/GPA filter
- Part 4.1 — two MongoDB filters reproduced with Spark SQL
- Part 4.2 — `explode`, course statistics, enrollment-count distribution
- Part 4.3 — birth-year averages, `DENSE_RANK`, duplicate full names, comparisons with all course averages
- Part 4.4 — Parquet write and read-back verification

See **[DEMO.md](DEMO.md)** for the SQL statements and outputs.
