# Big Data Homework 2 — Student Records with MongoDB & Spark SQL

**Scala-only implementation** of a two-stage data pipeline for 100 student records.

- **Parts 1–3 — Scala + MongoDB Java Driver:** Parse the CSV, insert typed BSON documents into MongoDB, build indexes, and execute interactive course and GPA queries.
- **Part 4 — Scala + Apache Spark SQL:** Load the JSON Lines output, run analytic SQL (arrays, aggregations, window functions and self-joins), and save/read Parquet.

> **[View DEMO.md — verified outputs, SQL queries, and explanation](DEMO.md)**

## Data flow

```text
hw2-data.csv
      |
      v
MongoStudentApp.scala  (Parts 1, 2 and 3)
      |----> MongoDB: university.students
      |      |-- courseIds multikey index
      |      |-- gpa index
      |      `-- interactive MongoDB queries / explain()
      |
      `----> students.json (JSON Lines)
                    |
                    v
StudentAnalysis.scala (Part 4: Spark SQL)
                    |
                    v
         students / enrollments views
                    |
                    v
        output/course_summary_parquet/
                    |
                    v
           Spark Parquet read-back
```

## Project files

```text
BigData_Assignment2/
├── build.sbt
├── project/build.properties
├── src/main/scala/
│   ├── MongoStudentApp.scala       # MongoDB import, indexes and queries (Parts 1-3)
│   └── StudentAnalysis.scala       # Spark SQL + Parquet (Part 4)
├── hw2-data.csv                   # Original dataset
├── students.json                  # JSON Lines output created by Scala
├── output/course_summary_parquet/ # Spark-generated Parquet file + _SUCCESS
├── README.md
├── DEMO.md
└── .gitignore
```

## Requirements

- Java 21, Scala 2.13.18, sbt
- MongoDB Community Server (tested with 8.0.x), listening on `mongodb://localhost:27017`
- Apache Spark 4.2.0 for local execution
- Dependencies in `build.sbt`: MongoDB synchronous Java driver 5.3.1 and Spark SQL 4.2.0

**No Python scripts or PyMongo are needed.**

## Reproduce the demo

Open the project in IntelliJ IDEA and start its **sbt shell**:

```text
reload
compile
runMain MongoStudentApp demo
```

The `demo` command **drops and recreates** `university.students`, so use only the assignment's local/test database. It then parses `hw2-data.csv`, imports all 100 records, creates indexes, writes `students.json`, and runs the three example MongoDB queries (course 103, GPA > 3.5, and combined). The interactive versions are available individually:

```text
runMain MongoStudentApp course
runMain MongoStudentApp gpa
runMain MongoStudentApp combined
```

The commands ask for input as necessary. For a non-interactive sample:

```text
runMain MongoStudentApp course 103
runMain MongoStudentApp gpa 3.5
runMain MongoStudentApp combined 103 3.5
```

To run Spark, first package the project from the sbt shell:

```text
package
```

Then run from **PowerShell** in the project root (adjust the Spark installation directory as needed):

```powershell
& "C:\spark\spark-4.2.0-bin-hadoop3\bin\spark-submit.cmd" --class StudentAnalysis --master "local[*]" "target\scala-2.13\bigdata_assignment2_2.13-1.0.jar" "students.json"
```

Spark displays the schema and SQL query results and writes `output/course_summary_parquet`; it then reads the Parquet output to verify it.

## Verified local execution (10 November 2026)

| Check | Observed result |
|---|---:|
| MongoDB inserted documents | 100 |
| MongoDB course 103 matches | 36 |
| MongoDB GPA > 3.5 matches | 49 |
| MongoDB combined matches | 9 |
| Course query uses the multikey index | `IXSCAN: true` |
| Spark students loaded | 100 |
| Spark enrollments after `explode` | 218 |
| Students taking 1 / 2 / 3 courses | 8 / 66 / 26 |
| Pairs with identical first and last names | 7 |
| GPA above every enrolled-course average | 50 |
| Course statistics saved and read from Parquet | 5 courses |

The Scala MongoDB results and Spark SQL results agree for the overlapping course and GPA filters. The Spark run completed with `exitCode 0`. Windows printed temporary JAR cleanup warnings during shutdown; these occurred **after** successful completion and did not prevent Parquet output.

## Data typing

MongoDB stores `_id` as an integer, `dateOfBirth` as BSON Date, `courseIds` as an integer array, `gpa` as a double, and names as strings. The JSON Lines export is produced by Scala. Spark converts `dateOfBirth` to a timestamp and `courseIds` to `array<int>` before registering the `students` view.

## Notes on SQL and index validation

See [DEMO.md](DEMO.md) for Spark SQL statements, summarized observed results, an explanation of ties under `DENSE_RANK`, the MongoDB `IXSCAN` evidence, and the Parquet read-back table.
