# Execution Demo — Homework 2 (Scala only)

This document presents **observed outputs** from local execution on **10 November 2026**. Parts 1–3 ran using Scala and MongoDB; Part 4 ran using Scala and Apache Spark SQL. Counts shown here were observed in program output, not taken from sample answers.

## 1. Dataset import and indexes (Scala + MongoDB)

**Input:** `hw2-data.csv` (100 rows). Its `CourseIDs` column contains commas inside square brackets, so splitting every comma would be incorrect.

**Implementation:** `src/main/scala/MongoStudentApp.scala` safely separates the first four CSV fields and final GPA from the course list, parses typed values, drops/recreates the test collection `university.students`, calls MongoDB `insertMany`, creates the `courseIds` multikey index and `gpa` index, and exports `students.json` for Spark.

Run the complete tested sample in IntelliJ **sbt shell**:

```text
runMain MongoStudentApp demo
```

**Actual output (10 November 2026):**

```text
Inserted documents: 100
Indexes:
  _id_: Document{{_id=1}}
  courseIds_1: Document{{courseIds=1}}
  gpa_1: Document{{gpa=1}}
JSON Lines exported to students.json
```

MongoDB document types: integer `_id`, strings for names, BSON Date `dateOfBirth`, integer array `courseIds`, and double `gpa`.

### Interactive queries (Parts 2 and 3)

| Scala command | Sample filter | Actual number of matches |
|---|---|---:|
| `runMain MongoStudentApp course 103` | Course ID `103` | **36** |
| `runMain MongoStudentApp gpa 3.5` | `gpa > 3.5` | **49** |
| `runMain MongoStudentApp combined 103 3.5` | Course `103` AND `gpa > 3.5` | **9** |

Each query is implemented with the MongoDB driver in **Scala**. User input is validated for integer course ID and GPA within 0.0–4.0. The course query sorts by surname; the GPA query sorts by GPA descending; the combined query supports optional filters.

**Index verification:** the real `explain()` output showed `winningPlan.stage = FETCH`, `winningPlan.inputStage.stage = IXSCAN`, `indexName = courseIds_1`, and `isMultiKey = true`. The program printed:

```text
courseIds index IXSCAN used: true
```

The MongoDB driver's optional `SLF4J not found` warning means its own logging was disabled; the queries still ran successfully.

## 2. Spark schema and initial validation

Spark uses **Option B** (`spark.read.json(...)`) and processes the JSON Lines file exported by `MongoStudentApp.scala`.

```text
root
 |-- _id: long (nullable = true)
 |-- courseIds: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- dateOfBirth: timestamp (nullable = true)
 |-- firstName: string (nullable = true)
 |-- gpa: double (nullable = true)
 |-- lastName: string (nullable = true)

Total students: 100
```

The input DataFrame is registered as the temporary SQL view `students`.

## 3. Part 4.1 — Reproduce MongoDB queries

### 4.1.1 Students in course 103

```sql
SELECT _id, firstName, lastName, gpa
FROM students
WHERE array_contains(courseIds, 103)
ORDER BY lastName ASC, _id ASC;
```

**Actual output summary:**

```text
===== STUDENTS IN COURSE 103 =====
Total students in course 103: 36
```

**Comparison:** MongoDB = **36**; Spark SQL = **36**. Results match.

### 4.1.2 Students with GPA > 3.5

```sql
SELECT _id, firstName, lastName, gpa
FROM students
WHERE gpa > 3.5
ORDER BY gpa DESC, _id ASC;
```

**Actual output summary:**

```text
===== STUDENTS WITH GPA > 3.5 =====
Total students with GPA > 3.5: 49
```

**Comparison:** MongoDB = **49**; Spark SQL = **49**. Results match. The rows were shown in GPA-descending order.

## 4. Part 4.2 — Arrays and aggregations

### 4.2.1 Expand the course array

```sql
SELECT
  _id AS studentId,
  firstName,
  lastName,
  dateOfBirth,
  gpa,
  explode(courseIds) AS course
FROM students;
```

Registered the result as the temporary view `enrollments`.

**Observed sample:**

```text
studentId  firstName  lastName  gpa  course
1          Alice      Johnson   3.8  101
1          Alice      Johnson   3.8  102
1          Alice      Johnson   3.8  103
2          Bob        Smith     3.5  102
2          Bob        Smith     3.5  104
2          Bob        Smith     3.5  105

Total enrollment rows: 218
```

### 4.2.2 Course statistics

```sql
SELECT
  course,
  COUNT(DISTINCT studentId) AS studentCount,
  ROUND(AVG(gpa), 2) AS averageGpa,
  MIN(gpa) AS minGpa,
  MAX(gpa) AS maxGpa
FROM enrollments
GROUP BY course
ORDER BY studentCount DESC, course ASC;
```

**Actual output:**

| Course | Enrolled students | Average GPA | Min GPA | Max GPA |
|---|---:|---:|---:|---:|
| 101 | 48 | 3.56 | 3.1 | 3.9 |
| 105 | 47 | 3.50 | 3.1 | 3.9 |
| 104 | 44 | 3.59 | 3.1 | 3.9 |
| 102 | 43 | 3.50 | 3.1 | 3.9 |
| 103 | 36 | 3.39 | 3.1 | 3.9 |

**Cross-check:** 48 + 47 + 44 + 43 + 36 = **218** enrollment rows.

### 4.2.3 Number of courses per student

```sql
SELECT
  size(courseIds) AS numberOfCourses,
  COUNT(*) AS studentCount
FROM students
WHERE size(courseIds) IN (1, 2, 3)
GROUP BY size(courseIds)
ORDER BY numberOfCourses ASC;
```

**Actual output:**

| Number of courses | Student count |
|---:|---:|
| 1 | 8 |
| 2 | 66 |
| 3 | 26 |

**Cross-check:** 8 + 66 + 26 = **100** students; 8 + 2×66 + 3×26 = **218** enrollments.

## 5. Part 4.3 — Aggregation and advanced SQL

### 4.3.1 Average GPA by birth year

```sql
SELECT
  YEAR(dateOfBirth) AS birthYear,
  COUNT(*) AS studentCount,
  ROUND(AVG(gpa), 2) AS averageGpa
FROM students
GROUP BY YEAR(dateOfBirth)
ORDER BY birthYear ASC;
```

**Actual output:**

| Birth year | Students | Average GPA |
|---|---:|---:|
| 1997 | 47 | 3.54 |
| 1998 | 29 | 3.50 |
| 1999 | 24 | 3.45 |

### 4.3.2 Top three GPA ranks per course (ties included)

```sql
WITH ranked_students AS (
  SELECT
    course, studentId, firstName, lastName, gpa,
    DENSE_RANK() OVER (
      PARTITION BY course ORDER BY gpa DESC
    ) AS gpaRank
  FROM enrollments
)
SELECT course, studentId, firstName, lastName, gpa, gpaRank
FROM ranked_students
WHERE gpaRank <= 3
ORDER BY course ASC, gpaRank ASC, studentId ASC;
```

**Observed GPA at each rank:**

| Course | Rank 1 | Rank 2 | Rank 3 |
|---|---:|---:|---:|
| 101 | 3.9 | 3.8 | 3.7 |
| 102 | 3.9 | 3.8 | 3.7 |
| 103 | 3.9 | 3.8 | 3.6 |
| 104 | 3.9 | 3.8 | 3.7 |
| 105 | 3.9 | 3.8 | 3.7 |

**How ties were handled:** `DENSE_RANK` assigns the *same* rank to every student with the same GPA within a course. It does **not** leave gaps in subsequent ranks. Filtering on `gpaRank <= 3` therefore returns every student in the top three **distinct GPA levels**, possibly more than three individual students. This is intentional.

### 4.3.3 Students with the same full name (self-join)

```sql
SELECT
  a._id AS student1Id,
  b._id AS student2Id,
  a.firstName,
  a.lastName,
  a.dateOfBirth AS student1BirthDate,
  b.dateOfBirth AS student2BirthDate
FROM students a
JOIN students b
  ON a.firstName = b.firstName
 AND a.lastName = b.lastName
 AND a._id < b._id
ORDER BY a.firstName, a.lastName;
```

**Actual output (seven distinct pairs):**

| Student 1 | Student 2 | Full name | Date of birth 1 | Date of birth 2 |
|---:|---:|---|---|---|
| 53 | 79 | Ava Gonzalez | 1997-10-18 | 1997-03-28 |
| 28 | 80 | Benjamin Ward | 1999-07-01 | 1999-08-16 |
| 56 | 82 | Daniel Martin | 1999-05-26 | 1998-01-22 |
| 12 | 90 | Liam Hall | 1997-12-27 | 1998-08-22 |
| 16 | 42 | Peter Williams | 1998-01-14 | 1998-11-30 |
| 18 | 70 | Rachel Young | 1998-09-19 | 1999-09-20 |
| 71 | 97 | Samuel Moore | 1997-01-29 | 1997-04-09 |

The condition `a._id < b._id` excludes matching a record to itself and avoids duplicate reversed pairs. Dates above are displayed as calendar dates for readability; Spark's timestamp output in the local timezone showed time offsets of 02:00 or 03:00.

### 4.3.4 GPA above every enrolled-course average

```sql
WITH course_averages AS (
  SELECT course, AVG(gpa) AS averageGpa
  FROM enrollments
  GROUP BY course
)
SELECT
  e.studentId, e.firstName, e.lastName, e.gpa,
  COUNT(*) AS enrolledCourses
FROM enrollments e
JOIN course_averages c
  ON e.course = c.course
GROUP BY e.studentId, e.firstName, e.lastName, e.gpa
HAVING MIN(
  CASE WHEN e.gpa > c.averageGpa THEN 1 ELSE 0 END
) = 1
ORDER BY e.gpa DESC, e.studentId ASC;
```

**Observed output summary:** **50** students met the condition. The highest qualifying GPA was **3.9**. A student with GPA **3.5** also qualified where all of their individual course averages were below that GPA.

The `HAVING MIN(CASE...) = 1` condition requires **all** joined course comparisons to succeed for each student, not just one of them.

## 6. Part 4.4 — Write and read Parquet

Using the assignment's JSON export option, the per-course summary was saved as Parquet and then reopened:

```scala
val parquetPath = "output/course_summary_parquet"
courseStats.write.mode("overwrite").parquet(parquetPath)
val parquetDf = spark.read.parquet(parquetPath)
parquetDf.show(100, truncate = false)
println("Total courses from Parquet: " + parquetDf.count())
```

**Actual output:**

```text
===== SAVING COURSE SUMMARY AS PARQUET =====
Parquet saved successfully!
===== READING PARQUET FILE =====
+------+------------+----------+------+------+
|course|studentCount|averageGpa|minGpa|maxGpa|
+------+------------+----------+------+------+
|101   |48          |3.56      |3.1   |3.9   |
|105   |47          |3.5       |3.1   |3.9   |
|104   |44          |3.59      |3.1   |3.9   |
|102   |43          |3.5       |3.1   |3.9   |
|103   |36          |3.39      |3.1   |3.9   |
+------+------------+----------+------+------+
Total courses from Parquet: 5
```

The output directory contains a `part-*.snappy.parquet` data file and a `_SUCCESS` marker. The final Spark run completed with **`exitCode 0`**. On Windows, shutdown cleanup warnings about deleting a temporary `.jar` were observed *after* the results were printed and did not stop the Parquet write/read verification.

## 7. Comparison — MongoDB vs Spark SQL

| Dimension | MongoDB stage | Spark SQL stage |
|---|---|---|
| Primary purpose | Interactive record retrieval | Analytical queries and summaries |
| Data access | Indexed queries (`IXSCAN` shown for course filter) | DataFrame and SQL execution plans |
| Arrays | Match a course ID in `courseIds` | `array_contains` and `explode` |
| Aggregation | Not the primary focus of the interactive scripts | `GROUP BY`, `AVG`, `MIN`, `MAX` |
| Complex analytics | Interactive Scala/MongoDB queries | Window functions, self-joins, CTEs |
| Output | Query results and Scala-written JSON Lines export | Query tables and Parquet dataset |

**Conclusion:** MongoDB provided typed document storage and efficient indexed lookups; Spark SQL made it straightforward to compute statistics and rank/compare students across groups using SQL.

---

*MongoDB and Spark results re-verified on 10 November 2026 using Scala only. Run the provided Scala entry points on the same data to reproduce them.*
