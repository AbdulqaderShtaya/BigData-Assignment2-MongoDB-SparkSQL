
import ast
import json
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, ASCENDING

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
client.admin.command("ping")

db = client["university"]
students_collection = db["students"]

# Read the CSV file
csv_path = Path(__file__).parent / "hw2-data.csv"
students = []

with open(csv_path, "r", encoding="utf-8-sig") as file:
    next(file)  # Skip the header

    for line in file:
        if not line.strip():
            continue

        # Split the first four columns
        parts = line.strip().split(",", 4)

        student_id = int(parts[0])
        first_name = parts[1].strip()
        last_name = parts[2].strip()

        date_of_birth = datetime.strptime(
            parts[3].strip(), "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc)

        # Separate course IDs from GPA
        courses_text, gpa_text = parts[4].rsplit(",", 1)

        course_ids = [
            int(course_id)
            for course_id in ast.literal_eval(courses_text)
        ]

        gpa = float(gpa_text)

        student = {
            "_id": student_id,
            "firstName": first_name,
            "lastName": last_name,
            "dateOfBirth": date_of_birth,
            "courseIds": course_ids,
            "gpa": gpa
        }

        students.append(student)

# Insert all records into MongoDB
students_collection.drop()

result = students_collection.insert_many(students)

# Create indexes
students_collection.create_index(
    [("courseIds", ASCENDING)]
)

students_collection.create_index(
    [("gpa", ASCENDING)]
)

# Export the records as JSON Lines for Spark
json_path = Path(__file__).parent / "students.json"

with open(json_path, "w", encoding="utf-8") as file:
    for student in students:
        json_student = student.copy()
        json_student["dateOfBirth"] = (
            student["dateOfBirth"].isoformat()
        )

        file.write(json.dumps(json_student) + "\n")

# Print results
print("Inserted documents:", len(result.inserted_ids))

print("\nIndexes:")
for index in students_collection.list_indexes():
    print(index["name"], dict(index["key"]))

print("\nFirst student:")
print(students_collection.find_one({"_id": 1}))

print("\nJSON file created:", json_path)

client.close()
