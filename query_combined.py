
import math
from pymongo import MongoClient, ASCENDING, DESCENDING

client = MongoClient("mongodb://localhost:27017")
collection = client["university"]["students"]

# Optional Course ID
while True:
    course_input = input("Enter Course ID (Enter to skip): ").strip()

    if course_input == "":
        course_id = None
        break

    try:
        course_id = int(course_input)
        break
    except ValueError:
        print("Invalid Course ID! Please enter an integer.")

# Optional Minimum GPA
while True:
    gpa_input = input("Enter Minimum GPA (Enter to skip): ").strip()

    if gpa_input == "":
        min_gpa = None
        break

    try:
        min_gpa = float(gpa_input)

        if math.isfinite(min_gpa) and 0.0 <= min_gpa <= 4.0:
            break

        print("GPA must be between 0.0 and 4.0.")
    except ValueError:
        print("Invalid GPA! Please enter a number.")

# Build query dynamically
query = {}

if course_id is not None:
    query["courseIds"] = course_id

if min_gpa is not None:
    query["gpa"] = {"$gt": min_gpa}

if not query:
    print("Please enter at least one search condition.")
else:
    students = list(
        collection.find(query).sort([
            ("gpa", DESCENDING),
            ("lastName", ASCENDING),
            ("_id", ASCENDING)
        ])
    )

    print("\nCombined Query:", query)

    for student in students:
        full_name = student["firstName"] + " " + student["lastName"]

        print(
            f"ID: {student['_id']} | "
            f"Name: {full_name} | "
            f"GPA: {student['gpa']} | "
            f"Courses: {student['courseIds']}"
        )

    print("\nTotal matches:", len(students))

client.close()
