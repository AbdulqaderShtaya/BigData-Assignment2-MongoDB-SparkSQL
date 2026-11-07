
from pymongo import MongoClient, ASCENDING
from pprint import pprint

client = MongoClient("mongodb://localhost:27017")
collection = client["university"]["students"]

# Get a valid course ID
while True:
    try:
        course_id = int(input("Enter Course ID: "))
        break
    except ValueError:
        print("Invalid input! Please enter an integer.")

# Find students registered in this course
query = {"courseIds": course_id}

students = list(
    collection.find(
        query,
        {"_id": 1, "firstName": 1, "lastName": 1, "gpa": 1}
    ).sort("lastName", ASCENDING)
)

print(f"\nStudents registered in course {course_id}:")

for student in students:
    full_name = student["firstName"] + " " + student["lastName"]

    print(
        f"ID: {student['_id']} | "
        f"Name: {full_name} | "
        f"GPA: {student['gpa']}"
    )

print("\nTotal matches:", len(students))

# Examine MongoDB's query execution plan
plan = collection.find(query).explain()

print("\nQuery execution plan:")
pprint(plan["queryPlanner"]["winningPlan"])

client.close()
