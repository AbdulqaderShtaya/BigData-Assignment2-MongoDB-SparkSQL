
from pymongo import MongoClient, DESCENDING

client = MongoClient("mongodb://localhost:27017")
collection = client["university"]["students"]

# Get a valid GPA from the user
while True:
    try:
        min_gpa = float(input("Enter minimum GPA (0.0 - 4.0): "))

        if 0.0 <= min_gpa <= 4.0:
            break
        else:
            print("GPA must be between 0.0 and 4.0.")

    except ValueError:
        print("Invalid input! Please enter a number.")

# Find students with GPA greater than the given value
query = {"gpa": {"$gt": min_gpa}}

students = list(
    collection.find(
        query,
        {"_id": 1, "firstName": 1, "lastName": 1, "gpa": 1}
    ).sort("gpa", DESCENDING)
)

print(f"\nStudents with GPA > {min_gpa}:")

for student in students:
    full_name = student["firstName"] + " " + student["lastName"]

    print(
        f"ID: {student['_id']} | "
        f"Name: {full_name} | "
        f"GPA: {student['gpa']}"
    )

print("\nTotal matches:", len(students))

client.close()
