import json

data = {
  "employees": [
    {
      "id": 1,
      "name": "John Doe",
      "position": "Software Engineer",
      "salary": 75000,
      "department": "Engineering"
    },
    {
      "id": 2,
      "name": "Jane Smith",
      "position": "Product Manager",
      "salary": 90000,
      "department": "Product"
    },
    {
      "id": 3,
      "name": "Sam Johnson",
      "position": "Data Analyst",
      "salary": 65000,
      "department": "Data"
    },
    {
      "id": 4,
      "name": "Emily Davis",
      "position": "UX Designer",
      "salary": 70000,
      "department": "Design"
    }
  ],
  "departments": [
    {
      "id": 1,
      "name": "Engineering",
      "head": "Alice Brown"
    },
    {
      "id": 2,
      "name": "Product",
      "head": "Bob Clark"
    },
    {
      "id": 3,
      "name": "Data",
      "head": "Charlie Martin"
    },
    {
      "id": 4,
      "name": "Design",
      "head": "Diana Lee"
    }
  ]
}

with open('sample_data.json', 'w') as json_file:
    json.dump(data, json_file, indent=4)

