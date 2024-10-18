import pandas as pd
from faker import Faker
import json

fake = Faker()

"""Creating a empty list so the generated records can be stored in there 
using a for loop. Also adding a timestamp in Event_Date_Records so it is
suitable for Kafka"""

data = []

for i in range(10000):
    record = {
        'PERSONAL_NUMBER_ID': i + 1,
        'FULL_NAME': fake.name(),
        'BIRTH_DATE': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
        'SSN': fake.ssn(),
        'EVENT_DATE_RECORD': fake.iso8601(),
        'PERSONAL_ADDRESS_STREET': fake.street_address(),
        'PERSONAL_ADDRESS_CITY': fake.city(),
        'PERSONAL_ADDRESS_STATE': fake.state(),
        'PERSONAL_ADDRESS_COUNTRY': fake.country(),
        'PERSONAL_ADDRESS_ZIP_CODE': fake.zipcode()
    }

    data.append(record)

df = pd.DataFrame(data)

"""Making sure that the data is suitable for Kafka """

# Verify serialization
sample_record = df.iloc[0].to_dict()
json_record = json.dumps(sample_record)

print("Sample Kafka Message (in JSON format):")
print(json_record)

# Making sure message is under 1MB
message_size = len(json_record.encode('utf-8'))
print(f"\nSize of the message in bytes: {message_size} bytes")

"""Exporting data to a JSON file"""

df.to_json('personal_data.json', orient='records', lines=True)