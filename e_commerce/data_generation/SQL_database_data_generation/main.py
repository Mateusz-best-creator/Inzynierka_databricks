import csv
from datetime import datetime, timedelta
from faker import Faker
import random

# We would like to have 5000 users
#
# so becuase of that we will have:
#
# - 5000 user profiles
# - 5000 user contacts
# - about 15000 user status history
# - about 10000 user location
# - about 1000 distinct locations

users_table = []
user_profile_table = []
user_contact_table = []
user_status_history_table = []
user_location_table = []
location_table = []

fake = Faker("en_US")

for i in range(1000):
    location_table.append({
        "location_id": str(i+1),
        "city": fake.city(),
        "state": fake.state(),
        "zip_code": fake.zipcode(),
        "country": "United States",
    })


for i in range(5000):

    user_id = str(i+1).zfill(10)
    customer_unique_id = hash(user_id)
    created_at = datetime.now()

    users_table.append({"user_id": user_id, "customer_unique_id": customer_unique_id,"created_at": created_at})

    choice = random.choice([0, 1])
    if i > 0 and i % 1000 == 0:
        gender = 'NOT_SPECIFIED'
    else:
        gender = 'MALE' if choice == 0 else 'FEMALE'
    if gender == 'MALE':
        first_name = fake.first_name_male()
        last_name = fake.last_name_male()
    else:
        first_name = fake.first_name_female()
        last_name = fake.last_name_female()

    birth_date = fake.date_of_birth(minimum_age=18, maximum_age=80)

    user_profile_table.append({"user_id": user_id, "first_name": first_name,
                               "last_name": last_name, "birth_date": birth_date, "gender": gender})

    email = fake.email().replace("example", "gmail")
    phone = fake.phone_number()

    user_contact_table.append({"user_id": user_id, "email": email, "phone": phone})

    location_choice = random.randint(0, 10)
    now = datetime.now()

    if location_choice <= 4:
        number_of_versions = 1
    elif location_choice <= 6:
        number_of_versions = 2
    elif location_choice <= 8:
        number_of_versions = 3
    elif location_choice == 9:
        number_of_versions = 4
    else:
        number_of_versions = 5

    location_ids = random.sample(range(len(location_table)), number_of_versions)

    years_ago = random.randint(8, 12)
    initial_valid_from = now - timedelta(365 * years_ago)
    dates = [initial_valid_from]

    for j in range(1, number_of_versions):

        previous_date = dates[-1]
        remaining_days = (now - previous_date).days
        random_days_amount = random.randint(1, remaining_days)
        new_date = previous_date + timedelta(days=random_days_amount)
        dates.append(new_date)

        if new_date + timedelta(days=365) > now:
            break

    for j in range(len(dates)):

        if j == len(dates) - 1:
            is_current = 1
            valid_to = None
        else:
            is_current = 0
            valid_to = dates[j+1]

        current_length = len(user_location_table)
        user_location_table.append({"id": current_length+1, "user_id": user_id, "location_id": location_ids[j],
                                    "valid_from": dates[j], "valid_to": valid_to, "is_current": is_current})

def write_csv_file(destination: str, data):
    with open(destination, 'w', newline='', encoding='utf-8') as file:
        fields_names = data[0].keys()
        writer = csv.DictWriter(file, fieldnames=fields_names)

        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    write_csv_file("generated_csv_data/users_table.csv", users_table)
    write_csv_file("generated_csv_data/user_profile_table.csv", user_profile_table)
    write_csv_file("generated_csv_data/user_contact_table.csv", user_contact_table)
    write_csv_file("generated_csv_data/user_location_table.csv", user_location_table)
    write_csv_file("generated_csv_data/location_table.csv", location_table)

    # write_csv_file("generated_csv_data/user_status_history_table.csv", user_status_history_table)
