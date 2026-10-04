from datetime import datetime, timedelta
from faker import Faker
import random


NUMBER_OF_USERS = 5000
NUMBER_OF_LOCATIONS = 1000

users_table = []
user_profile_table = []
user_contact_table = []
user_status_history_table = []
user_location_table = []
location_table = []


fake = Faker("en_US")


for i in range(NUMBER_OF_LOCATIONS):
    location_table.append({
        "location_id": str(i + 1),
        "city": fake.city(),
        "state": fake.state(),
        "zip_code": fake.zipcode(),
        "country": "United States",
    })


for i in range(NUMBER_OF_USERS):

    user_id = str(i + 1).zfill(10)
    customer_unique_id = hash(user_id)
    created_at = datetime.now()

    users_table.append({
        "user_id": user_id,
        "customer_unique_id": customer_unique_id,
        "created_at": created_at
    })


    choice = random.choice([0, 1])

    if i > 0 and i % 1000 == 0:
        gender = "NOT_SPECIFIED"
    else:
        gender = "MALE" if choice == 0 else "FEMALE"

    if gender == "MALE":
        first_name = fake.first_name_male()
        last_name = fake.last_name_male()
    elif gender == "FEMALE":
        first_name = fake.first_name_female()
        last_name = fake.last_name_female()
    else:
        first_name = fake.first_name()
        last_name = fake.last_name()

    birth_date = fake.date_of_birth(
        minimum_age=18,
        maximum_age=80
    )

    user_profile_table.append({
        "user_id": user_id,
        "first_name": first_name,
        "last_name": last_name,
        "birth_date": birth_date,
        "gender": gender
    })


    email = fake.email().replace("example", "gmail")
    phone = fake.phone_number()

    user_contact_table.append({
        "user_id": user_id,
        "email": email,
        "phone": phone
    })


    now = datetime.now()

    # 0 -> 1 version
    # 1 -> 2 versions
    # 2 -> 3 versions
    # 3 -> 4 versions
    # 4 -> 5 versions
    location_choice = random.randint(0, 99)

    if location_choice < 30:
        number_of_versions = 1

    elif location_choice < 70:
        number_of_versions = 2

    elif location_choice < 90:
        number_of_versions = 3

    elif location_choice < 95:
        number_of_versions = 4

    else:
        number_of_versions = 5

    start_date = now - timedelta(days=random.randint(
        365,
        10 * 365
    ))

    dates = [start_date]

    for version in range(1, number_of_versions):

        previous_date = dates[-1]
        remaining_days = (now - previous_date).days

        if remaining_days <= 1:
            change_date = now
        else:
            change_date = previous_date + timedelta(
                days=random.randint(1, remaining_days // 2)
            )

        dates.append(change_date)


    location_ids = random.sample(
        range(1, NUMBER_OF_LOCATIONS + 1),
        number_of_versions
    )


    for version in range(number_of_versions):

        valid_from = dates[version]

        if version == number_of_versions - 1:
            # Latest version
            valid_to = None
            is_current = 1

        else:
            # Ends when the next version starts
            valid_to = dates[version + 1]
            is_current = 0


        user_location_table.append({
            "id": str(len(user_location_table) + 1),
            "user_id": user_id,
            "location_id": str(location_ids[version]),
            "valid_from": valid_from,
            "valid_to": valid_to,
            "is_current": is_current
        })


print("Users:", len(users_table))
print("Profiles:", len(user_profile_table))
print("Contacts:", len(user_contact_table))
print("Locations:", len(location_table))
print("User location history:", len(user_location_table))

print("\nExample user location history:")

example_user = "0000000001"

for row in user_location_table:
    if row["user_id"] == example_user:
        print(row)
