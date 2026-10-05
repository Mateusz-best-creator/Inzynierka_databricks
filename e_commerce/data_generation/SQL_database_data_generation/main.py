import csv
import os
import random
from datetime import datetime, timedelta
from faker import Faker


NUMBER_OF_USERS = 5000
NUMBER_OF_LOCATIONS = 1000
OUTPUT_DIRECTORY = "generated_csv_data"

fake = Faker("en_US")


def generate_dates(number_of_versions):
    if number_of_versions <= 0:
        return []

    now = datetime.now()

    years_ago = random.randint(8, 12)
    initial_valid_from = now - timedelta(days=365 * years_ago)

    dates = [initial_valid_from]

    for remaining_versions in range(number_of_versions - 1, 0, -1):
        previous_date = dates[-1]

        remaining_days = (now - previous_date).days
        max_days = remaining_days - (remaining_versions - 1)

        if max_days <= 1:
            new_date = previous_date + timedelta(days=1)
        else:
            new_date = previous_date + timedelta(days=random.randint(1, max_days))

        dates.append(new_date)

    return dates


# For each combination of transitions, let's generate a few of possible descriptions.
STATUS_DESCRIPTIONS = {
    ("ACTIVE", "INACTIVE"): [
        "User voluntarily deactivated the account.",
        "User temporarily disabled the account.",
        "Account was deactivated at the user's request.",
        "User requested temporary account deactivation.",
    ],
    ("INACTIVE", "ACTIVE"): [
        "User reactivated the account.",
        "User returned and reactivated the account.",
        "Account was reactivated at the user's request.",
        "User restored the previously deactivated account.",
    ],
    ("ACTIVE", "SUSPENDED"): [
        "Account temporarily suspended due to suspicious activity.",
        "Account suspended following a policy violation.",
        "Account temporarily restricted pending a security review.",
        "Account suspended due to unusual account activity.",
    ],
    ("SUSPENDED", "ACTIVE"): [
        "Account suspension was lifted after review.",
        "Security review completed and account restored.",
        "Temporary suspension expired and account was reactivated.",
        "Account restored after the issue was resolved.",
    ],
    ("ACTIVE", "BANNED"): [
        "Account permanently banned due to a serious policy violation.",
        "Account permanently banned following repeated policy violations.",
        "Account banned due to confirmed fraudulent activity.",
        "Account permanently restricted due to severe abuse.",
    ],
    ("SUSPENDED", "BANNED"): [
        "Temporary suspension escalated to a permanent ban.",
        "Account permanently banned following a failed policy review.",
        "Account banned after repeated violations during suspension.",
        "Suspended account permanently banned due to severe policy violations.",
    ],
    ("ACTIVE", "DELETED"): [
        "User permanently deleted the account.",
        "Account permanently deleted at the user's request.",
        "User requested permanent account deletion.",
        "Account was closed and permanently deleted.",
    ],
    ("INACTIVE", "DELETED"): [
        "Inactive account was permanently deleted at the user's request.",
        "User permanently deleted the previously inactive account.",
        "Inactive account was closed and permanently deleted.",
    ],
    ("SUSPENDED", "DELETED"): [
        "User permanently deleted the suspended account.",
        "Suspended account was permanently deleted at the user's request.",
        "Account was closed following the suspension.",
    ],
}


def choose_next_status(current_status):
    if current_status in ("BANNED", "DELETED"):
        return None

    if current_status == "ACTIVE":
        choices = [
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "INACTIVE",
            "INACTIVE",
            "SUSPENDED",
            "BANNED",
            "DELETED",
        ]

    elif current_status == "INACTIVE":
        choices = [
            "INACTIVE",
            "INACTIVE",
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "DELETED",
        ]

    elif current_status == "SUSPENDED":
        choices = [
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "SUSPENDED",
            "BANNED",
            "DELETED",
        ]

    else:
        return None

    return random.choice(choices)


def get_status_description(previous_status, new_status):
    descriptions = STATUS_DESCRIPTIONS.get((previous_status, new_status))

    if descriptions:
        return random.choice(descriptions)

    if previous_status == new_status:
        if new_status == "ACTIVE":
            return "Account remained active."

        if new_status == "INACTIVE":
            return "Account remained inactive."

        if new_status == "SUSPENDED":
            return "Account remained suspended."

        if new_status == "BANNED":
            return "Account remained permanently banned."

        if new_status == "DELETED":
            return "Account remained permanently deleted."

    return f"Account status changed from {previous_status} to {new_status}."


def generate_status_history(number_of_versions):
    if number_of_versions <= 0:
        return []

    dates = generate_dates(number_of_versions)

    history = []

    current_status = "ACTIVE"

    history.append(
        {
            "status": "ACTIVE",
            "status_change_description": "Creation of the account.",
            "valid_from": dates[0],
        }
    )

    for i in range(1, len(dates)):
        previous_status = current_status

        next_status = choose_next_status(current_status)

        if next_status is None:
            break

        current_status = next_status

        description = get_status_description(previous_status, current_status)

        history.append(
            {
                "status": current_status,
                "status_change_description": description,
                "valid_from": dates[i],
            }
        )

        if current_status in ("BANNED", "DELETED"):
            break

    for i in range(len(history)):
        if i == len(history) - 1:
            history[i]["valid_to"] = None
            history[i]["is_current"] = 1
        else:
            history[i]["valid_to"] = history[i + 1]["valid_from"]
            history[i]["is_current"] = 0

    return history


def generate_locations(number_of_locations):
    location_table = []

    for i in range(number_of_locations):
        location_table.append(
            {
                "location_id": str(i + 1),
                "city": fake.city(),
                "state": fake.state(),
                "zip_code": fake.zipcode(),
                "country": "United States",
            }
        )

    return location_table


def generate_number_of_location_versions():
    choice = random.randint(0, 10)

    if choice <= 4:
        return 1
    elif choice <= 6:
        return 2
    elif choice <= 8:
        return 3
    else:
        return 4


def generate_number_of_status_versions():
    choice = random.randint(0, 10)

    if choice <= 4:
        return 1
    elif choice <= 6:
        return 2
    elif choice <= 8:
        return 3
    else:
        return 4


def generate_data():
    users_table = []
    user_profile_table = []
    user_contact_table = []
    user_status_history_table = []
    user_location_table = []
    location_table = []

    location_table = generate_locations(NUMBER_OF_LOCATIONS)

    for i in range(NUMBER_OF_USERS):
        # 0000000002
        user_id = str(i + 1).zfill(10)
        customer_unique_id = f"CUST-{user_id}"
        created_at = datetime.now()

        users_table.append(
            {
                "user_id": user_id,
                "customer_unique_id": customer_unique_id,
                "created_at": created_at,
            }
        )

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
            # For NOT_SPECIFIED option we will generate generic names
            first_name = fake.first_name()
            last_name = fake.last_name()

        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=80)

        user_profile_table.append(
            {
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "birth_date": birth_date,
                "gender": gender,
            }
        )

        email = fake.email()
        phone = fake.phone_number()

        user_contact_table.append(
            {
                "user_id": user_id,
                "email": email,
                "phone": phone,
            }
        )

        number_of_location_versions = generate_number_of_location_versions()
        location_ids = random.sample(
            range(1, NUMBER_OF_LOCATIONS + 1), number_of_location_versions
        )
        location_dates = generate_dates(number_of_location_versions)

        for j in range(len(location_dates)):
            # The last date is treated as current
            if j == len(location_dates) - 1:
                is_current = 1
                valid_to = None
            else:
                is_current = 0
                valid_to = location_dates[j + 1]

            current_length = len(user_location_table)
            user_location_table.append(
                {
                    "id": current_length + 1,
                    "user_id": user_id,
                    "location_id": location_ids[j],
                    "valid_from": location_dates[j],
                    "valid_to": valid_to,
                    "is_current": is_current,
                }
            )

        number_of_status_versions = generate_number_of_status_versions()
        status_history = generate_status_history(number_of_status_versions)

        for status_record in status_history:
            current_length = len(user_status_history_table)

            status_history_id = str(current_length + 1).zfill(10)

            user_status_history_table.append(
                {
                    "id": status_history_id,
                    "user_id": user_id,
                    "status": status_record["status"],
                    "status_change_description": (
                        status_record["status_change_description"]
                    ),
                    "valid_from": status_record["valid_from"],
                    "valid_to": status_record["valid_to"],
                    "is_current": status_record["is_current"],
                }
            )

    return (
        users_table,
        user_profile_table,
        user_contact_table,
        user_location_table,
        location_table,
        user_status_history_table,
    )


def format_csv_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def write_csv_file(destination, data):
    field_names = data[0].keys()

    with open(destination, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)

        writer.writeheader()

        for row in data:
            formatted_row = {key: format_csv_value(value) for key, value in row.items()}

            writer.writerow(formatted_row)


if __name__ == "__main__":
    (
        users_table,
        user_profile_table,
        user_contact_table,
        user_location_table,
        location_table,
        user_status_history_table,
    ) = generate_data()

    write_csv_file(f"{OUTPUT_DIRECTORY}/users_table.csv", users_table)

    write_csv_file(f"{OUTPUT_DIRECTORY}/user_profile_table.csv", user_profile_table)

    write_csv_file(f"{OUTPUT_DIRECTORY}/user_contact_table.csv", user_contact_table)

    write_csv_file(f"{OUTPUT_DIRECTORY}/user_location_table.csv", user_location_table)

    write_csv_file(f"{OUTPUT_DIRECTORY}/location_table.csv", location_table)

    write_csv_file(
        f"{OUTPUT_DIRECTORY}/user_status_history_table.csv", user_status_history_table
    )

    print(f"Users: {len(users_table)}")
    print(f"Profiles: {len(user_profile_table)}")
    print(f"Contacts: {len(user_contact_table)}")
    print(f"Locations: {len(location_table)}")
    print(f"User location history: {len(user_location_table)}")
    print(f"User status history: {len(user_status_history_table)}")
