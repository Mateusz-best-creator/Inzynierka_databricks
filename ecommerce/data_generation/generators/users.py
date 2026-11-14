import random
from datetime import date
import config
import utils

STATUS_DESCRIPTIONS = {
    ("ACTIVE", "INACTIVE"): [
        "User voluntarily deactivated the account.",
        "User temporarily disabled the account.",
    ],
    ("INACTIVE", "ACTIVE"): [
        "User reactivated the account.",
        "User returned and reactivated the account.",
    ],
    ("ACTIVE", "SUSPENDED"): [
        "Account temporarily suspended due to suspicious activity."
    ],
    ("SUSPENDED", "ACTIVE"): ["Account suspension was lifted after review."],
    ("ACTIVE", "BANNED"): [
        "Account permanently banned due to a serious policy violation."
    ],
    ("SUSPENDED", "BANNED"): ["Temporary suspension escalated to a permanent ban."],
    ("ACTIVE", "DELETED"): ["User permanently deleted the account."],
    ("INACTIVE", "DELETED"): [
        "Inactive account was permanently deleted at the user's request."
    ],
    ("SUSPENDED", "DELETED"): ["User permanently deleted the suspended account."],
}


def choose_next_status(current_status):
    if current_status in ("BANNED", "DELETED"):
        return None

    if current_status == "ACTIVE":
        choices = [
            "ACTIVE",
            "ACTIVE",
            "ACTIVE",
            "INACTIVE",
            "SUSPENDED",
            "BANNED",
            "DELETED",
        ]
    elif current_status == "INACTIVE":
        choices = ["INACTIVE", "ACTIVE", "DELETED"]
    elif current_status == "SUSPENDED":
        choices = ["ACTIVE", "SUSPENDED", "BANNED", "DELETED"]
    else:
        return None
    return random.choice(choices)


def get_status_description(previous_status, new_status):
    descriptions = STATUS_DESCRIPTIONS.get((previous_status, new_status))
    if descriptions:
        return random.choice(descriptions)
    if previous_status == new_status:
        return f"Account remained {new_status.lower()}."
    return f"Account status changed from {previous_status} to {new_status}."


def generate_status_history(created_at, number_of_versions):
    if number_of_versions <= 0:
        return []

    dates = utils.generate_scd2_timeline(created_at, number_of_versions)
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
                "city": random.choice(utils.POOL_CITIES),
                "state": random.choice(utils.POOL_STATES),
                "zip_code": random.choice(utils.POOL_ZIPCODES),
                "country": "United States",
            }
        )
    return location_table


def generate_data():
    users_table, user_profile_table, user_contact_table = [], [], []
    user_status_history_table, user_location_table = [], []
    location_table = generate_locations(config.NUMBER_OF_LOCATIONS)

    for i in range(config.NUMBER_OF_USERS):
        user_id = str(i + 1).zfill(10)
        user_created_at = utils.random_date(
            config.FOUNDATION_START, config.FOUNDATION_END
        )
        users_table.append(
            {
                "user_id": user_id,
                "customer_unique_id": f"CUST-{user_id}",
                "created_at": user_created_at,
            }
        )

        gender = (
            "NOT_SPECIFIED"
            if i > 0 and i % config.NOT_SPECIFIED_GENDER_RATE == 0
            else ("MALE" if random.choice([0, 1]) == 0 else "FEMALE")
        )
        if gender == "MALE":
            first_name = random.choice(utils.POOL_FIRST_MALE)
            last_name = random.choice(utils.POOL_LAST_NAMES)
        elif gender == "FEMALE":
            first_name = random.choice(utils.POOL_FIRST_FEMALE)
            last_name = random.choice(utils.POOL_LAST_NAMES)
        else:
            first_name = random.choice(utils.POOL_FIRST_NEUTRAL)
            last_name = random.choice(utils.POOL_LAST_NAMES)

        user_profile_table.append(
            {
                "user_id": user_id,
                "first_name": first_name,
                "last_name": last_name,
                "birth_date": date(
                    random.randint(1945, 2005),
                    random.randint(1, 12),
                    random.randint(1, 28),
                ),
                "gender": gender,
            }
        )

        email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@gmail.com"
        phone = random.choice(utils.POOL_PHONES)
        user_contact_table.append({"user_id": user_id, "email": email, "phone": phone})

        status_history = generate_status_history(
            user_created_at, random.choices([1, 2, 3, 4], weights=[55, 25, 12, 8])[0]
        )

        terminal_date = None
        for status_record in status_history:
            if status_record["status"] in ("DELETED", "BANNED"):
                terminal_date = status_record["valid_from"]
                break

        num_loc_versions = random.choices([1, 2, 3], weights=[60, 25, 15])[0]
        loc_dates = utils.generate_scd2_timeline(user_created_at, num_loc_versions)

        if terminal_date:
            loc_dates = [d for d in loc_dates if d < terminal_date]
            if not loc_dates:
                loc_dates = [user_created_at]

        loc_ids = random.sample(
            range(1, config.NUMBER_OF_LOCATIONS + 1), len(loc_dates)
        )

        for j in range(len(loc_dates)):
            is_last = j == len(loc_dates) - 1
            is_current = 1 if is_last else 0
            valid_to = None if is_last else loc_dates[j + 1]

            user_location_table.append(
                {
                    "id": len(user_location_table) + 1,
                    "user_id": user_id,
                    "location_id": str(loc_ids[j]),
                    "valid_from": loc_dates[j],
                    "valid_to": valid_to,
                    "is_current": is_current,
                }
            )

        for status_record in status_history:
            user_status_history_table.append(
                {
                    "id": str(len(user_status_history_table) + 1).zfill(10),
                    "user_id": user_id,
                    "status": status_record["status"],
                    "status_change_description": status_record[
                        "status_change_description"
                    ],
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
