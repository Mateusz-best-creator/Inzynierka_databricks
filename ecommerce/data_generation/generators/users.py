"""Generates synthetic user domain tables and SCD Type 2 history records.

Builds user records, demographic profiles, contact details, physical
locations, user-to-location address histories, and account status histories.
"""

from datetime import date, datetime
import random

import config
import utils

TERMINATION_STATUSES = ("BANNED", "DELETED")
STATUS_VERSION_WEIGHTS = [55, 25, 12, 8]
LOCATION_VERSION_WEIGHTS = [60, 25, 15]

STATUS_DESCRIPTIONS: dict[tuple[str, str], list[str]] = {
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


def choose_next_status(current_status: str) -> str | None:
    """Samples the next account status based on the current status.

    Args:
        current_status: Current user account status (for example ACTIVE or
            SUSPENDED).

    Returns:
        Next status string, or None if current_status is terminal or invalid.
    """
    if current_status in TERMINATION_STATUSES:
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


def get_status_description(previous_status: str, new_status: str) -> str:
    """Returns a description for an account status transition.

    Args:
        previous_status: Status prior to the transition.
        new_status: Status after the transition.

    Returns:
        Descriptive message explaining the status change.
    """
    descriptions = STATUS_DESCRIPTIONS.get((previous_status, new_status))
    if descriptions:
        return random.choice(descriptions)
    if previous_status == new_status:
        return f"Account remained {new_status.lower()}."
    return f"Account status changed from {previous_status} to {new_status}."


def generate_status_history(
    created_at: datetime, number_of_versions: int
) -> list[dict]:
    """Generates an SCD Type 2 account status timeline starting at created_at.

    Stops generating further transitions once a terminal status (BANNED or
    DELETED) is reached.

    Args:
        created_at: Timestamp when the user account was created.
        number_of_versions: Maximum number of status versions to generate.

    Returns:
        List of status history dictionaries with valid_from, valid_to, and
        is_current fields populated.
    """
    if number_of_versions <= 0:
        return []

    dates = utils.generate_scd2_timeline(created_at, number_of_versions)
    history: list[dict] = []
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

        if current_status in TERMINATION_STATUSES:
            break

    for i in range(len(history)):
        if i == len(history) - 1:
            history[i]["valid_to"] = None
            history[i]["is_current"] = 1
        else:
            history[i]["valid_to"] = history[i + 1]["valid_from"]
            history[i]["is_current"] = 0

    return history


def generate_locations(number_of_locations: int) -> list[dict]:
    """Generates the shared physical location table.

    Args:
        number_of_locations: Number of distinct location records to create.

    Returns:
        List of location dictionaries with city, state, zip_code, and country.
    """
    location_table: list[dict] = []
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


def generate_user_profile(user_index: int, user_id: str) -> dict:
    """Builds a user profile record with demographic attributes.

    Args:
        user_index: Zero-based user index used for periodic NOT_SPECIFIED gender.
        user_id: Formatted user identifier.

    Returns:
        Dictionary containing user profile fields.
    """
    gender = (
        "NOT_SPECIFIED"
        if user_index > 0 and user_index % config.NOT_SPECIFIED_GENDER_RATE == 0
        else ("MALE" if random.choice([0, 1]) == 0 else "FEMALE")
    )
    if gender == "MALE":
        first_name = random.choice(utils.POOL_FIRST_MALE)
    elif gender == "FEMALE":
        first_name = random.choice(utils.POOL_FIRST_FEMALE)
    else:
        first_name = random.choice(utils.POOL_FIRST_NEUTRAL)

    return {
        "user_id": user_id,
        "first_name": first_name,
        "last_name": random.choice(utils.POOL_LAST_NAMES),
        "birth_date": date(
            random.randint(1945, 2005),
            random.randint(1, 12),
            random.randint(1, 28),
        ),
        "gender": gender,
    }


def append_user_locations(
    user_location_table: list[dict],
    user_id: str,
    user_created_at: datetime,
    terminal_date: datetime | None,
) -> None:
    """Appends SCD Type 2 user-to-location association records for a user.

    Truncates address changes so none occur after terminal_date if the user
    account was banned or deleted.

    Args:
        user_location_table: List collecting user location history rows.
        user_id: Formatted user identifier.
        user_created_at: Account creation timestamp.
        terminal_date: Timestamp when the account was banned or deleted, if any.
    """
    num_loc_versions = random.choices([1, 2, 3], weights=LOCATION_VERSION_WEIGHTS)[0]
    loc_dates = utils.generate_scd2_timeline(user_created_at, num_loc_versions)

    if terminal_date:
        loc_dates = [d for d in loc_dates if d < terminal_date]
        if not loc_dates:
            loc_dates = [user_created_at]

    loc_ids = random.sample(range(1, config.NUMBER_OF_LOCATIONS + 1), len(loc_dates))

    for j in range(len(loc_dates)):
        is_last = j == len(loc_dates) - 1
        user_location_table.append(
            {
                "id": len(user_location_table) + 1,
                "user_id": user_id,
                "location_id": str(loc_ids[j]),
                "valid_from": loc_dates[j],
                "valid_to": None if is_last else loc_dates[j + 1],
                "is_current": 1 if is_last else 0,
            }
        )


def generate_users_data() -> tuple[
    list[dict], list[dict], list[dict], list[dict], list[dict], list[dict]
]:
    """Generates all tables for the user domain.

    Returns:
        Tuple of (users_table, user_profile_table, user_contact_table,
        user_location_table, location_table, user_status_history_table).
    """
    users_table: list[dict] = []
    user_profile_table: list[dict] = []
    user_contact_table: list[dict] = []
    user_status_history_table: list[dict] = []
    user_location_table: list[dict] = []
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

        profile = generate_user_profile(i, user_id)
        user_profile_table.append(profile)

        email = (
            f"{profile['first_name'].lower()}.{profile['last_name'].lower()}"
            f"{random.randint(1, 999)}@gmail.com"
        )
        phone = random.choice(utils.POOL_PHONES)
        user_contact_table.append({"user_id": user_id, "email": email, "phone": phone})

        status_history = generate_status_history(
            user_created_at,
            random.choices([1, 2, 3, 4], weights=STATUS_VERSION_WEIGHTS)[0],
        )

        terminal_date: datetime | None = None
        for status_record in status_history:
            if status_record["status"] in TERMINATION_STATUSES:
                terminal_date = status_record["valid_from"]
                break

        append_user_locations(
            user_location_table, user_id, user_created_at, terminal_date
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
