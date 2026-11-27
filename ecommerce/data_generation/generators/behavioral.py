"""Generates synthetic user browsing sessions and clickstream event logs.

Simulates sessions tied to completed orders and general sessions. Combined with event-level clickstream data,
simulating each user's action on the website, including page views, product views, searches, cart additions/removals, and purchases.
"""

from datetime import datetime, timedelta
import random
import config
import utils

DEVICE_TYPES = ["Desktop", "Mobile", "Tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
TRAFFIC_SOURCES = ["Google", "Direct", "Facebook", "Newsletter", "Twitter"]
GENERAL_ARCHETYPES = ["bounce", "brief", "cart_abandonment", "extended_research"]
GENERAL_ARCHETYPE_WEIGHTS = [20, 30, 30, 20]


def append_event(
    clickstream_table: list[dict],
    event_counter: int,
    session_id: str,
    user_id: str,
    product_id: str,
    event_time: datetime,
    event_type: str,
) -> int:
    """Appends a single clickstream event record and increments the event counter.

    Args:
        clickstream_table: List collecting generated clickstream event rows.
        event_counter: Sequential integer used to format event_id.
        session_id: Parent session identifier.
        user_id: User identifier associated with the session.
        product_id: Target product identifier, or empty string for non-product events.
        event_time: Timestamp when the event occurred.
        event_type: Action type (for example page_view, add_to_cart, or purchase).

    Returns:
        Next sequential event counter value.
    """
    clickstream_table.append(
        {
            "session_id": session_id,
            "user_id": user_id,
            "event_id": f"EVT-{str(event_counter).zfill(12)}",
            "product_id": product_id,
            "event_time": event_time,
            "event_type": event_type,
        }
    )
    return event_counter + 1


def _append_event_sequence(
    clickstream_table: list[dict],
    event_counter: int,
    session_id: str,
    user_id: str,
    session_start: datetime,
    session_end: datetime,
    events: list[tuple[str, str]],
    min_step_sec: int,
    max_step_sec: int,
) -> int:
    """Appends a timed sequence of clickstream events bounded by session_end.

    Args:
        clickstream_table: List collecting generated clickstream event rows.
        event_counter: Current sequential event counter.
        session_id: Parent session identifier.
        user_id: User identifier associated with the session.
        session_start: Start timestamp of the session.
        session_end: End timestamp of the session.
        events: Ordered list of (event_type, product_id) pairs.
        min_step_sec: Minimum seconds elapsed between consecutive events.
        max_step_sec: Maximum seconds elapsed between consecutive events.

    Returns:
        Updated event counter after appending the sequence.
    """
    curr_time = session_start
    for ev_type, prod_id in events:
        if curr_time > session_end:
            break
        event_counter = append_event(
            clickstream_table,
            event_counter,
            session_id,
            user_id,
            prod_id,
            curr_time,
            ev_type,
        )
        curr_time += timedelta(seconds=random.randint(min_step_sec, max_step_sec))
    return event_counter


def generate_sessions_and_clickstreams(
    users_table: list[dict],
    product_catalog: list[dict],
    order_purchase_meta: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Generates chronological orders, sessions, and clickstream event tables.

    Builds one session per order in order_purchase_meta and non-converting sessions across four
    behavioral archetypes, then sorts all sessions chronologically and returns their detailed clickstream events and session information.

    Args:
        users_table: List of user dimension records.
        product_catalog: List of product catalog records.
        order_purchase_meta: List of order metadata dicts containing order_id,
        user_id, created_at, has_discount, and purchased items.

    Returns:
        Tuple of (orders_table, sessions_table, clickstream_table).
    """
    raw_sessions = []

    user_ids = [u["user_id"] for u in users_table]
    unique_product_ids = list({p["product_id"] for p in product_catalog})

    for order in order_purchase_meta:
        created_at = order["created_at"]
        duration_sec = random.choices(
            [
                random.randint(45, 59),
                random.randint(65, 295),
                random.randint(305, 1500),
            ],
            weights=[10, 45, 45],
        )[0]
        session_start = created_at - timedelta(seconds=duration_sec)

        raw_sessions.append(
            {
                "type": "purchase",
                "archetype": "purchase",
                "order_id": order["order_id"],
                "user_id": order["user_id"],
                "session_start": session_start,
                "session_end": created_at,
                "device_type": random.choices(DEVICE_TYPES, weights=[45, 45, 10])[0],
                "browser": random.choice(BROWSERS),
                "traffic_source": random.choice(TRAFFIC_SOURCES),
                "has_discount": order.get("has_discount", False),
                "items": order["items"],
            }
        )

    for _ in range(config.NUMBER_OF_GENERAL_SESSIONS):
        user_id = random.choice(user_ids)
        session_start = utils.random_date(
            config.FOUNDATION_END, config.NOW - timedelta(hours=1)
        )
        archetype = random.choices(
            GENERAL_ARCHETYPES, weights=GENERAL_ARCHETYPE_WEIGHTS
        )[0]

        if archetype == "bounce":
            duration_sec = random.randint(2, 9)
        elif archetype == "brief":
            duration_sec = random.randint(12, 58)
        elif archetype == "cart_abandonment":
            duration_sec = random.randint(65, 295)
        elif archetype == "extended_research":
            duration_sec = random.randint(310, 1200)

        raw_sessions.append(
            {
                "type": "general",
                "archetype": archetype,
                "order_id": None,
                "user_id": user_id,
                "session_start": session_start,
                "session_end": session_start + timedelta(seconds=duration_sec),
                "device_type": random.choices(DEVICE_TYPES, weights=[40, 50, 10])[0],
                "browser": random.choice(BROWSERS),
                "traffic_source": random.choice(TRAFFIC_SOURCES),
                "has_discount": False,
                "items": [],
            }
        )

    raw_sessions.sort(key=lambda x: x["session_start"])

    orders_table, sessions_table = [], []
    clickstream_table: list[dict] = []
    session_counter = 1
    event_counter = 1

    for raw in raw_sessions:
        session_id = f"SES-{str(session_counter).zfill(10)}"
        session_counter += 1

        sessions_table.append(
            {
                "session_id": session_id,
                "user_id": raw["user_id"],
                "session_start": raw["session_start"],
                "session_end": raw["session_end"],
                "device_type": raw["device_type"],
                "browser": raw["browser"],
                "traffic_source": raw["traffic_source"],
            }
        )

        if raw["type"] == "purchase":
            orders_table.append(
                {
                    "order_id": raw["order_id"],
                    "user_id": raw["user_id"],
                    "session_id": session_id,
                    "created_at": raw["session_end"],
                }
            )

            planned_events = [("page_view", "")]
            if random.random() < 0.6:
                planned_events.append(("search", ""))

            for item in raw["items"]:
                p_id = item["product_id"]
                planned_events.append(("view_product", p_id))
                if random.random() < 0.25:
                    planned_events.append(("view_reviews", p_id))
                if random.random() < 0.20:
                    planned_events.append(("wishlist", p_id))
                for _ in range(item["quantity"]):
                    planned_events.append(("add_to_cart", p_id))

            if raw["has_discount"] or random.random() < 0.2:
                planned_events.append(("apply_coupon", ""))

            planned_events.append(("checkout", ""))
            planned_events.append(("payment", ""))
            planned_events.append(("purchase", ""))

            total_span = max(
                len(planned_events),
                int((raw["session_end"] - raw["session_start"]).total_seconds()),
            )
            step_sec = max(1, total_span // len(planned_events))
            curr_time = raw["session_start"]

            for idx, (ev_type, p_id) in enumerate(planned_events):
                if idx == len(planned_events) - 1:
                    ev_time = raw["session_end"]
                else:
                    ev_time = min(
                        curr_time,
                        raw["session_end"]
                        - timedelta(seconds=len(planned_events) - idx),
                    )
                event_counter = append_event(
                    clickstream_table,
                    event_counter,
                    session_id,
                    raw["user_id"],
                    p_id,
                    ev_time,
                    ev_type,
                )
                curr_time = ev_time + timedelta(
                    seconds=random.randint(1, max(1, step_sec))
                )

        else:
            archetype = raw["archetype"]

            if archetype == "bounce":
                event_counter = append_event(
                    clickstream_table,
                    event_counter,
                    session_id,
                    raw["user_id"],
                    "",
                    raw["session_start"],
                    "page_view",
                )

            elif archetype == "brief":
                p_id = random.choice(unique_product_ids)
                event_counter = _append_event_sequence(
                    clickstream_table,
                    event_counter,
                    session_id,
                    raw["user_id"],
                    raw["session_start"],
                    raw["session_end"],
                    [("page_view", ""), ("search", ""), ("view_product", p_id)],
                    min_step_sec=4,
                    max_step_sec=15,
                )

            elif archetype == "cart_abandonment":
                p_id = random.choice(unique_product_ids)
                event_counter = _append_event_sequence(
                    clickstream_table,
                    event_counter,
                    session_id,
                    raw["user_id"],
                    raw["session_start"],
                    raw["session_end"],
                    [
                        ("page_view", ""),
                        ("view_product", p_id),
                        ("add_to_cart", p_id),
                        (random.choice(["remove_from_cart", "wishlist"]), p_id),
                    ],
                    min_step_sec=12,
                    max_step_sec=45,
                )

            elif archetype == "extended_research":
                curr_time = raw["session_start"]
                event_counter = append_event(
                    clickstream_table,
                    event_counter,
                    session_id,
                    raw["user_id"],
                    "",
                    curr_time,
                    "page_view",
                )
                curr_time += timedelta(seconds=random.randint(10, 30))
                for _ in range(random.randint(4, 8)):
                    if curr_time >= raw["session_end"]:
                        break
                    ev_type = random.choices(
                        [
                            "search",
                            "view_product",
                            "view_reviews",
                            "wishlist",
                            "add_to_cart",
                            "remove_from_cart",
                        ],
                        weights=[20, 35, 15, 10, 10, 10],
                    )[0]
                    p_id = (
                        random.choice(unique_product_ids) if ev_type != "search" else ""
                    )
                    event_counter = append_event(
                        clickstream_table,
                        event_counter,
                        session_id,
                        raw["user_id"],
                        p_id,
                        curr_time,
                        ev_type,
                    )
                    curr_time += timedelta(seconds=random.randint(25, 90))

    return orders_table, sessions_table, clickstream_table
