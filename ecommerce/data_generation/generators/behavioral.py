import random
from datetime import datetime, timedelta
import config
import utils


def append_event(
    clickstream_table: list,
    event_counter: int,
    session_id: str,
    user_id: str,
    product_id: str,
    event_time: datetime,
    event_type: str,
) -> int:
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


def generate_sessions_and_clickstreams(
    users_table, product_catalog, order_purchase_meta
):
    raw_sessions = []

    device_types = ["Desktop", "Mobile", "Tablet"]
    browsers = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
    traffic_sources = ["Google", "Direct", "Facebook", "Newsletter", "Twitter"]

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
                "device_type": random.choices(device_types, weights=[45, 45, 10])[0],
                "browser": random.choice(browsers),
                "traffic_source": random.choice(traffic_sources),
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
            ["bounce", "brief", "cart_abandonment", "extended_research"],
            weights=[20, 30, 30, 20],
        )[0]

        if archetype == "bounce":
            duration_sec = random.randint(2, 9)
        elif archetype == "brief":
            duration_sec = random.randint(12, 58)
        elif archetype == "cart_abandonment":
            duration_sec = random.randint(65, 295)
        else:
            duration_sec = random.randint(310, 1200)

        raw_sessions.append(
            {
                "type": "general",
                "archetype": archetype,
                "order_id": None,
                "user_id": user_id,
                "session_start": session_start,
                "session_end": session_start + timedelta(seconds=duration_sec),
                "device_type": random.choices(device_types, weights=[40, 50, 10])[0],
                "browser": random.choice(browsers),
                "traffic_source": random.choice(traffic_sources),
                "has_discount": False,
                "items": [],
            }
        )

    raw_sessions.sort(key=lambda x: x["session_start"])

    orders_table, sessions_table, clickstream_table = [], [], []
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
                seq = [("page_view", ""), ("search", ""), ("view_product", p_id)]
                curr_time = raw["session_start"]
                for ev_type, prod in seq:
                    if curr_time > raw["session_end"]:
                        break
                    event_counter = append_event(
                        clickstream_table,
                        event_counter,
                        session_id,
                        raw["user_id"],
                        prod,
                        curr_time,
                        ev_type,
                    )
                    curr_time += timedelta(seconds=random.randint(4, 15))

            elif archetype == "cart_abandonment":
                p_id = random.choice(unique_product_ids)
                seq = [
                    ("page_view", ""),
                    ("view_product", p_id),
                    ("add_to_cart", p_id),
                    (random.choice(["remove_from_cart", "wishlist"]), p_id),
                ]
                curr_time = raw["session_start"]
                for ev_type, prod in seq:
                    if curr_time > raw["session_end"]:
                        break
                    event_counter = append_event(
                        clickstream_table,
                        event_counter,
                        session_id,
                        raw["user_id"],
                        prod,
                        curr_time,
                        ev_type,
                    )
                    curr_time += timedelta(seconds=random.randint(12, 45))

            else:
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
