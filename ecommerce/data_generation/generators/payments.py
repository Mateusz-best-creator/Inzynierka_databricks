"""Generates synthetic payment detail records and SCD Type 2 payment status logs.

Simulates single and multi-method split payments per order, credit card
installment plans, and payment gateway status transitions.
"""

from datetime import datetime
import random

PAYMENT_TYPES = ["CREDIT_CARD", "BLIK", "VOUCHER", "BANK_TRANSFER"]
SPLIT_COUNTS = [1, 2, 3]
SPLIT_WEIGHTS = [78, 18, 4]
INSTALLMENT_OPTIONS = [1, 2, 3, 6, 12]
INSTALLMENT_WEIGHTS = [40, 15, 15, 15, 15]
INITIAL_FAILURE_RATE = 0.08


def _split_payment_values(total_value: float, num_payments: int) -> list[float]:
    """Splits an order's total payment amount across 1 to 3 payment methods.

    Args:
        total_value: Total order amount to allocate.
        num_payments: Number of separate payment transactions (1, 2, or 3).

    Returns:
        List of rounded payment amounts summing to total_value.
    """
    if num_payments == 1:
        return [total_value]
    if num_payments == 2:
        first_val = round(total_value * random.uniform(0.15, 0.50), 2)
        return [first_val, round(total_value - first_val, 2)]

    first_val = round(total_value * 0.20, 2)
    second_val = round(total_value * 0.30, 2)
    return [
        first_val,
        second_val,
        round(total_value - first_val - second_val, 2),
    ]


def _append_payment_history(
    history: list[dict],
    payment_id: str,
    payment_date: datetime,
    end_time: datetime,
) -> None:
    """Appends SCD Type 2 status for a single payment transaction.

    Simulates an optional initial payment decline before transitioning through
    PENDING to COMPLETED.

    Args:
        history: List collecting payment status history records.
        payment_id: Parent payment transaction ID.
        payment_date: Timestamp when payment processing began.
        end_time: Timestamp when payment completed.
    """
    if random.random() < INITIAL_FAILURE_RATE:
        mid_time = payment_date + (end_time - payment_date) / 2
        history.append(
            {
                "log_id": f"LOG-{len(history) + 1}",
                "payment_id": payment_id,
                "status": "FAILED",
                "gateway_response": "300_DECLINED",
                "valid_from": payment_date,
                "valid_to": mid_time,
                "is_current": 0,
            }
        )
        pending_start = mid_time
    else:
        pending_start = payment_date

    history.append(
        {
            "log_id": f"LOG-{len(history) + 1}",
            "payment_id": payment_id,
            "status": "PENDING",
            "gateway_response": "100_INIT",
            "valid_from": pending_start,
            "valid_to": end_time,
            "is_current": 0,
        }
    )
    history.append(
        {
            "log_id": f"LOG-{len(history) + 1}",
            "payment_id": payment_id,
            "status": "COMPLETED",
            "gateway_response": "200_OK",
            "valid_from": end_time,
            "valid_to": None,
            "is_current": 1,
        }
    )


def generate_payments_domain_tables(
    order_payment_info: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Generates payment details and payment status history tables for all orders.

    Args:
        order_payment_info: List of order payment metadata dictionaries containing
        order_id, total_value, payment_started_at, and paid_at.

    Returns:
        Tuple of (details, history) record lists.
    """
    details: list[dict] = []
    history: list[dict] = []

    for order in order_payment_info:
        order_id = order["order_id"]
        total_value = order["total_value"]
        payment_date = order["payment_started_at"]
        end_time = order["paid_at"]

        num_payments = random.choices(SPLIT_COUNTS, weights=SPLIT_WEIGHTS)[0]
        values = _split_payment_values(total_value, num_payments)
        order_payment_types = random.sample(PAYMENT_TYPES, num_payments)

        for seq in range(1, num_payments + 1):
            pay_id = f"PAY-{len(details) + 1}"
            p_type = order_payment_types[seq - 1]
            val = values[seq - 1]

            installments = (
                random.choices(INSTALLMENT_OPTIONS, weights=INSTALLMENT_WEIGHTS)[0]
                if p_type == "CREDIT_CARD"
                else 1
            )

            details.append(
                {
                    "payment_id": pay_id,
                    "order_id": order_id,
                    "payment_sequential": seq,
                    "payment_type": p_type,
                    "payment_installments": installments,
                    "payment_value": val,
                }
            )

            _append_payment_history(history, pay_id, payment_date, end_time)

    return details, history
