import random


def generate_payments_domain_tables(order_payment_info):
    details, history = [], []
    payment_types = ["CREDIT_CARD", "BLIK", "VOUCHER", "BANK_TRANSFER"]

    for order in order_payment_info:
        order_id = order["order_id"]
        total_value = order["total_value"]
        payment_date = order["payment_started_at"]
        end_time = order["paid_at"]

        num_payments = random.choices([1, 2, 3], weights=[78, 18, 4])[0]
        if num_payments == 1:
            values = [total_value]
        elif num_payments == 2:
            first_val = round(total_value * random.uniform(0.15, 0.50), 2)
            values = [first_val, round(total_value - first_val, 2)]
        else:
            first_val = round(total_value * 0.20, 2)
            second_val = round(total_value * 0.30, 2)
            values = [
                first_val,
                second_val,
                round(total_value - first_val - second_val, 2),
            ]

        order_payment_types = random.sample(payment_types, num_payments)

        for seq in range(1, num_payments + 1):
            pay_id = f"PAY-{len(details) + 1}"
            p_type = order_payment_types[seq - 1]
            val = values[seq - 1]

            installments = (
                random.choices([1, 2, 3, 6, 12], weights=[40, 15, 15, 15, 15])[0]
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

            if random.random() < 0.08:
                mid_time = payment_date + (end_time - payment_date) / 2
                history.append(
                    {
                        "log_id": f"LOG-{len(history) + 1}",
                        "payment_id": pay_id,
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
                    "payment_id": pay_id,
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
                    "payment_id": pay_id,
                    "status": "COMPLETED",
                    "gateway_response": "200_OK",
                    "valid_from": end_time,
                    "valid_to": None,
                    "is_current": 1,
                }
            )

    return details, history
