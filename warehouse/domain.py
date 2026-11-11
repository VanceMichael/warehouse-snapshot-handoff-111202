from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal


def overview(connection):
    rows = [dict(record) for record in connection.execute('SELECT department, status, COUNT(*) AS assets FROM assets GROUP BY department, status ORDER BY department, status')]
    return rows
