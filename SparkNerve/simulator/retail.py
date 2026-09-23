"""RetailDB: the SQL Server source (sales.customers, sales.products, sales.orders).

One simulator, two backends:
* SqliteBackend: in memory, for the live demo and the tests. The `sales` schema
  is an attached database, so the same `sales.orders` SQL works unchanged.
* MssqlBackend: a real SQL Server over pymssql, for the Docker stack.

Every write stamps `modified_at` from a strictly increasing clock, the property
the watermark-based extraction relies on. Defect injectors create rows that
break specific metadata rules, so the quarantine path can be exercised on
purpose, and `fix_defects` repairs them at the source to show recovery.
"""

from __future__ import annotations

import os
import random
import sqlite3
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

SOURCE_TABLES = {"customers": "sales.customers", "products": "sales.products", "orders": "sales.orders"}

DDL = {
    "customers": """
        CREATE TABLE sales.customers (
            customer_id   INT           NOT NULL PRIMARY KEY,
            full_name     NVARCHAR(100) NULL,
            email         NVARCHAR(200) NULL,
            country_code  VARCHAR(3)    NULL,
            signup_date   DATE          NULL,
            modified_at   DATETIME2(6)  NOT NULL
        )""",
    "products": """
        CREATE TABLE sales.products (
            product_id    INT           NOT NULL PRIMARY KEY,
            sku           VARCHAR(20)   NULL,
            product_name  NVARCHAR(100) NULL,
            category      VARCHAR(30)   NULL,
            unit_price    DECIMAL(10,2) NULL,
            is_active     BIT           NOT NULL,
            modified_at   DATETIME2(6)  NOT NULL
        )""",
    "orders": """
        CREATE TABLE sales.orders (
            order_id       BIGINT        NOT NULL PRIMARY KEY,
            customer_id    INT           NULL,
            product_id     INT           NULL,
            quantity       INT           NULL,
            amount         DECIMAL(12,2) NULL,
            status         VARCHAR(20)   NULL,
            order_date     DATE          NULL,
            delivered_date DATE          NULL,
            modified_at    DATETIME2(6)  NOT NULL
        )""",
}

PRIMARY_KEYS = {"customers": "customer_id", "products": "product_id", "orders": "order_id"}

FIRST = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Ananya", "Vihaan", "Isha", "Arjun", "Sara",
         "Liam", "Emma", "Noah", "Olivia", "Mateo", "Sofia", "Yuki", "Chen", "Fatima", "Omar"]
LAST = ["Shah", "Patel", "Iyer", "Rao", "Mehta", "Khan", "Singh", "Das", "Nair", "Gupta",
        "Smith", "Garcia", "Muller", "Rossi", "Tanaka", "Wang", "Haddad", "Silva", "Kim", "Brown"]
COUNTRIES = ["IN", "US", "GB", "DE", "SG", "AE", "AU", "CA"]
CATEGORIES = {
    "Electronics": ["Wireless Earbuds", "USB-C Hub", "Smart Watch", "Power Bank", "Bluetooth Speaker"],
    "Home": ["Cast Iron Pan", "Desk Lamp", "Air Purifier", "Throw Blanket"],
    "Apparel": ["Running Shoes", "Rain Jacket", "Linen Shirt", "Wool Socks"],
    "Books": ["Designing Data-Intensive Applications", "The Pragmatic Programmer", "Clean Architecture"],
    "Grocery": ["Arabica Coffee 1kg", "Green Tea 100 bags", "Dark Chocolate"],
    "Sports": ["Yoga Mat", "Dumbbell Set", "Cycling Helmet"],
}
NEXT_STATUS = {"PLACED": "PAID", "PAID": "SHIPPED", "SHIPPED": "DELIVERED"}
TIERS = ["BRONZE", "SILVER", "GOLD"]


def _sql_value(value: object) -> object:
    """Python value -> what sqlite3 stores (ISO text for dates, text for decimals)."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bool):
        return int(value)
    return value


class SqliteBackend:
    name = "sqlite"

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
        self.conn.execute("ATTACH DATABASE ':memory:' AS sales")

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.conn.execute(sql, tuple(_sql_value(p) for p in params))

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        cursor = self.conn.execute(sql, tuple(_sql_value(p) for p in params))
        names = [d[0] for d in cursor.description]
        return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

    def table_exists(self, table: str) -> bool:
        return bool(self.query("SELECT name FROM sales.sqlite_master WHERE type = 'table' AND name = ?", (table,)))

    def columns(self, table: str) -> dict[str, str]:
        return {r["name"]: r["type"].upper() for r in self.query(f"PRAGMA sales.table_info({table})")}


class MssqlBackend:
    name = "sqlserver"

    def __init__(self, host: str, port: int, user: str, password: str, database: str) -> None:
        import pymssql

        with pymssql.connect(server=host, port=port, user=user, password=password, database="master",
                             autocommit=True) as master:
            master.cursor().execute(f"IF DB_ID('{database}') IS NULL CREATE DATABASE [{database}]")
        self.conn = pymssql.connect(server=host, port=port, user=user, password=password, database=database,
                                    autocommit=True)
        self.execute("IF SCHEMA_ID('sales') IS NULL EXEC('CREATE SCHEMA sales')")

    @classmethod
    def from_env(cls) -> MssqlBackend:
        return cls(
            host=os.getenv("RETAILDB_HOST", "localhost"),
            port=int(os.getenv("RETAILDB_PORT", "1433")),
            user=os.getenv("RETAILDB_USER", "sa"),
            password=os.environ["RETAILDB_PASSWORD"],
            database=os.getenv("RETAILDB_DATABASE", "RetailDB"),
        )

    def execute(self, sql: str, params: tuple = ()) -> None:
        cursor = self.conn.cursor()
        cursor.execute(sql.replace("?", "%s"), params or None)

    def query(self, sql: str, params: tuple = ()) -> list[dict]:
        cursor = self.conn.cursor(as_dict=True)
        cursor.execute(sql.replace("?", "%s"), params or None)
        return list(cursor.fetchall())

    def table_exists(self, table: str) -> bool:
        return bool(self.query("SELECT 1 AS x FROM sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('sales')",
                               (table,)))

    def columns(self, table: str) -> dict[str, str]:
        rows = self.query(
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, "
            "DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'sales' AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            (table,),
        )
        out = {}
        for r in rows:
            kind = r["DATA_TYPE"].upper()
            if kind in ("DECIMAL", "NUMERIC"):
                kind = f"{kind}({r['NUMERIC_PRECISION']},{r['NUMERIC_SCALE']})"
            elif r["CHARACTER_MAXIMUM_LENGTH"]:
                size = "MAX" if r["CHARACTER_MAXIMUM_LENGTH"] == -1 else r["CHARACTER_MAXIMUM_LENGTH"]
                kind = f"{kind}({size})"
            elif kind == "DATETIME2":
                kind = f"DATETIME2({r['DATETIME_PRECISION']})"
            out[r["COLUMN_NAME"]] = kind
        return out


class RetailDB:
    def __init__(self, backend: SqliteBackend | MssqlBackend, seed: int = 2026, start: datetime | None = None,
                 clock: Callable[[], datetime] | None = None) -> None:
        """`start` gives a simulated clock (deterministic tests); otherwise wall-clock UTC."""
        self.db = backend
        self.rng = random.Random(seed)
        self._simulated = start
        self._clock = clock or (lambda: datetime.now(timezone.utc).replace(tzinfo=None))
        self._last: datetime | None = None
        self.defects: dict[tuple[str, int], str] = {}

    # ---- clock ---------------------------------------------------------------

    def _tick(self) -> datetime:
        if self._simulated is not None:
            self._simulated += timedelta(seconds=self.rng.randint(5, 90))
            now = self._simulated
        else:
            now = self._clock()
        if self._last is not None and now <= self._last:
            now = self._last + timedelta(microseconds=1)
        self._last = now
        return now

    @property
    def today(self) -> date:
        return (self._last or self._simulated or self._clock()).date()

    @property
    def now(self) -> datetime:
        return self._last or self._simulated or self._clock()

    # ---- schema ----------------------------------------------------------------

    def create_schema(self) -> None:
        for table, ddl in DDL.items():
            if not self.db.table_exists(table):
                self.db.execute(ddl)

    def columns(self, table: str) -> dict[str, str]:
        return self.db.columns(table)

    def add_column(self, table: str, column: str, sql_type: str) -> str:
        if column in self.columns(table):
            return f"sales.{table}.{column} already exists"
        self.db.execute(f"ALTER TABLE sales.{table} ADD {column} {sql_type} NULL")
        touched = 0
        if table == "customers" and column == "loyalty_tier":
            for row in self._pick("SELECT customer_id FROM sales.customers ORDER BY customer_id", 6):
                self.db.execute("UPDATE sales.customers SET loyalty_tier = ?, modified_at = ? WHERE customer_id = ?",
                                (self.rng.choice(TIERS), self._tick(), row["customer_id"]))
                touched += 1
        populated = f" (+{touched} rows populated)" if touched else ""
        return f"ALTER TABLE sales.{table} ADD {column} {sql_type}{populated}"

    def drop_column(self, table: str, column: str) -> str:
        if column not in self.columns(table):
            return f"sales.{table}.{column} does not exist"
        self.db.execute(f"ALTER TABLE sales.{table} DROP COLUMN {column}")
        return f"ALTER TABLE sales.{table} DROP COLUMN {column}"

    # ---- reads -------------------------------------------------------------------

    def rows(self, table: str) -> list[dict]:
        return self.db.query(f"SELECT * FROM sales.{table} ORDER BY {PRIMARY_KEYS[table]}")

    def count(self, table: str) -> int:
        return int(self.db.query(f"SELECT COUNT(*) AS n FROM sales.{table}")[0]["n"])

    def _pick(self, sql: str, k: int, params: tuple = ()) -> list[dict]:
        rows = self.db.query(sql, params)
        return self.rng.sample(rows, min(k, len(rows))) if rows else []

    def _next_id(self, table: str, start: int) -> int:
        top = self.db.query(f"SELECT MAX({PRIMARY_KEYS[table]}) AS m FROM sales.{table}")[0]["m"]
        return max(int(top or 0) + 1, start)

    # ---- writes ------------------------------------------------------------------

    def _insert(self, table: str, row: dict) -> None:
        cols = ", ".join(row)
        marks = ", ".join("?" for _ in row)
        self.db.execute(f"INSERT INTO sales.{table} ({cols}) VALUES ({marks})", tuple(row.values()))

    def _update(self, table: str, key: int, changes: dict) -> None:
        changes = {**changes, "modified_at": self._tick()}
        sets = ", ".join(f"{c} = ?" for c in changes)
        pk = PRIMARY_KEYS[table]
        self.db.execute(f"UPDATE sales.{table} SET {sets} WHERE {pk} = ?", (*changes.values(), key))

    def _customer(self, customer_id: int) -> dict:
        first, last = self.rng.choice(FIRST), self.rng.choice(LAST)
        row = {
            "customer_id": customer_id,
            "full_name": f"{first} {last}",
            "email": f"{first}.{last}{customer_id}@example.com".lower(),
            "country_code": self.rng.choice(COUNTRIES),
            "signup_date": self.today - timedelta(days=self.rng.randint(0, 900)),
            "modified_at": self._tick(),
        }
        if "loyalty_tier" in self.columns("customers"):
            row["loyalty_tier"] = self.rng.choice(TIERS)
        return row

    def _product(self, product_id: int) -> dict:
        category = self.rng.choice(sorted(CATEGORIES))
        return {
            "product_id": product_id,
            "sku": f"SKU-{10000 + product_id:05d}",
            "product_name": self.rng.choice(CATEGORIES[category]),
            "category": category,
            "unit_price": Decimal(self.rng.randint(299, 49999)) / 100,
            "is_active": True,
            "modified_at": self._tick(),
        }

    def _order(self, order_id: int, order_date: date | None = None) -> dict:
        bad = {key for (table, key) in self.defects if table in ("customers", "products")}
        customers = [r for r in self.db.query("SELECT customer_id FROM sales.customers ORDER BY customer_id")
                     if r["customer_id"] not in bad]
        products = [r for r in self.db.query(
            "SELECT product_id, unit_price FROM sales.products WHERE is_active = 1 AND unit_price > 0 "
            "ORDER BY product_id") if r["product_id"] not in bad]
        customer = self.rng.choice(customers)
        product = self.rng.choice(products)
        quantity = self.rng.randint(1, 4)
        order_date = order_date or self.today
        age = (self.today - order_date).days
        status = "PLACED"
        delivered = None
        if age >= 6:
            status = self.rng.choice(["DELIVERED", "DELIVERED", "DELIVERED", "CANCELLED"])
        elif age >= 1:
            status = self.rng.choice(["PAID", "SHIPPED", "PLACED"])
        if status == "DELIVERED":
            delivered = order_date + timedelta(days=self.rng.randint(2, 5))
        return {
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "amount": (Decimal(str(product["unit_price"])) * quantity).quantize(Decimal("0.01")),
            "status": status,
            "order_date": order_date,
            "delivered_date": delivered,
            "modified_at": self._tick(),
        }

    def seed(self, customers: int = 40, products: int = 14, orders: int = 240, history_days: int = 30) -> str:
        """Initial load (idempotent: does nothing if the tables already hold data)."""
        self.create_schema()
        if self.count("orders"):
            return "RetailDB already seeded"
        for i in range(1, customers + 1):
            self._insert("customers", self._customer(i))
        for i in range(1, products + 1):
            self._insert("products", self._product(i))
        for i in range(orders):
            day = self.today - timedelta(days=self.rng.randint(1, history_days))
            self._insert("orders", self._order(100001 + i, day))
        return f"Seeded {customers} customers, {products} products, {orders} orders over {history_days} days"

    # ---- workloads ---------------------------------------------------------------

    def business_as_usual(self, changes: int = 25) -> list[str]:
        """Realistic valid traffic: new orders, status progressions, new customers, edits."""
        log = []
        for _ in range(changes):
            roll = self.rng.random()
            if roll < 0.40:
                order = self._order(self._next_id("orders", 100001))
                self._insert("orders", order)
                log.append(f"INSERT order {order['order_id']} ({order['quantity']} x product {order['product_id']})")
            elif roll < 0.75:
                picked = self._pick(
                    "SELECT order_id, status, order_date FROM sales.orders WHERE status IN ('PLACED','PAID','SHIPPED') "
                    "AND order_date IS NOT NULL ORDER BY order_id", 1)
                if not picked:
                    continue
                row = picked[0]
                if row["status"] == "PLACED" and self.rng.random() < 0.12:
                    new = {"status": "CANCELLED"}
                else:
                    new = {"status": NEXT_STATUS[row["status"]]}
                    if new["status"] == "DELIVERED":
                        new["delivered_date"] = self.today
                self._update("orders", row["order_id"], new)
                log.append(f"UPDATE order {row['order_id']}: {row['status']} -> {new['status']}")
            elif roll < 0.85:
                customer = self._customer(self._next_id("customers", 1))
                self._insert("customers", customer)
                log.append(f"INSERT customer {customer['customer_id']} ({customer['full_name']})")
            elif roll < 0.93:
                picked = self._pick("SELECT customer_id, full_name FROM sales.customers ORDER BY customer_id", 1)
                cid = picked[0]["customer_id"]
                first = picked[0]["full_name"].split(" ")[0] if picked[0]["full_name"] else "user"
                self._update("customers", cid, {"email": f"{first}.{cid}@mail.example.org".lower(),
                                                "country_code": self.rng.choice(COUNTRIES)})
                log.append(f"UPDATE customer {cid}: new email/country")
            elif roll < 0.98:
                picked = self._pick("SELECT product_id, unit_price FROM sales.products ORDER BY product_id", 1)
                pid = picked[0]["product_id"]
                price = (Decimal(str(picked[0]["unit_price"] or 10)) * Decimal("1.05")).quantize(Decimal("0.01"))
                self._update("products", pid, {"unit_price": price})
                log.append(f"UPDATE product {pid}: price -> {price}")
            else:
                product = self._product(self._next_id("products", 1))
                self._insert("products", product)
                log.append(f"INSERT product {product['product_id']} ({product['product_name']})")
        return log

    DEFECTS = (
        ("orders", "orphan customer_id", "customer_exists"),
        ("orders", "orphan product_id", "product_exists"),
        ("orders", "NULL customer_id", "customer_present"),
        ("orders", "quantity = 0", "quantity_valid"),
        ("orders", "negative amount", "amount_non_negative"),
        ("orders", "unknown status 'LOST'", "status_known"),
        ("orders", "order_date in the future", "order_date_not_in_future"),
        ("orders", "NULL order_date", "partition_order_date_not_null"),
        ("orders", "delivered before ordered (warning only)", "delivered_after_ordered"),
        ("customers", "NULL email", "email_present"),
        ("customers", "malformed email", "email_format"),
        ("customers", "3-letter country code", "country_is_iso2"),
        ("customers", "signup date in the future (warning only)", "signup_not_in_future"),
        ("products", "price 0.00", "price_positive"),
        ("products", "unknown category 'Toys'", "category_known"),
        ("products", "malformed SKU", "sku_format"),
    )

    def _defective_row(self, table: str, defect: str) -> dict:
        if table == "orders":
            row = self._order(self._next_id("orders", 100001))
            patch = {
                "orphan customer_id": {"customer_id": 900000 + self.rng.randint(1, 999)},
                "orphan product_id": {"product_id": 9000 + self.rng.randint(1, 99)},
                "NULL customer_id": {"customer_id": None},
                "quantity = 0": {"quantity": 0, "amount": Decimal("0.00")},
                "negative amount": {"amount": Decimal("-49.99")},
                "unknown status 'LOST'": {"status": "LOST"},
                "order_date in the future": {"order_date": self.today + timedelta(days=45)},
                "NULL order_date": {"order_date": None},
                "delivered before ordered (warning only)": {
                    "status": "DELIVERED", "delivered_date": self.today - timedelta(days=3)},
            }[defect]
        elif table == "customers":
            row = self._customer(self._next_id("customers", 1))
            patch = {
                "NULL email": {"email": None},
                "malformed email": {"email": "no-at-sign.example.com"},
                "3-letter country code": {"country_code": "IND"},
                "signup date in the future (warning only)": {"signup_date": self.today + timedelta(days=10)},
            }[defect]
        else:
            row = self._product(self._next_id("products", 1))
            patch = {
                "price 0.00": {"unit_price": Decimal("0.00")},
                "unknown category 'Toys'": {"category": "Toys"},
                "malformed SKU": {"sku": "SKU12"},
            }[defect]
        return {**row, **patch}

    def inject_defect(self, table: str, defect: str) -> str:
        row = self._defective_row(table, defect)
        self._insert(table, row)
        key = row[PRIMARY_KEYS[table]]
        rule = next(r for t, d, r in self.DEFECTS if t == table and d == defect)
        message = f"{table} {key}: {defect} (breaks {rule})"
        if "warning only" not in defect:
            self.defects[(table, key)] = defect
        return message

    def inject_bad_records(self, count: int = 6) -> list[str]:
        return [self.inject_defect(t, d) for t, d, _ in self.rng.sample(self.DEFECTS, min(count, len(self.DEFECTS)))]

    def bad_batch(self, count: int = 24) -> list[str]:
        """A broken upstream deploy: a burst of orders that are mostly invalid."""
        kinds = ["negative amount", "unknown status 'LOST'", "quantity = 0", "orphan customer_id"]
        return [self.inject_defect("orders", self.rng.choice(kinds)) for _ in range(count)]

    def fix_defects(self) -> list[str]:
        """Correct every injected defect at the source (each fix is a new row version)."""
        log = []
        valid_customer = self.db.query("SELECT MIN(customer_id) AS c FROM sales.customers")[0]["c"]
        valid_product = self.db.query("SELECT MIN(product_id) AS p, MIN(unit_price) AS price FROM sales.products "
                                      "WHERE unit_price > 0")[0]
        for (table, key), defect in sorted(self.defects.items()):
            if table == "orders":
                changes = {"customer_id": valid_customer, "product_id": valid_product["p"], "quantity": 1,
                           "amount": Decimal(str(valid_product["price"])).quantize(Decimal("0.01")),
                           "status": "PLACED", "order_date": self.today, "delivered_date": None}
            elif table == "customers":
                changes = {"email": f"fixed.customer{key}@example.com", "country_code": "IN"}
            else:
                changes = {"unit_price": Decimal("19.99"), "category": "Home", "sku": f"SKU-{10000 + key:05d}"}
            self._update(table, key, changes)
            log.append(f"FIX {table} {key}: {defect}")
        self.defects.clear()
        return log
