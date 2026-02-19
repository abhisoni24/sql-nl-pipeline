## SQL Generation Test Pipeline

This test suite validates SQL generation against 37 specific contracts, ensuring that generated queries are both syntactically correct and logically sound according to the schema.

### How to Run

You can execute the tests using the following commands depending on your use case:

- **Against the default dataset:**

```bash
python pipeline_tests/generation_process/sql/test_sql_generation.py

```

- **Against a specific file:**

```bash
python pipeline_tests/generation_process/sql/test_sql_generation.py \
 --input dataset/current/raw_social_media_queries_20.json

```

- **Verbose mode (prints each failure as it occurs):**

```bash
python pipeline_tests/generation_process/sql/test_sql_generation.py -v

```

> **Note:** The script exits with **code 0** if all checks pass and **code 1** if any checks fail.

---

### What is Checked (37 Contracts)

The validation logic is divided into layers. Every query must pass the **All complexities** check before being validated against its specific type.

| Layer                | Checks Performed                                                                                                                                                                                                                       |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **All complexities** | Required record keys, SQL parseable, known complexity, tables list matches AST, no Python object repr in SQL, all referenced tables exist in schema.                                                                                   |
| **Simple**           | Is `SELECT`, exactly 1 table, no `JOIN`/`UNION`/subquery, `WHERE` columns belong to that table.                                                                                                                                        |
| **Join**             | Exactly 1 `JOIN`, FK pair is valid, `ON` clause uses correct FK columns, join type is legal, selected columns in scope of both tables.                                                                                                 |
| **Advanced**         | Subtype auto-detected; **subquery_where**: `IN` has inner `SELECT` + FK relationship; **subquery_from**: `FROM` is derived subquery; **self_join**: same table twice; **exists_subquery**: `EXISTS` in `WHERE` + correlated condition. |
| **Union**            | Top-level is `UNION` node, both legs are `SELECT`s, same column count, `UNION ALL`/distinct flag matches SQL.                                                                                                                          |
| **Insert**           | Is `INSERT`, table in schema, column count = value count, `id` not in columns, all columns in schema, value types match column types.                                                                                                  |
| **Update**           | Is `UPDATE`, table in schema, exactly 1 `SET` column, composite PK columns not updated, `id` not updated, `WHERE` clause present.                                                                                                      |
| **Delete**           | Is `DELETE`, table in schema, `WHERE` clause present, no subqueries.                                                                                                                                                                   |

---

### Failure Reporting

The failure report groups issues by **check name** and displays up to **3 examples** per failing check. This allows developers to immediately pinpoint regressions introduced by changes to the generator.

Would you like me to help you draft a troubleshooting guide or a README for this test suite?
