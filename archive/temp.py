import sqlglot
from sqlglot import exp 
# this is a file where I will try to generate some SQL queries and learn about the SQLglot library

ast = (
    exp
    .select("a", "b")
    .from_("x")
    .where("b < 4")
    .limit(10)
)
# ast = sqlglot.parse_one("Select * from users where id = 1")
print(ast)

# print(ast.find(exp.Select))

# for node in ast.walk():
#     print(node)