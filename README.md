# Apache Calcite Tutorial

This repository contains code which demonstrate the use of Apache Calcite as a library for SQL query planning.

First part of the tutorial can be found [here](https://medium.com/@mpathirage/query-planning-with-apache-calcite-part-1-fe957b011c36).

To try the planner:
1) ./gradlew build
2) ./gradlew idea can generate intellij project.
2) run main() inside SimpleQueryPlanner. It will output the logical plan.
3) change the query to see other plans generated.
