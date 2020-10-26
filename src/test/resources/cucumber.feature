Feature: Cucumber demo
  Scenario: run test job
    Given spark format: "jdbc" for table: "users" has data:
      | first_name STRING | last_name STRING | age INT |
      | John              | Petrovich        | 17      |
      | Henry             | Johnson          | 18      |
      | Harry             | Potter           | 19      |

    When test job is started

    Then spark format: "jdbc" for table: "adults" wrote data as "Adults":
      | full_name     | age |
      | Henry Johnson | 18  |
      | Harry Potter  | 19  |