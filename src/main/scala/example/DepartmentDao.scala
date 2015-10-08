// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Notice: Variable binding is vulnerable for SQL injection.
 */
class DepartmentDao(sqlc: SQLContext) {

  def sumBudgets(): Long = sqlc
    .sql("SELECT SUM(budget) FROM departments")
    .map(row => row.getLong(0)).first()

  def numberOfEmployees(): RDD[(Int, Long)] = sqlc
    .sql("SELECT department, COUNT(*) FROM employees GROUP BY department")
    .map(row => (row.getInt(0), row.getLong(1)))

}
