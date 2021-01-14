package com.am.ndo.helper

import java.sql.Timestamp

case class Audit(program: String, user_id: String, app_id: String, start_time: String, app_duration: String, status: String)

case class AuditSourceTable(table_name: String, snapNbr: Long)
case class AuditTargetTable(run_id: Long, table_name: String, snapNbr: Long)

case class Variance(tableName: String, partitionColumn: String, partitionValue: String, columnName: String,
                    operationDescription: String, operation: String, operationValue: Double, previous_operation_value: Double,
                    percentageVariance: Double, threshold: Double, isThresholdCrossed: String, subjectArea: String)