CREATE VIEW TXN_STREAMdatabricks_trial.poc_bronze.event_log_bronze AS SELECT * FROM event_log("c512631a-e2f3-47c0-99a1-42f2980fdff4");


--Replace <event-log-path> with the event log location.

CREATE  VIEW TXN_STREAMdatabricks_trial.poc_bronze.latest_update AS 
SELECT origin.update_id AS id FROM TXN_STREAMdatabricks_trial.poc_bronze.event_log_bronze WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1;


-- /*Lineage */
SELECT
  details:flow_definition.output_dataset as output_dataset,
  details:flow_definition.input_datasets as input_dataset
FROM
  TXN_STREAMdatabricks_trial.poc_bronze.event_log_bronze ,
  TXN_STREAMdatabricks_trial.poc_bronze.latest_update
WHERE
  event_type = 'flow_definition'
  AND
  origin.update_id = latest_update.id


--   /*Queued tasks */

SELECT
  timestamp,
  get_json_object(details, '$.cluster_resources.avg_num_queued_tasks')   as queue_size
FROM
  TXN_STREAMdatabricks_trial.poc_bronze.event_log_bronze ,
  TXN_STREAMdatabricks_trial.poc_bronze.latest_update
WHERE
  event_type = 'cluster_resources'
  AND
  origin.update_id = latest_update.id ;

/*utilization*/

SELECT
  timestamp,
  details:cluster_resources.avg_task_slot_utilization as utilization
FROM
  TXN_STREAMdatabricks_trial.poc_bronze.event_log_bronze ,
  TXN_STREAMdatabricks_trial.poc_bronze.latest_update
WHERE
  event_type = 'cluster_resources'
  AND
  origin.update_id = latest_update.id ;




/*user actions */

SELECT 
timestamp, 
details:user_action:action, 
details:user_action:user_name 
FROM event_log_bronze 
WHERE event_type = 'user_action'



/*backlog*/

SELECT
  timestamp,
  Double(details:flow_progress.metrics.backlog_bytes) as backlog
FROM
  TXN_STREAMdatabricks_trial.poc_bronze.event_log_bronze ,
  TXN_STREAMdatabricks_trial.poc_bronze.latest_update
WHERE
  event_type ='flow_progress'
  AND
  origin.update_id = latest_update.id ;



SELECT * FROM TXN_STREAMdatabricks_trial.poc_bronze.event_log_bronze where  event_type = 'cluster_resources'

