--DROP VIEW admin.v_wlm_queue_state;
/**********************************************************************************************
Purpose: Add WLM_QUEUE_STATE_VW view from Amazon Redshift Tutorial https://docs.aws.amazon.com/redshift/latest/dg/tutorial-wlm-understanding-default-processing.html
History:
2019-02-16 benkim05 Created
**********************************************************************************************/
CREATE VIEW admin.v_wlm_queue_state
AS
SELECT 
    (config.service_class-5) as queue
    , trim (class.condition) AS description
    , config.num_query_tasks AS slots
    , config.query_working_mem AS mem
    , config.max_execution_time AS max_time
    , config.user_group_wild_card AS "user_*"
    , config.query_group_wild_card AS "query_*"
    , state.num_queued_queries queued
    , state.num_executing_queries executing
    , state.num_executed_queries executed
FROM
    STV_WLM_CLASSIFICATION_CONFIG class,
    STV_WLM_SERVICE_CLASS_CONFIG config,
    STV_WLM_SERVICE_CLASS_STATE state
WHERE
    class.action_service_class = config.service_class 
    AND class.action_service_class = state.service_class 
    AND config.service_class > 4
ORDER BY config.service_class;
