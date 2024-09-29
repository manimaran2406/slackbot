# Query Finder functionality modal and processing
from slack_bot.app_init import app
import logging
from services.s3_handler import S3Handler  # Updated import
from utils.slack_helper import post_link_to_slack

logger = logging.getLogger(__name__)

def query_finder_func(ack, body, client):
    ack()
    client.views_update(
        view_id=body['view']['id'],
        hash=body['view']['hash'],
        view={
            "type": "modal",
            "callback_id": "query_finder_function",
            "title": {"type": "plain_text", "text": "Query Finder"},
            "blocks": [
                {
                    "type": "input",
                    "block_id": "columns_input_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "columns_input",
                        "placeholder": {"type": "plain_text", "text": "Enter column names separated by commas"}
                    },
                    "label": {"type": "plain_text", "text": "Column Names"}
                }
            ],
            "submit": {"type": "plain_text", "text": "Find Queries"}
        }
    )


@app.view('query_finder_function')
def handle_query_finder_submission(ack, body, client):
    ack()
    user_info = body['user']
    user_id = user_info['id']
    response = client.conversations_open(users=user_id)
    dm_channel = response['channel']['id']

    # Retrieve column input
    columns_input = body['view']['state']['values']['columns_input_block']['columns_input']['value']
    input_columns = [col.strip() for col in columns_input.split(",") if col.strip()]

    if input_columns:
        # Build the dynamic SQL query to search for relevant queries
        column_conditions = " UNION ALL ".join([f"SELECT '{col}' AS keyword" for col in input_columns])
        query = f"""
        WITH input_keywords AS (
            {column_conditions}
        ),
        matches AS (
            SELECT
                t.QUERY,
                COUNT(DISTINCT k.keyword) AS matched_keywords_count,
                ARRAY_SIZE(FIELD_NAMES_PRESTO) AS total_columns_in_query
            FROM
                FW_OPERATIONAL_DATA.LQS_QUERY_HISTORY.ETL_QUERY_HISTORY t,
                LATERAL FLATTEN(input => FIELD_NAMES_PRESTO) f
            JOIN input_keywords k ON f.value::STRING ILIKE '%' || k.keyword || '%'
            WHERE t.query_type_presto='SELECT' AND t.state_presto='FINISHED'
            GROUP BY t.QUERY, FIELD_NAMES_PRESTO
        )
        SELECT
            QUERY,
            matched_keywords_count,
            total_columns_in_query,
            (matched_keywords_count / total_columns_in_query) AS match_score
        FROM matches
        ORDER BY matched_keywords_count DESC, match_score DESC
        LIMIT 10;
        """
        try:
            # Initialize S3Handler
            s3_handler = S3Handler(AWS_BUCKET_NAME)

            # Execute Snowflake query and save results to S3
            results, columns = execute_snowflake_query(query)
            presigned_url = s3_handler.upload_file(results, columns)

            # Send results to the user via Slack
            if presigned_url:
                post_link_to_slack(dm_channel,
                                   "Your requested queries are ready. Click the link below to download the results. The link will expire in 30 minutes.",
                                   'Download Queries', presigned_url)
            else:
                client.chat_postMessage(
                    channel=dm_channel,
                    text="Failed to generate query results."
                )
        except Exception as e:
            logger.error(f"Error generating queries: {e}")
            client.chat_postMessage(
                channel=dm_channel,
                text="An error occurred while processing your query. Please try again later."
            )
    else:
        client.chat_postMessage(
            channel=dm_channel,
            text="Please provide at least one column name to search for relevant queries."
        )
