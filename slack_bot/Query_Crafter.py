import boto3
import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from io import StringIO
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from pyhive import presto
from requests.exceptions import RequestException, Timeout, ConnectionError
from ssl import SSLError
import requests

# Declare the variables globally at the top of the script
placement_id = None
start_date = None
end_date = None
action_value=None
network_id=None

# S3 Bucket and `.env` file setup
BUCKET_NAME = 'slackflake-credentials'
ENV_FILE_KEY = '.env'

# Initialize the S3 client
s3 = boto3.client('s3')

# Try to load the `.env` file from S3
try:
    s3_object = s3.get_object(Bucket=BUCKET_NAME, Key=ENV_FILE_KEY)
    env_content = s3_object['Body'].read().decode('utf-8')

    # Load the environment variables from the `.env` content
    load_dotenv(stream=StringIO(env_content))

except s3.exceptions.NoSuchKey:
    print(f".env file not found in S3 bucket {BUCKET_NAME}")
    sys.exit(1)
except Exception as e:
    print(f"Error loading .env file from S3: {e}")
    sys.exit(1)

# Verify that the SLACK_BOT_TOKEN is loaded properly
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
if not SLACK_BOT_TOKEN:
    print("SLACK_BOT_TOKEN is not set in the environment. Please check the .env file.")
    sys.exit(1)

SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN")
if not SLACK_APP_TOKEN:
    print("SLACK_APP_TOKEN is not set in the environment. Please check the .env file.")
    sys.exit(1)

# Initialize your Slack app with the token from the environment
app = App(token=SLACK_BOT_TOKEN)
def get_session_token(user_name, password):
    """Fetches a session token using the provided username and password."""
    try:
        response = requests.post(
            'https://tokens.fw1.aws.fwmrm.net:8080/v1/apply_token',
            data=json.dumps({"user_name": user_name, "password": password}),
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        response.raise_for_status()
        resp_json = response.json()
        token = resp_json.get('token')
        if not token:
            raise ValueError("Failed to obtain token from the response")
        return token
    except (RequestException, Timeout, ConnectionError, SSLError) as e:
        print(f'Error obtaining token: {e}')
        sys.exit(1)
    except ValueError as ve:
        print(f'Value Error: {ve}')
        sys.exit(1)
    except json.JSONDecodeError as je:
        print(f'Error decoding JSON response: {je}')
        sys.exit(1)
    except Exception as e:
        print(f'An unexpected error occurred while obtaining the token: {e}')
        sys.exit(1)

def create_session(token):
    """Creates a requests session with the given token."""
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}'})
    return session

def execute_presto_query(query, connection):
    """Executes the provided Presto query and returns the results."""
    print("Executing Presto query...")
    cursor = connection.cursor()
    cursor.execute(query)
    
    if cursor.description:
        columns = [desc[0] for desc in cursor.description]
    else:
        raise ValueError("No column description found in query result")
    
    results = cursor.fetchall()
    cursor.close()
    
    return {"columns": columns, "results": results}

def main(query):
    """Main function to handle the execution of the Presto query."""
    user_name = 'svx-script-runner'
    password = 'B8zT?VdNh7KxX4v5'
    connection = None
    try:
        token = get_session_token(user_name, password)
        session = create_session(token)
        connection = presto.connect(
            host='presto-gateway.presto.fw1.aws.fwmrm.net',
            port=8080,
            username=user_name,
            protocol='https',
            requests_session=session
        )
        result = execute_presto_query(query, connection)
        return result
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
        return None
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception as e:
                print(f'Error closing connection: {e}')

# Program Starts From Here
def handle_help_command(ack, body, client):
    """Handles the /help command by showing a modal view."""
    ack()
    try:
        with open('Payloads/home.json', 'r') as f:
            data = json.load(f)
            blocks = data.get('blocks')
            if not blocks or not isinstance(blocks, list):
                raise ValueError("Invalid Block Kit structure in payload.json")
    except FileNotFoundError:
        client.chat_postMessage(
            channel=body["channel_id"],
            text="Error: payload.json file not found."
        )
        return
    except (json.JSONDecodeError, ValueError) as e:
        client.chat_postMessage(
            channel=body["channel_id"],
            text=f"Error: {str(e)}"
        )
        return
            # Define the modal view with a submit button
    view = {
        "type": "modal",
        "callback_id": "home_modal",  
        "title": {
            "type": "plain_text",
            "text": "SlakeFlake"
        },
        "close": {
            "type": "plain_text",
            "text": "Close"
        },
        "submit": {
            "type": "plain_text",
            "text": "Submit"
        },
        "blocks": blocks
        
    }

    client.views_open(
        trigger_id=body["trigger_id"],
        view=view
    )

@app.view("home_modal")
def handle_modal_submission(ack, body, client):
    """Handles the submission of the home modal view."""
    ack()
    print(body)
    Check=body['view']['state']['values']['ShwCf']['radio_buttons-action']['selected_option']['value']
    if(Check =="value-2"):
        # Read the payload.json file
        try:
            with open('Payloads/payload.json', 'r') as f:
                data = json.load(f)
                blocks = data.get('blocks')
                if not blocks or not isinstance(blocks, list):
                    raise ValueError("Invalid Block Kit structure in payload.json")
        except FileNotFoundError:
            client.chat_postMessage(
                channel=body["channel_id"],
                text="Error: payload.json file not found."
            )
            return
        except (json.JSONDecodeError, ValueError) as e:
            client.chat_postMessage(
                channel=body["channel_id"],
                text=f"Error: {str(e)}"
            )
            return

        # Define the modal view with a submit button
        view = {
            "type": "modal",
            "callback_id": "help_modal",  
            "title": {
                "type": "plain_text",
                "text": "LQS Query Generator"
            },
            "close": {
                "type": "plain_text",
                "text": "Close"
            },
            "submit": {
                "type": "plain_text",
                "text": "Submit"
            },
            "blocks": blocks
        }

        client.views_open(
            trigger_id=body["trigger_id"],
            view=view
        )
    else :
        # Define the modal view with a submit button
        view={
            "type": "modal",
            "callback_id": "query_generator_view",
            "title": {"type": "plain_text", "text": "Query Shaper"},
            "blocks": [
                {
                    "type": "input",
                    "block_id": "placement_id_input",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "placement_id",
                        "placeholder": {"type": "plain_text", "text": "Enter Placement ID"}
                    },
                    "label": {"type": "plain_text", "text": "Placement ID"}
                },
                {
                    "type": "input",
                    "block_id": "network_id_input",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "network_id",
                        "placeholder": {"type": "plain_text", "text": "Enter Network ID"}
                    },
                    "label": {"type": "plain_text", "text": "Network ID"}
                },
                {
                    "type": "input",
                    "block_id": "selected_columns_block",
                    "element": {
                        "type": "multi_static_select",
                        "action_id": "selected_columns_select",
                        "placeholder": {"type": "plain_text", "text": "Select columns"},
                        "options": [
                            {"text": {"type": "plain_text", "text": "transaction__request"}, "value": "transaction__request"},
                            {"text": {"type": "plain_text", "text": "transaction__request__slots__time_position_class"}, "value": "transaction__request__slots__time_position_class"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__network_id"}, "value": "transaction__request__context__network_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__profile_id"}, "value": "transaction__request__context__profile_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__standard_genre_ids"}, "value": "transaction__request__context__standard_genre_ids"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__network__network_id"}, "value": "transaction__request__context__network__network_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__standard_programmer_id"}, "value": "transaction__request__context__standard_programmer_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__site_section_id"}, "value": "transaction__request__context__site_section_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__advertisements__placement_id"}, "value": "transaction__request__advertisements__placement_id"},
                            {"text": {"type": "plain_text", "text": "transaction__acks__creative_id"}, "value": "transaction__acks__creative_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__visitor__user_agent_device_type"}, "value": "transaction__request__visitor__user_agent_device_type"},
                            {"text": {"type": "plain_text", "text": "transaction__request__visitor__user_agent"}, "value": "transaction__request__visitor__user_agent"},
                            {"text": {"type": "plain_text", "text": "transaction__request__advertisements__creative_id"}, "value": "transaction__request__advertisements__creative_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__visitor__postal_code"}, "value": "transaction__request__visitor__postal_code"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__network__geo_zipcode_visibility"}, "value": "transaction__request__context__network__geo_zipcode_visibility"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__standard_channel_id"}, "value": "transaction__request__context__standard_channel_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__content_rating_id"}, "value": "transaction__request__context__content_rating_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__asset_id"}, "value": "transaction__request__context__asset_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__time_record__total"}, "value": "transaction__request__time_record__total"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__standard_endpoint_id"}, "value": "transaction__request__context__standard_endpoint_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__backend_filtration_reason"}, "value": "transaction__request__backend_filtration_reason"},
                            {"text": {"type": "plain_text", "text": "transaction__request__visitor__user_id"}, "value": "transaction__request__visitor__user_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__standard_brand_id"}, "value": "transaction__request__context__standard_brand_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__visitor__postal_code_id"}, "value": "transaction__request__visitor__postal_code_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__visitor__standard_device_type_ids"}, "value": "transaction__request__visitor__standard_device_type_ids"},
                            {"text": {"type": "plain_text", "text": "transaction__request__audience_item__audience_item_id"}, "value": "transaction__request__audience_item__audience_item_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__standard_endpoint_owner_id"}, "value": "transaction__request__context__standard_endpoint_owner_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__context__custom_asset_id"}, "value": "transaction__request__context__custom_asset_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__errors__site_section_id"}, "value": "transaction__request__errors__site_section_id"},
                            {"text": {"type": "plain_text", "text": "transaction__request__inventory_group__group_id"}, "value": "transaction__request__inventory_group__group_id"},
                            {"text": {"type": "plain_text", "text": "transaction__acks__ad_impression"}, "value": "transaction__acks__ad_impression"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Selected Columns"}
                },
                {
                    "type": "actions",
                    "block_id": "date_picker_block",
                    "elements": [
                        {
                            "type": "datepicker",
                            "placeholder": {"type": "plain_text", "text": "Select start date"},
                            "action_id": "start_date_picker"
                        },
                        {
                            "type": "datepicker",
                            "placeholder": {"type": "plain_text", "text": "Select end date"},
                            "action_id": "end_date_picker"
                        }
                    ]
                }
            ],
            "submit": {"type": "plain_text", "text": "Submit"}
        }

        client.views_open(
            trigger_id=body["trigger_id"],
            view=view
        )
@app.view("query_generator_view")
def handle_query_submission(ack, body, client):
    ack(response_action="clear")
    user_info = body["user"]
    user_id = user_info["id"]
    user_name = user_info["username"]
    placement_id = body["view"]["state"]["values"]["placement_id_input"]["placement_id"]["value"]
    network_id = body["view"]["state"]["values"]["network_id_input"]["network_id"]["value"]
    selected_columns = body["view"]["state"]["values"]["selected_columns_block"]["selected_columns_select"]["selected_options"]
    start_date = body["view"]["state"]["values"]["date_picker_block"]["start_date_picker"]["selected_date"]
    end_date = body["view"]["state"]["values"]["date_picker_block"]["end_date_picker"]["selected_date"]

    # logger.info(f"Query Generator submission by user: {user_name} ({user_id}) with Placement ID: {placement_id}")
    client.chat_postMessage(
    channel=user_id,
    text=f"Please Wait While Query is executing...."
    )   

    try:
        # QUERY TO BE ADDED QUERY SHAPER
        sql_query = f"""
            SELECT
        tc.id,
        tc.name,
        lct.id AS criteria_type_id,  -- Added alias for clarity
        tc.relation,
        tc.negative,
        CASE
            WHEN lct.criteria_type = 'COUNTRY' AND tc.negative = 0 THEN
                CONCAT('transaction__request__visitor__country_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'COUNTRY' AND tc.negative = 1 THEN
                CONCAT('transaction__request__visitor__country_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'STATE' AND tc.negative = 0 THEN
                CONCAT('transaction__request__visitor__state_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'STATE' AND tc.negative = 1 THEN
                CONCAT('transaction__request__visitor__state_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'CITY' AND tc.negative = 0 THEN
                CONCAT('transaction__request__visitor__city_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'CITY' AND tc.negative = 1 THEN
                CONCAT('transaction__request__visitor__city_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'DMA' AND tc.negative = 0 THEN
                CONCAT('transaction__request__visitor__dma_code_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'DMA' AND tc.negative = 1 THEN
                CONCAT('transaction__request__visitor__dma_code_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'USER_AGENT_DEVICE' AND tc.negative = 0 THEN
                CONCAT('transaction__request__visitor__user_agent_device_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'USER_AGENT_DEVICE' AND tc.negative = 1 THEN
                CONCAT('transaction__request__visitor__user_agent_device_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'STANDARD_PUBLISHER' AND tc.negative = 0 THEN
                CONCAT('transaction__request__context__standard_publisher_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'STANDARD_PUBLISHER' AND tc.negative = 1 THEN
                CONCAT('transaction__request__context__standard_publisher_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'STANDARD_APP' AND tc.negative = 0 THEN
                CONCAT('transaction__request__context__standard_app_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'STANDARD_APP' AND tc.negative = 1 THEN
                CONCAT('transaction__request__context__standard_app_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'STANDARD_SITE_DOMAIN' AND tc.negative = 0 THEN
                CONCAT('transaction__request__context__standard_site_domain_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'STANDARD_SITE_DOMAIN' AND tc.negative = 1 THEN
                CONCAT('transaction__request__context__standard_site_domain_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'STANDARD_SUBSCRIPTION_MODEL' AND tc.negative = 0 THEN
                CONCAT('transaction__request__context__standard_content_subscription_model_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'STANDARD_SUBSCRIPTION_MODEL' AND tc.negative = 1 THEN
                CONCAT('transaction__request__context__standard_content_subscription_model_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'STANDARD_CONTENT_CREDENTIAL_STATUS' AND tc.negative = 0 THEN
                CONCAT('transaction__request__context__standard_content_credential_status_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'STANDARD_CONTENT_CREDENTIAL_STATUS' AND tc.negative = 1 THEN
                CONCAT('transaction__request__context__standard_content_credential_status_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'STANDARD_OPERATOR' AND tc.negative = 0 THEN
                CONCAT('transaction__request__visitor__standard_operator_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'STANDARD_OPERATOR' AND tc.negative = 1 THEN
                CONCAT('transaction__request__visitor__standard_operator_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'DAY_PART' AND tc.negative = 0 THEN
                CONCAT('transaction__request__context__standard_content_daypart_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'DAY_PART' AND tc.negative = 1 THEN
                CONCAT('transaction__request__context__standard_content_daypart_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'STANDARD_OS' AND tc.negative = 0 THEN
                CONCAT('transaction__request__visitor__platform_os_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'STANDARD_OS' AND tc.negative = 1 THEN
                CONCAT('transaction__request__visitor__platform_os_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'STANDARD_CONTENT_CHANNEL' AND tc.negative = 0 THEN
                CONCAT('transaction__request__context__standard_channel_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'STANDARD_CONTENT_CHANNEL' AND tc.negative = 1 THEN
                CONCAT('transaction__request__context__standard_channel_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'SITE_SECTION' AND tc.negative = 0 THEN
                CONCAT('transaction__request__context__site_section_id IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')
            WHEN lct.criteria_type = 'SITE_SECTION' AND tc.negative = 1 THEN
                CONCAT('transaction__request__context__site_section_id NOT IN (', 
                    array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                    ')')

            WHEN lct.criteria_type = 'ASSET' AND tc.negative = 0 THEN
            CONCAT(
                'video_cro_asset_id IN (', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                ') OR distributor_asset_id IN (', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                ')'
            )
            WHEN lct.criteria_type = 'ASSET' AND tc.negative = 1 THEN
            CONCAT(
                'video_cro_asset_id NOT IN (', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                ') AND distributor_asset_id NOT IN (', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                ')'
            )
        WHEN lct.criteria_type = 'POSTAL_CODE' AND tc.negative = 0 THEN  
            CONCAT('cardinality(array_intersect(transaction__request__visitor__postal_code_id, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) > 0')
        WHEN lct.criteria_type = 'POSTAL_CODE' AND tc.negative = 1 THEN  
            CONCAT('cardinality(array_intersect(transaction__request__visitor__postal_code_id, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) = 0')

        WHEN lct.criteria_type = 'POSTAL_CODE_PACKAGE' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__visitor__postal_code_package__postal_code_package_id, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) > 0')
        WHEN lct.criteria_type = 'POSTAL_CODE_PACKAGE' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__visitor__postal_code_package__postal_code_package_id, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) = 0')

        WHEN lct.criteria_type = 'AUDIENCE_ITEM' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__audience_item__audience_item_id, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) > 0')
        WHEN lct.criteria_type = 'AUDIENCE_ITEM' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__audience_item__audience_item_id, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) = 0')

        WHEN lct.criteria_type = 'STANDARD_DEVICE' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__visitor__standard_device_type_ids, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) > 0')
        WHEN lct.criteria_type = 'STANDARD_DEVICE' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__visitor__standard_device_type_ids, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) = 0')

        WHEN lct.criteria_type = 'STANDARD_GENRE' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__context__standard_genre_ids, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) > 0')
        WHEN lct.criteria_type = 'STANDARD_GENRE' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__context__standard_genre_ids, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) = 0')

        WHEN lct.criteria_type = 'STREAM_MODE' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__context__stream_mode_ids, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) > 0')
        WHEN lct.criteria_type = 'STREAM_MODE' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__context__stream_mode_ids, ARRAY[', 
                array_join(array_agg(DISTINCT tcia.criteria_value ORDER BY tcia.criteria_value ASC), ', '), 
                '])) = 0')
                
        WHEN lct.criteria_type = 'MKPL_LISTING' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__slots__listing_id, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) > 0')
        WHEN lct.criteria_type = 'MKPL_LISTING' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__slots__listing_id, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) = 0')

        -- For AUCTION_NETWORK
        WHEN lct.criteria_type = 'AUCTION_NETWORK' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__auction_network_contexts__auction_network_id, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) > 0')
        WHEN lct.criteria_type = 'AUCTION_NETWORK' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__auction_network_contexts__auction_network_id, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) = 0')

        -- For DEVICE_TYPE
        WHEN lct.criteria_type = 'DEVICE_TYPE' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__visitor__standard_device_type_ids, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) > 0')
        WHEN lct.criteria_type = 'DEVICE_TYPE' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__visitor__standard_device_type_ids, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) = 0')

        -- For STANDARD_IAB_CATEGORY
        WHEN lct.criteria_type = 'STANDARD_IAB_CATEGORY' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__context__standard_iab_category_ids, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) > 0')
        WHEN lct.criteria_type = 'STANDARD_IAB_CATEGORY' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__context__standard_iab_category_ids, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) = 0')

        -- For ASSET_GROUP
        WHEN lct.criteria_type = 'ASSET_GROUP' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__inventory_group__group_id, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) > 0')
        WHEN lct.criteria_type = 'ASSET_GROUP' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__inventory_group__group_id, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) = 0')

        -- For SITE_SECTION_GROUP
        WHEN lct.criteria_type = 'SITE_SECTION_GROUP' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__inventory_group__group_id, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) > 0')
        WHEN lct.criteria_type = 'SITE_SECTION_GROUP' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__inventory_group__group_id, array[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) = 0')

        -- For STANDARD_ENVIRONMENT
        WHEN lct.criteria_type = 'STANDARD_ENVIRONMENT' AND tc.negative = 0 THEN
            CONCAT('transaction__request__visitor__standard_environment_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'STANDARD_ENVIRONMENT' AND tc.negative = 1 THEN
            CONCAT('transaction__request__visitor__standard_environment_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

        -- For ENDPOINT_OWNER
        WHEN lct.criteria_type = 'ENDPOINT_OWNER' AND tc.negative = 0 THEN
            CONCAT('transaction__request__context__standard_endpoint_owner_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'ENDPOINT_OWNER' AND tc.negative = 1 THEN
            CONCAT('transaction__request__context__standard_endpoint_owner_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

        -- For IP_ENABLED_AUDIENCE
        WHEN lct.criteria_type = 'IP_ENABLED_AUDIENCE' AND tc.negative = 0 THEN
            CONCAT('transaction__request__context__ip_enabled_audience_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'IP_ENABLED_AUDIENCE' AND tc.negative = 1 THEN
            CONCAT('transaction__request__context__ip_enabled_audience_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

        -- For INVENTORY_LOCATION
        WHEN lct.criteria_type = 'INVENTORY_LOCATION' AND tc.negative = 0 THEN
            CONCAT('transaction__request__context__inventory_location_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'INVENTORY_LOCATION' AND tc.negative = 1 THEN
            CONCAT('transaction__request__context__inventory_location_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

        -- For STANDARD_CONTENT_TERRITORY
        WHEN lct.criteria_type = 'STANDARD_CONTENT_TERRITORY' AND tc.negative = 0 THEN
            CONCAT('transaction__request__context__standard_content_territory_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'STANDARD_CONTENT_TERRITORY' AND tc.negative = 1 THEN
            CONCAT('transaction__request__context__standard_content_territory_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

        -- For STANDARD_CONTENT_DAYPART
        WHEN lct.criteria_type = 'STANDARD_CONTENT_DAYPART' AND tc.negative = 0 THEN
            CONCAT('transaction__request__context__standard_content_daypart_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'STANDARD_CONTENT_DAYPART' AND tc.negative = 1 THEN
            CONCAT('transaction__request__context__standard_content_daypart_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

        -- For ISP
        WHEN lct.criteria_type = 'ISP' AND tc.negative = 0 THEN
            CONCAT('transaction__request__visitor__isp_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'ISP' AND tc.negative = 1 THEN
            CONCAT('transaction__request__visitor__isp_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
                

        -- Standard Content Language
        WHEN lct.criteria_type = 'STANDARD_CONTENT_LANGUAGE' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__context__standard_language_ids, ARRAY[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) > 0')
        WHEN lct.criteria_type = 'STANDARD_CONTENT_LANGUAGE' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__context__standard_language_ids, ARRAY[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) = 0')

        -- Inventory Owner
        WHEN lct.criteria_type = 'INVENTORY_OWNER' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__slots__carriage_inventory_owner_id, ARRAY[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) > 0')
        WHEN lct.criteria_type = 'INVENTORY_OWNER' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__slots__carriage_inventory_owner_id, ARRAY[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) = 0')

        -- Inventory Package
        WHEN lct.criteria_type = 'INVENTORY_PACKAGE' AND tc.negative = 0 THEN
            CONCAT('cardinality(array_intersect(transaction__request__slots__network__network_execution_ctx__inventory_package_ids, ARRAY[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) > 0')
        WHEN lct.criteria_type = 'INVENTORY_PACKAGE' AND tc.negative = 1 THEN
            CONCAT('cardinality(array_intersect(transaction__request__slots__network__network_execution_ctx__inventory_package_ids, ARRAY[', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                '])) = 0')

        -- Inventory Source
        WHEN lct.criteria_type = 'INVENTORY_SOURCE' AND tc.negative = 0 THEN
            CONCAT('transaction__request__bidding_context__bid_request__inventory_source IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'INVENTORY_SOURCE' AND tc.negative = 1 THEN
            CONCAT('transaction__request__bidding_context__bid_request__inventory_source NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

        -- Standard Content Rating
        WHEN lct.criteria_type = 'STANDARD_CONTENT_RATING' AND tc.negative = 0 THEN
            CONCAT('transaction__request__context__content_rating_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'STANDARD_CONTENT_RATING' AND tc.negative = 1 THEN
            CONCAT('transaction__request__context__content_rating_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

        -- Standard Content Form
        WHEN lct.criteria_type = 'STANDARD_CONTENT_FORM' AND tc.negative = 0 THEN
            CONCAT('transaction__request__context__content_form_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'STANDARD_CONTENT_FORM' AND tc.negative = 1 THEN
            CONCAT('transaction__request__context__content_form_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

        -- TV Network
        WHEN lct.criteria_type = 'TV_NETWORK' AND tc.negative = 0 THEN
            CONCAT('transaction__request__context__tv_network_id IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')
        WHEN lct.criteria_type = 'TV_NETWORK' AND tc.negative = 1 THEN
            CONCAT('transaction__request__context__tv_network_id NOT IN (', 
                array_join(array_agg(distinct tcia.criteria_value order by tcia.criteria_value asc), ','), 
                ')')

            ELSE NULL
        END AS dynamic_sql
    FROM
        oltp.fwmrm_oltp.targeting_criteria tc
    LEFT JOIN
        oltp.fwmrm_oltp.targeting_criteria_assignment tca ON tc.id = tca.child_criteria_id
    LEFT JOIN
        oltp.fwmrm_oltp.targeting_criteria_item_assignment tcia ON tcia.targeting_criteria_id = tc.id
    LEFT JOIN
        oltp.fwmrm_oltp.lu_criteria_type lct ON lct.id = tcia.criteria_type_id
    WHERE
        tc.root_criteria_id = (
            SELECT 
                criteria_id
            FROM 
                oltp.fwmrm_oltp.ad_tree_node
            WHERE 
                id = {placement_id}
        )

    GROUP BY tc.id, tc.name, lct.criteria_type, tc.relation, tc.negative, lct.id
    ORDER BY tc.id """

        result = main(sql_query)
        # Extracting the dynamic_sql column
        dynamic_sql_list = [row[5] for row in result['results'] if row[5] is not None]

        # Concatenate with 'AND'
        concatenated_sql = ' AND '.join(dynamic_sql_list)

        client.chat_postMessage(
            channel=user_id,
            text=f"Generated Query: ```WHERE {concatenated_sql}```"
        )
        client.chat_postMessage(
            channel=user_id,
            text=f"Query Generated Sucessfully"
        )
        # logger.info(f"Generated Query sent to user: {user_name} ({user_id})")
    except Exception as e:
        logger.exception(f"An error occurred while processing the query: {str(e)}")
        client.chat_postMessage(
            channel=user_id,
            text="An error occurred while processing your request. Please try again later."
        )

@app.view("help_modal")
def handle_modal_submission(ack, body, client):
    """Handles the submission of the help modal view."""
    ack()

    # Extract submitted values from the body
    submitted_data = body.get('view', {}).get('state', {}).get('values', {})

    query_type_select = next((value for key, value in submitted_data.items() 
                              if 'query_type_select-action' in value), None)

    if query_type_select:
        selected_value = query_type_select.get('query_type_select-action', {}).get('selected_option', {}).get('value')
        
        if selected_value == 'ad_request':
            handle_ad_request(submitted_data, body, client)
        elif selected_value == 'transactions':
            handle_transactions(submitted_data,body, client)
        elif selected_value == 'ack':
            handle_ack(submitted_data, body, client)

def handle_ad_request(submitted_data, body, client):
    """Handles the Ad Request selection."""
    print("Ad_req")

    print(submitted_data)


    # Extract values
    placement_id = submitted_data['ZYQQ8']['placement_id-input']['value']
    network_id = submitted_data['6DbRH']['plain_text_input-action']['value']
    query_type_value = submitted_data['1C2nK']['query_type_select-action']['selected_option']['value']
    start_date = submitted_data['wDahJ']['start_datepicker-action']['selected_date']
    end_date = submitted_data['wDahJ']['end_datepicker-action']['selected_date']
    action_value = submitted_data['bJvAH']['actionId-0']['selected_option']['value']

    query = f'''
    SELECT count(1) as "Total Ad Requests" 
    FROM fw.default.transaction 
    WHERE request_event_date >= timestamp '{start_date} 00:00:00' 
    AND request_event_date <= timestamp '{end_date} 23:59:59' 
    AND transaction__request__is_first_request = true 
    AND video_cro_network_id IN ({network_id}) 
    AND cardinality(array_intersect(transaction__request__advertisements__placement_id, ARRAY[{placement_id}])) > 0 
    ORDER BY "Total Ad Requests" DESC 
    LIMIT 10000
    '''
    # Construct the pqm_link
    pqm_link = (
        f"https://pqm.fwmrm.net/d/ZMbWHksWk/ad-osi-monitor-placement-delivery-detail?"
        f"orgId=1&var-ad_tree_node_id={placement_id}&var-granularity=30m"
        f"&var-datasource=ad_osi__ops_feed&var-change_history_table=placement_change_history"
        f"&from=now-30d&to=now"
    )
    client.chat_postMessage(
        channel=body["user"]["id"],
        text="You selected an Ad Request! Running the query..."
    )
    print(action_value)
    # Check if the action value is 'value-0' to open the modal
    if action_value == 'value-0':
        client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": "query_modal",
                "title": {
                    "type": "plain_text",
                    "text": "Query Result"
                },
                "blocks": [
                    {
                        "type": "section",
                        "block_id": "result_section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"Retrieved Query successfully.\n\n*Result:*\n{query}"
                        }
                    },
                    {
                        "type": "section",
                        "block_id": "link_section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Here is the PQM link:*\n<{pqm_link}|Click here to view PQM>"
                        }
                    }
                ]
            }
        )
    else:
        # Proceed to execute the query
        if query:
            print("Executing query:", query)
            result = main(query)
            
            if result:
                # Extract columns and results from the result object
                columns = result.get("columns", [])
                results = result.get("results", [])
                
                # Format the result for Slack display
                if len(results) > 0 and len(columns) > 0:
                    formatted_result = f"{columns[0]}: {results[0][0]}"
                else:
                    formatted_result = "No results found."

                # Send a Slack message with the query result
                client.chat_postMessage(
                    channel=body["user"]["id"],  # Send message to the user who triggered the action
                    blocks=[
                        {
                            "type": "section",
                            "block_id": "result_section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"Query executed successfully.\n\n*Result:*\n{formatted_result}"
                            }
                        },
                        {
                            "type": "section",
                            "block_id": "link_section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Here is the PQM link:*\n<{pqm_link}|Click here to view PQM>"
                            }
                        }
                    ]
                )
            else:
                # Handle case when no result is returned
                client.chat_postMessage(
                    channel=body["user"]["id"],
                    text="Query executed, but no results were found."
                )
        else:
            # Handle case when no query is provided
            client.chat_postMessage(
                channel=body["user"]["id"],
                text="No query was provided. Please submit a valid query."
            )


def handle_transactions(submitted_data,body, client):
    """Handles the Transactions selection."""
    
    print("trans")

    # Extract values
    placement_id = submitted_data['ZYQQ8']['placement_id-input']['value']
    network_id = submitted_data['6DbRH']['plain_text_input-action']['value']
    query_type_value = submitted_data['1C2nK']['query_type_select-action']['selected_option']['value']
    start_date = submitted_data['wDahJ']['start_datepicker-action']['selected_date']
    end_date = submitted_data['wDahJ']['end_datepicker-action']['selected_date']
    action_value = submitted_data['bJvAH']['actionId-0']['selected_option']['value']

    print(submitted_data)

    query = f'''select
    date_format(utc_timestamp_to_local(request_event_date, timezone_of_network({network_id})), '%Y-%m-%d %H:%i:%s') as "Date Time", --network id is used for time zone
    transaction_id as "Transaction ID",
    count(1) as "Total Ad Requests"
    from transaction
    where request_event_date >= timestamp '{start_date} 00:00:00'
    and request_event_date <= timestamp '{end_date} 23:59:59'
    and transaction_request_is_first_request = true
    and placement_id = {placement_id}
    group by 1,2
    order by "Total Ad Requests" desc
    limit 10000'''


    # Construct the pqm_link
    pqm_link = (
        f"https://pqm.fwmrm.net/d/ZMbWHksWk/ad-osi-monitor-placement-delivery-detail?"
        f"orgId=1&var-ad_tree_node_id={placement_id}&var-granularity=30m"
        f"&var-datasource=ad_osi__ops_feed&var-change_history_table=placement_change_history"
        f"&from=now-30d&to=now"
    )

    client.chat_postMessage(
        channel=body["user"]["id"],
        text="You selected transactions! Running the query..."
    )

    if action_value == 'value-0':
        client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": "query_modal",
                "title": {
                    "type": "plain_text",
                    "text": "Query Result"
                },
                "blocks": [
                    {
                        "type": "section",
                        "block_id": "result_section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"Retrieved Query successfully.\n\n*Result:*\n{query}"
                        }
                    },
                    {
                        "type": "section",
                        "block_id": "link_section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Here is the PQM link:*\n<{pqm_link}|Click here to view PQM>"
                        }
                    }
                ]
            }
        )
    else:
        # Proceed to execute the query
        if query:
            print("Executing query:", query)
            result = main(query)
            
            if result:
                # Extract columns and results from the result object
                columns = result.get("columns", [])
                results = result.get("results", [])
                
                # Format the result for Slack display
                if len(results) > 0 and len(columns) > 0:
                    formatted_result = f"{columns[0]}: {results[0][0]}"
                else:
                    formatted_result = "No results found."

                # Send a Slack message with the query result
                client.chat_postMessage(
                    channel=body["user"]["id"],  # Send message to the user who triggered the action
                    blocks=[
                        {
                            "type": "section",
                            "block_id": "result_section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"Query executed successfully.\n\n*Result:*\n{formatted_result}"
                            }
                        },
                        {
                            "type": "section",
                            "block_id": "link_section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Here is the PQM link:*\n<{pqm_link}|Click here to view PQM>"
                            }
                        }
                    ]
                )
            else:
                # Handle case when no result is returned
                client.chat_postMessage(
                    channel=body["user"]["id"],
                    text="Query executed, but no results were found."
                )
        else:
            # Handle case when no query is provided
            client.chat_postMessage(
                channel=body["user"]["id"],
                text="No query was provided. Please submit a valid query."
            )

def handle_ack(submitted_data, body, client):
    """Handles the Ack selection."""
    print("ack")
    global placement_id, start_date, end_date,pqm_link,action_value,network_id
    # Display the submitted data from the first modal
    print("Submitted data from first modal:")
    print(submitted_data)

    # Extract values
    placement_id = submitted_data['ZYQQ8']['placement_id-input']['value']
    network_id = submitted_data['6DbRH']['plain_text_input-action']['value']
    query_type_value = submitted_data['1C2nK']['query_type_select-action']['selected_option']['value']
    start_date = submitted_data['wDahJ']['start_datepicker-action']['selected_date']
    end_date = submitted_data['wDahJ']['end_datepicker-action']['selected_date']
    action_value = submitted_data['bJvAH']['actionId-0']['selected_option']['value']

    print(f"Placement ID: {placement_id}")

    # Load the payload2.json file for the second modal
    try:
        with open('Payloads/payload2.json', 'r') as f:
            data = json.load(f)
            blocks = data.get('blocks')
            if not blocks or not isinstance(blocks, list):
                raise ValueError("Invalid Block Kit structure in payload2.json")
    except FileNotFoundError:
        client.chat_postMessage(
            channel=body["user"]["id"],
            text="Error: `payload2.json` file not found."
        )
        return
    except (json.JSONDecodeError, ValueError) as e:
        client.chat_postMessage(
            channel=body["user"]["id"],
            text=f"Error: {str(e)}"
        )
        return

    # Define the new modal view with the content from payload2.json
    view = {
        "type": "modal",
        "callback_id": "help_modal_updated",  
        "title": {
            "type": "plain_text",
            "text": "Pick the Columns"
        },
        "close": {
            "type": "plain_text",
            "text": "Close"
        },
        "submit": {
            "type": "plain_text",
            "text": "Submit"
        },
        "blocks": blocks
    }

    # Open the new modal
    client.views_open(
        trigger_id=body["trigger_id"],
        view=view
    )

@app.view("help_modal_updated")
def handle_second_modal_submission(ack, body, client):
    """Handles the submission of the second modal view."""
    ack()

    # Extract submitted values from the body of the second modal
    submitted_data_updated = body.get('view', {}).get('state', {}).get('values', {})

    client.chat_postMessage(
        channel=body["user"]["id"],
        text="You selected ACK! Running the query..."
    )
    

    # Extracting the selected options in checkboxes
    selected_options = submitted_data_updated.get('ack_columns_block', {}).get('ack_columns_select-action', {}).get('selected_options', [])
    selected_values  = [option.get('value') for option in selected_options]

    # Define the mapping for available fields
    available_fields = {
        'request_time': "date_format(utc_to_networklocal(request_event_date, {network_id}), '%Y-%m-%d') request_time",
        'advertisement_placement_id': "advertisement__placement_id",
        'profile': "advertisement__request__context__profile_id profile",
        'select_ads': "count_if(advertisement__request__is_first_request) select_ads",
        'fallback_ads': "count_if(((bitwise_and(advertisement__flags, 32) > 0) AND advertisement__request__is_first_request)) fallback_ads",
        'Impressions': "sum(ad_impression) Impressions",
        'ack_ratio': "((1E0 * sum(ad_impression)) / count_if(advertisement__request__is_first_request)) ack_ratio"
    }

    # Start constructing the query
    sql_query = "SELECT\n"

    # Add the selected fields dynamically
    selected_fields = [available_fields[field] for field in selected_values if field in available_fields]

    # Join the selected fields with a comma and newline
    sql_query += ",\n".join(selected_fields) + "\n"

    # Add the FROM clause
    sql_query += "FROM fw.default.advertisement\n"

    # Add the WHERE clause, substituting the dynamic variables
    sql_query += f"WHERE ((request_event_date >= networklocal_to_utc(TIMESTAMP '{start_date} 00:00:00', {network_id})) AND (request_event_date <= networklocal_to_utc(TIMESTAMP '{end_date} 23:59:59', {network_id})) AND (advertisement__placement_id = {placement_id}))\n"

    print(selected_fields)
    # Set default number of fields
    num_fields = 1

    # Check if specific fields are in selected fields
    if 'advertisement__request__context__profile_id profile' in selected_fields:
        num_fields += 1
    if 'advertisement__placement_id' in selected_fields:
        num_fields += 1
    
    # Generate GROUP BY clause dynamically
    group_by_clause = ", ".join(f"{i+1}" for i in range(num_fields))
    sql_query += f"GROUP BY {group_by_clause}\n"

    # Order by
    sql_query += "ORDER BY request_time ASC\n"

    # Substitute the variables into the query
    sql_query = sql_query.format(
        network_id=network_id, 
        start_date=start_date,  
        end_date=end_date,     
        placement_id=placement_id  
    )

    print(sql_query)

    # Construct the pqm_link
    pqm_link = (
        f"https://pqm.fwmrm.net/d/ZMbWHksWk/ad-osi-monitor-placement-delivery-detail?"
        f"orgId=1&var-ad_tree_node_id={placement_id}&var-granularity=30m"
        f"&var-datasource=ad_osi__ops_feed&var-change_history_table=placement_change_history"
        f"&from=now-30d&to=now"
    )

    print(placement_id)

    if action_value == 'value-0':
        client.views_open(
            trigger_id=body["trigger_id"],
            view={
                "type": "modal",
                "callback_id": "query_modal",
                "title": {
                    "type": "plain_text",
                    "text": "Query Result"
                },
                "blocks": [
                    {
                        "type": "section",
                        "block_id": "result_section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"Retrieved Query successfully.\n\n*Result:*\n{sql_query}"
                        }
                    },
                    {
                        "type": "section",
                        "block_id": "link_section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Here is the PQM link:*\n<{pqm_link}|Click here to view PQM>"
                        }
                    }
                ]
            }
        )
    else:
        # Proceed to execute the query
        if sql_query:
            print("Executing query:", sql_query)
            result = main(sql_query)
            
            if result:
                # Extract columns and results from the result object
                columns = result.get("columns", [])
                results = result.get("results", [])
                
                # Format the result for Slack display
                if len(results) > 0 and len(columns) > 0:
                    formatted_result = f"{columns[0]}: {results[0][0]}"
                else:
                    formatted_result = "No results found."

                # Send a Slack message with the query result
                client.chat_postMessage(
                    channel=body["user"]["id"],  # Send message to the user who triggered the action
                    blocks=[
                        {
                            "type": "section",
                            "block_id": "result_section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"Query executed successfully.\n\n*Result:*\n{formatted_result}"
                            }
                        },
                        {
                            "type": "section",
                            "block_id": "link_section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*Here is the PQM link:*\n<{pqm_link}|Click here to view PQM>"
                            }
                        }
                    ]
                )
            else:
                # Handle case when no result is returned
                client.chat_postMessage(
                    channel=body["user"]["id"],
                    text="Query executed, but no results were found."
                )
        else:
            # Handle case when no query is provided
            client.chat_postMessage(
                channel=body["user"]["id"],
                text="No query was provided. Please submit a valid query."
            )

