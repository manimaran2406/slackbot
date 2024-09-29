from services.snowflake_connector import SnowflakeConnector
from services.s3_handler import S3Handler
from utils.slack_helper import post_message_to_slack

class ReportsHandler:
    def __init__(self, config, snowflake_connector: SnowflakeConnector, s3_handler: S3Handler, logger):
        self.config = config
        self.snowflake_connector = snowflake_connector
        self.s3_handler = s3_handler
        self.logger = logger

    def open_report_modal(self, ack, body, client):
        ack()
        client.views_open(trigger_id=body["trigger_id"], view=self.get_report_modal_view())

    def get_report_modal_view(self):
        return {
            "type": "modal",
            "callback_id": "report_function",
            "title": {"type": "plain_text", "text": "Generate Report"},
            "blocks": [
                {
                    "type": "input",
                    "block_id": "selected_columns_block",
                    "element": {
                        "type": "multi_static_select",
                        "action_id": "selected_columns_select",
                        "placeholder": {"type": "plain_text", "text": "Select columns"},
                        "options": [
                            {"text": {"type": "plain_text", "text": "ID"}, "value": "option_a"},
                            {"text": {"type": "plain_text", "text": "Priority"}, "value": "option_b"},
                            {"text": {"type": "plain_text", "text": "Status"}, "value": "option_c"},
                            {"text": {"type": "plain_text", "text": "Subject"}, "value": "option_d"},
                            {"text": {"type": "plain_text", "text": "Date open"}, "value": "option_e"},
                            {"text": {"type": "plain_text", "text": "Date closed"}, "value": "option_f"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Selected Columns"}
                },
                {
                    "type": "actions",
                    "block_id": "date_picker_block",
                    "elements": [
                        {"type": "datepicker", "action_id": "start_date_picker", "placeholder": {"type": "plain_text", "text": "Select a start date"}},
                        {"type": "datepicker", "action_id": "end_date_picker", "placeholder": {"type": "plain_text", "text": "Select an end date"}}
                    ]
                },
                {
                    "type": "input",
                    "block_id": "organization_block",
                    "element": {
                        "type": "multi_static_select",
                        "action_id": "organization_select",
                        "placeholder": {"type": "plain_text", "text": "Select organization"},
                        "options": [
                            {"text": {"type": "plain_text", "text": "NBC Universal"}, "value": "org_1"},
                            {"text": {"type": "plain_text", "text": "Fox Networks Group"}, "value": "org_2"},
                            {"text": {"type": "plain_text", "text": "Charter Communications"}, "value": "org_3"},
                            {"text": {"type": "plain_text", "text": "Paramount"}, "value": "org_4"},
                            {"text": {"type": "plain_text", "text": "Viacom US"}, "value": "org_5"},
                            {"text": {"type": "plain_text", "text": "Altice USA"}, "value": "org_6"},
                            {"text": {"type": "plain_text", "text": "British Sky Broadcasting"}, "value": "org_7"},
                            {"text": {"type": "plain_text", "text": "Warner Brothers Discovery"}, "value": "org_8"},
                            {"text": {"type": "plain_text", "text": "Channel 4"}, "value": "org_9"},
                            {"text": {"type": "plain_text", "text": "ABC"}, "value": "org_10"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Organization"}
                },
                {
                    "type": "input",
                    "block_id": "priority_block",
                    "element": {
                        "type": "multi_static_select",
                        "action_id": "priority_select",
                        "placeholder": {"type": "plain_text", "text": "Select priority"},
                        "options": [
                            {"text": {"type": "plain_text", "text": "All"}, "value": "all"},
                            {"text": {"type": "plain_text", "text": "Urgent"}, "value": "urgent"},
                            {"text": {"type": "plain_text", "text": "High"}, "value": "high"},
                            {"text": {"type": "plain_text", "text": "Medium"}, "value": "medium"},
                            {"text": {"type": "plain_text", "text": "Low"}, "value": "low"}
                        ]
                    },
                    "label": {"type": "plain_text", "text": "Priority"}
                }
            ],
            "submit": {"type": "plain_text", "text": "Submit"}
        }

    def handle_report_submission(self, ack, body, client):
        ack(response_action="clear")
        user_info = body['user']
        dm_channel = client.conversations_open(users=user_info['id'])['channel']['id']

        state_values = body['view']['state']['values']
        selected_columns = [option['value'] for option in state_values['selected_columns_block']['selected_columns_select']['selected_options']]
        organization_values = [option['value'] for option in state_values['organization_block']['organization_select']['selected_options']]
        priority_values = [option['value'] for option in state_values['priority_block']['priority_select']['selected_options']]
        start_date = state_values['date_picker_block']['start_date_picker']['selected_date']
        end_date = state_values['date_picker_block']['end_date_picker']['selected_date']

        query = self.construct_query(selected_columns, organization_values, priority_values, start_date, end_date)
        results, columns = self.snowflake_connector.execute_query(query)
        presigned_url = self.s3_handler.upload_file(results, columns)

        if presigned_url:
            post_message_to_slack(dm_channel, "Your report is ready. Click the link to download.", presigned_url, client)
        else:
            client.chat_postMessage(channel=dm_channel, text="Failed to generate the report.")

    def construct_query(self, selected_columns, organization_values, priority_values, start_date, end_date):
        column_mapping = {
            'option_a': 'ID',
            'option_b': 'Priority',
            'option_c': 'Status',
            'option_d': 'Subject',
            'option_e': 'Date open',
            'option_f': 'Date closed'
        }
        org_mapping = {
            'org_1': 'NBC Universal',
            'org_2': 'Fox Networks Group',
            'org_3': 'Charter Communications',
            'org_4': 'Paramount',
            'org_5': 'Viacom US',
            'org_6': 'Altice USA',
            'org_7': 'British Sky Broadcasting',
            'org_8': 'Warner Brothers Discovery',
            'org_9': 'Channel 4',
            'org_10': 'ABC'
        }

        selected_columns_transformed = [column_mapping[col] for col in selected_columns]
        organization_values_transformed = [org_mapping[org] for org in organization_values]

        priority_str = ', '.join(f"'{p}'" for p in priority_values)
        query = f"""
        SELECT {', '.join(selected_columns_transformed)}
        FROM FW_OPERATIONAL_DATA.FIVETRAN_ZENDESK_SUPPORT.TICKET t
        JOIN FW_OPERATIONAL_DATA.FIVETRAN_ZENDESK_SUPPORT.ORGANIZATION o
        ON t.organization_id = o.id
        WHERE o.name IN ({', '.join(f"'{org}'" for org in organization_values_transformed)})
        AND t.priority IN ({priority_str})
        AND DATE(t.created_at) BETWEEN '{start_date}' AND '{end_date}'
        """
        return query
