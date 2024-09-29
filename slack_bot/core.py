import requests  # Add this line to import the requests module
from slack_bot.app_init import app
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_bot.reports import ReportsHandler
from slack_bot.tickets import TicketsHandler
from slack_bot.Query_Crafter import handle_help_command
from slack_bot.Query_finder import query_finder_func
import logging


class SlackBotCore:
    def __init__(self, config, snowflake_connector, s3_handler, logger):
        # Store the config
        self.config = config
        
        # Initialize Slack App with the token from config
        self.app = App(token=self.config['SLACK_BOT_TOKEN'])
        
        # Initialize handlers
        self.reports_handler = ReportsHandler(self.config, snowflake_connector, s3_handler, logger)
        self.tickets_handler = TicketsHandler(self.config, logger)
        self.logger = logger

        # Register Slack event handlers
        self.app.command("/slackflake")(self.execute)
        self.app.action("report")(self.reports_handler.open_report_modal)
        self.app.view("report_function")(self.reports_handler.handle_report_submission)
        self.app.action("relevant_ticket")(self.tickets_handler.handle_relevant_ticket)
        self.app.action("Query_Crafter")(handle_help_command)
        self.app.action("query_finder")(query_finder_func)

    def execute(self, ack, body, client):
        ack()
        user_id = body['user_id']

        # Define the API details for authentication
        api_url = "https://focapi.freewheel.com/api/v1/access/slack/user/auth"
        params = {"slackid": user_id, "ldapgroup": self.config['LDAP_GROUP'], "apikey": self.config['API_KEY']}

        try:
            response = requests.get(api_url, params=params)
            data = response.json()

            if data["status"] == 200 and data["output"] is True:
                # User is authorized, open the main menu modal
                client.views_open(
                    trigger_id=body["trigger_id"],
                    view=self.get_main_menu_view()
                )
            else:
                client.chat_postMessage(channel=body['channel_id'], text="You are not authorized to use this bot.")

        except requests.exceptions.RequestException as e:
            client.chat_postMessage(channel=body['channel_id'], text=f"Error occurred: {str(e)}")

    def get_main_menu_view(self):
        return {
            "type": "modal",
            "callback_id": "task-menu",
            "title": {"type": "plain_text", "text": "Slack Flake"},
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": "Slack Flake"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "plain_text", "text": "Report"}, "accessory": {"type": "button", "text": {"type": "plain_text", "text": "Click Here"}, "action_id": "report"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "plain_text", "text": "Relevant ticket"}, "accessory": {"type": "button", "text": {"type": "plain_text", "text": "Click Here"}, "action_id": "relevant_ticket"}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "plain_text", "text": "Query Crafter"}, "accessory": {"type": "button", "text": {"type": "plain_text", "text": "Click Here"}, "action_id": "Query_Crafter"}},
                {"type": "divider"},
                {"type": "section","text": {"type": "plain_text", "text": "Query Finder", "emoji": True},"accessory": {"type": "button","text": {"type": "plain_text", "text": "Click Here"},"action_id": "query_finder"}}
            ]
        }

    def start(self):
        # Starting the Slack app with SocketModeHandler
        SocketModeHandler(self.app, self.config['SLACK_APP_TOKEN']).start()
