class TicketsHandler:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def handle_relevant_ticket(self, ack, body, client):
        ack(response_action="clear")
        self.logger.info("This functionality is under construction.")
        client.views_update(
            view_id=body['view']['id'],
            hash=body['view']['hash'],
            view={
                "type": "modal",
                "callback_id": "relevant_function",
                "title": {"type": "plain_text", "text": "Slack Flake"},
                "blocks": [
                    {"type": "header", "text": {"type": "plain_text", "text": "Relevant ticket"}},
                    {"type": "section", "text": {"type": "plain_text", "text": "This functionality is under construction."}}
                ],
                "close": {"type": "plain_text", "text": "Close"}
            }
        )
