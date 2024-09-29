import requests

def post_link_to_slack(channel, message, hyperlink_text, hyperlink_url, client):
    markdown_message = f"{message} <{hyperlink_url}|{hyperlink_text}>"
    headers = {'Authorization': f'Bearer {client.token}', 'Content-Type': 'application/json'}
    data = {'channel': channel, 'text': markdown_message}
    response = requests.post('https://slack.com/api/chat.postMessage', headers=headers, json=data)
    return response

def post_message_to_slack(channel, message, client):
    headers = {'Authorization': f'Bearer {client.token}', 'Content-Type': 'application/json'}
    data = {'channel': channel, 'text': message}
    response = requests.post('https://slack.com/api/chat.postMessage', headers=headers, json=data)
    return response
