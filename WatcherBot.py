import configparser
import json
import datetime
import arrow
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch
from telethon.tl.types import (PeerChannel)
from telethon.tl.functions.messages import (GetHistoryRequest)
import asyncio
import pandas as pd


# constants
channels = [ 'https://t.me/SignalTestChannele2', 'https://t.me/signalTestChannele' ]
message_object_key = 'Message'
message_key = 'message'
message_type_key = '_' 
date_type_key = 'date'
name_excel = pd.read_excel (r'names.xlsx')
keywords_used_in_search = list(pd.DataFrame(name_excel)['namad'])
print(keywords_used_in_search)
search_result_by_keyword_filtered_by_channels = dict()

last_date_data = {}
previous_last_date_data = {}
try:
    with open('last date for each channel.json') as json_file:
        previous_last_date_data = json.load(json_file)
except FileNotFoundError:
    print('last data not found')
# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# Setting configuration values
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']

api_hash = str(api_hash)

phone = config['Telegram']['phone']
username = config['Telegram']['username']

client = TelegramClient(username, api_id, api_hash)

async def main():
    await client.start()
    print("Client Created")
    # Ensure you're authorized
    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    for channel in channels:
        await fetch_messages_from_single_channel(channel) 
    
async def fetch_messages_from_single_channel(channel):
    if channel.isdigit():
        entity = PeerChannel(int(channel))
    else:
        entity = channel

    my_channel = await client.get_entity(entity)

    offset_id = 0
    limit = 50 #how many messages in each time we want to fetch from the channel
    all_messages = [] # list of all messages recieved from channel
    total_messages = 0 # total number of messages fetched from channel
    total_count_limit = 200 # count limit on how many messages we want to fetch from the channel totally
    
    while True:
        print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
        history = await client(GetHistoryRequest(
            peer=my_channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            all_messages.append(message.to_dict())
        offset_id = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break
        message_data = json.loads(json.dumps(all_messages, cls=DateTimeEncoder))
        message_extracted_data = []
        
        for m in message_data:
            if m[message_type_key] == message_object_key:
                message_extracted_data.append( (m[message_key], m[date_type_key]) )

        search_result_by_keyword = dict()
        for key_word in keywords_used_in_search:
            # search_result_by_keyword[key_word] = list(filter(lambda l: key_word in l[0], message_extracted_data))

            for filtered in list(filter(lambda l: key_word in l[0], message_extracted_data)):
                search_result_by_keyword[key_word] = (filtered, len(filtered) / 2)
            search_result_by_keyword_filtered_by_channels[channel] = search_result_by_keyword

        last_date_data[channel] = message_data[0][date_type_key]

        if previous_last_date_data.get(channel) != None and message_data[-1][date_type_key] > previous_last_date_data.get(channel):
            break

class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime.datetime):
            return (str(z))
        elif isinstance(z, bytes):
            return(str(z))
        else:
            return super().default(z)

def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

with client:
    client.loop.run_until_complete(main())
    with open('search result.json', 'w', encoding='utf8') as f:
        json.dump(search_result_by_keyword_filtered_by_channels, f, ensure_ascii=False) 
    
    print(last_date_data)

    with open('last date for each channel.json', 'w', encoding='utf8') as last_date_file:
        json.dump(last_date_data, last_date_file)
    