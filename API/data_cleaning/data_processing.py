import json
from datetime import datetime

data_path = 'data_cleaning/data/events.jsonl'
event_ids = set()
user_status = {}
valid_event_types = ['registration', 'login', 'logout', 'transaction']
valid_os_types = ['iOS', 'Android', 'Web']
valid_currencies = ['EUR', 'USD']
valid_transaction_amounts = [0.99, 1.99, 2.99, 4.99, 9.99]


def validate_registration_event(event_data):
    user_id = event_data.get('user_id')
    if not isinstance(user_id, str) or not user_id:
        raise ValueError('Invalid or missing user_id.')

    country = event_data.get('country')
    if not isinstance(country, str) or not country:
        raise ValueError('Invalid or missing country.')

    device_os = event_data.get('device_os')
    if not isinstance(device_os, str) or device_os not in valid_os_types:
        raise ValueError('Invalid or missing device_os.')

    marketing_campaign = event_data.get('marketing_campaign')
    if not (marketing_campaign is None or isinstance(marketing_campaign, str)):
        raise ValueError('Invalid marketing_campaign.')

    # check if user is already registered
    if user_id in user_status:
        raise ValueError(f'User {user_id} already registered.')
    user_status[user_id] = True


def validate_transaction_event(event_data):
    user_id = event_data.get('user_id')
    if not isinstance(user_id, str) or not user_id:
        raise ValueError('Invalid or missing user_id.')

    transaction_amount = event_data.get('transaction_amount')
    if not isinstance(transaction_amount, float) or transaction_amount not in valid_transaction_amounts:
        raise ValueError('Invalid or missing transaction_amount.')

    transaction_currency = event_data.get('transaction_currency')
    if not isinstance(transaction_currency, str) or transaction_currency not in valid_currencies:
        raise ValueError('Invalid or missing transaction_currency.')

    # check if user is logged in
    if user_id not in user_status or not user_status[user_id]:
        raise ValueError(f'User {user_id} can\'t make transaction while not logged in.')


def validate_login_event(event_data):
    user_id = event_data.get('user_id')
    if not isinstance(user_id, str) or not user_id:
        raise ValueError('Invalid or missing user_id.')

    # check if user is registered and logged out
    if user_id not in user_status:
        raise ValueError(f'User {user_id} not registered.')
    elif user_status[user_id]:
        raise ValueError(f'User {user_id} already logged in.')
    user_status[user_id] = True


def validate_logout_event(event_data):
    user_id = event_data.get('user_id')
    if not isinstance(user_id, str) or not user_id:
        raise ValueError('Invalid or missing user_id.')

    # check if user is registered and logged in
    if user_id not in user_status:
        raise ValueError(f'User {user_id} not registered.')
    elif not user_status[user_id]:
        raise ValueError(f'User {user_id} not logged in.')
    user_status[user_id] = False


def validate_base_event_data(event):
    if not isinstance(event.get('event_id'), int):
        raise ValueError('Invalid event_id')
    event_id = event.get('event_id')
    if event_id in event_ids:
        raise ValueError(f'Duplicate event_id: {event_id}.')
    event_ids.add(event_id)

    if not isinstance(event.get('event_timestamp'), int):
        raise ValueError('Invalid event_timestamp.')

    if not isinstance(event.get('event_type'), str) or event['event_type'] not in valid_event_types:
        raise ValueError('Invalid event_type.')

    if not isinstance(event.get('event_data'), dict):
        raise ValueError('Invalid event_data.')


def validate_event(event):
    validate_base_event_data(event)

    event_type = event.get('event_type')
    event_data = event.get('event_data')
    if event_type == 'registration':
        validate_registration_event(event_data)
    elif event_type == 'transaction':
        validate_transaction_event(event_data)
    elif event_type == 'login':
        validate_login_event(event_data)
    elif event_type == 'logout':
        validate_logout_event(event_data)


def prepare_events():
    events = []
    with open(data_path, 'r') as file:
        for line in file:
            event = json.loads(line)
            events.append(event)

    events.sort(key=lambda x: x['event_timestamp'])

    registration_events = {}
    transaction_events = {}
    login_logout_events = {}

    for event in events:
        try:
            validate_event(event)
            if event.get('event_type') == 'registration':  # registration should be unique so check is redundant
                registration_events[event.get('event_data').get('user_id')] = event
            elif event.get('event_type') == 'transaction':
                if event.get('event_data').get('user_id') not in transaction_events:
                    transaction_events[event.get('event_data').get('user_id')] = [event]
                else:
                    transaction_events[event.get('event_data').get('user_id')].append(event)
            elif event.get('event_type') == 'login' or event.get('event_type') == 'logout':
                if event.get('event_data').get('user_id') not in login_logout_events:
                    login_logout_events[event.get('event_data').get('user_id')] = [event]
                else:
                    login_logout_events[event.get('event_data').get('user_id')].append(event)
        except ValueError as e:
            print(f'Error validating event {event.get("event_id")}: {e}')
    return registration_events, transaction_events, login_logout_events


def prepare_for_load():
    registration_events, transaction_events, login_logout_events = prepare_events()
    records_for_load = {}

    for key in registration_events:
        user_record = {
            'user_id': key,
            'country': registration_events[key].get('event_data').get('country'),
            'registration_timestamp': datetime.utcfromtimestamp(registration_events[key].get('event_timestamp')),
            'device_os': registration_events[key].get('event_data').get('device_os'),
            'marketing_campaign': registration_events[key].get('event_data').get('marketing_campaign')
        }

        user_transaction_records = []
        if key in transaction_events:
            for transaction in transaction_events[user_record.get('user_id')]:
                transaction_record = {
                    'user_id': key,
                    'transaction_timestamp': datetime.utcfromtimestamp(transaction.get('event_timestamp')),
                    'amount': transaction.get('event_data').get('transaction_amount'),
                    'currency': transaction.get('event_data').get('transaction_currency')
                }
                user_transaction_records.append(transaction_record)

        user_session_records = []
        if key in login_logout_events:
            if len(login_logout_events[key]) == 1:
                session_record = {
                    'user_id': key,
                    'login_timestamp': datetime.utcfromtimestamp(registration_events[key].get('event_timestamp')),
                    'logout_timestamp': datetime.utcfromtimestamp(login_logout_events[key][0].get('event_timestamp')),
                    'session_length_seconds': registration_events[key].get('event_timestamp') -
                                              login_logout_events[key][0].get('event_timestamp')
                }
                user_session_records.append(session_record)
            else:
                i = 0
                while i < len(login_logout_events[key]):
                    if i == 0: # registration and logout pair
                        session_record = {
                            'user_id': key,
                            'login_timestamp': datetime.utcfromtimestamp(registration_events[key].get('event_timestamp')),
                            'logout_timestamp': datetime.utcfromtimestamp(login_logout_events[key][0].get('event_timestamp')),
                            'session_length_seconds': login_logout_events[key][0].get('event_timestamp') -
                                                      registration_events[key].get('event_timestamp')

                        }
                        user_session_records.append(session_record)
                        i = i + 1
                    else:
                        if i + 1 < len(login_logout_events[key]):  # we have a login and logout pair
                            session_record = {
                                'user_id': key,
                                'login_timestamp': datetime.utcfromtimestamp(login_logout_events[key][i].get('event_timestamp')),
                                'logout_timestamp': datetime.utcfromtimestamp(login_logout_events[key][i + 1].get('event_timestamp')),
                                'session_length_seconds': login_logout_events[key][i + 1].get('event_timestamp') -
                                                          login_logout_events[key][i].get('event_timestamp')
                            }
                            user_session_records.append(session_record)
                            i = i + 2
                        else:  # we don't have a logout for the last login
                            session_record = {
                                'user_id': key,
                                'login_timestamp': datetime.utcfromtimestamp(login_logout_events[key][i].get('event_timestamp')),
                                'logout_timestamp': None,
                                'session_length_seconds': None
                            }
                            user_session_records.append(session_record)
                            i = i + 1
        records_for_load[key] = (user_record, user_transaction_records, user_session_records)

    return records_for_load
