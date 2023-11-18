import os
import sys
import json
from datetime import timedelta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import func, case, or_, text, create_engine

database_url = os.getenv('DATABASE_URL')

exchangerate_path = 'data_cleaning/data/exchange_rates.jsonl'
EURtoUSD = None
with open(exchangerate_path, 'r') as file:
    for line in file:
        data = json.loads(line)

        if data['currency'] == 'EUR':
            EURtoUSD = float(data['rate_to_usd'])
            break

Base = automap_base()
try:
    engine = create_engine(database_url)
except Exception as e:
    print(str(e))
    sys.exit()
Session = sessionmaker(bind=engine)
Base.prepare(engine, reflect=True)

User = Base.classes.users
Transaction = Base.classes.transactions
BaseSession = Base.classes.sessions


def create_session():
    try:
        session = Session()
        return session
    except Exception as ex:
        print(f'Error while creating DB session: {ex}')


def insert_records(class_type, records):
    session = create_session()
    if isinstance(records, list):
        try:
            for record in records:
                instance = class_type()
                for key, value in record.items():
                    setattr(instance, key, value)
                session.add(instance)

            session.commit()
            print(f"All {class_type.__name__} records inserted successfully.")
        except Exception as ex:
            session.rollback()
            print(f"Error occurred during insertion: {ex}")
        finally:
            session.close()
    else:
        try:
            instance = class_type()
            for key, value in records.items():
                setattr(instance, key, value)
            session.add(instance)
            session.commit()
            print(f"All {class_type.__name__} records inserted successfully.")
        except Exception as ex:
            session.rollback()
            print(ex)
            print(f"Error occurred during insertion: {ex}")
        finally:
            session.close()


def load_data(records_for_load):
    for key in records_for_load:
        insert_records(User, records_for_load[key][int(0)])
        insert_records(Transaction, records_for_load[key][int(1)])
        insert_records(BaseSession, records_for_load[key][int(2)])


def get_country(session, user_id):
    country_query = session.query(User.country).filter(User.user_id == user_id).scalar()
    return country_query


def get_no_of_logins_and_days_since_last_login(session, user_id, date):
    login_query = session.query(
        func.count(BaseSession.session_id).label('num_logins'),
        func.max(BaseSession.login_timestamp).label('last_login')
    ).filter(BaseSession.user_id == user_id).one()

    number_of_logins = login_query.num_logins
    last_login = login_query.last_login

    if date is None:
        latest_login_date_query = session.query(func.max(BaseSession.login_timestamp)).scalar()
        latest_logout_date_query = session.query(func.max(BaseSession.logout_timestamp)).scalar()
        latest_transaction_date_query = session.query(func.max(Transaction.transaction_timestamp)).scalar()
        latest_date_in_database = max(latest_login_date_query, latest_transaction_date_query, latest_logout_date_query)
        days_since_last_login = (latest_date_in_database - last_login).days
    else:
        days_since_last_login = (date - last_login).days

    return number_of_logins, days_since_last_login + 1


def get_session_count(session, user_id, date):
    if date is None:
        session_count_query = session.query(func.count(BaseSession.session_id)).filter(
            BaseSession.user_id == user_id,
            BaseSession.session_valid == True
        ).scalar()
    else:
        session_count_query = session.query(func.count(BaseSession.session_id)).filter(
            BaseSession.user_id == user_id,
            BaseSession.session_valid == True,
            func.date(BaseSession.login_timestamp) == date
        ).scalar()

    return session_count_query


def get_time_spent(session, user_id, date):
    if date is None:
        time_spent = session.query(func.sum(BaseSession.session_length_seconds)).filter(
            BaseSession.user_id == user_id).scalar()
    else:
        query_text = text("""
                        SELECT SUM(
                            CASE
                                WHEN login_timestamp < :date_start AND logout_timestamp > :date_end THEN
                                    EXTRACT(EPOCH FROM INTERVAL '1 day')
                                WHEN login_timestamp < :date_start THEN
                                    EXTRACT(EPOCH FROM logout_timestamp - :date_start)
                                WHEN logout_timestamp > :date_end THEN
                                    EXTRACT(EPOCH FROM :date_end - login_timestamp)
                                ELSE
                                    session_length_seconds
                            END
                        ) AS total_time_spent
                        FROM sessions
                        WHERE user_id = :user_id
                          AND session_valid = TRUE
                          AND login_timestamp < :date_end
                          AND logout_timestamp > :date_start
                    """)

        time_spent = session.execute(query_text, {'user_id': user_id, 'date_start': date,
                                                  'date_end': date + timedelta(days=1)}).scalar()

    return time_spent if time_spent is not None else 0


def query_user_data(user_id, date):
    try:
        session = create_session()
        country = get_country(session, user_id)
        number_of_logins, days_since_last_login = get_no_of_logins_and_days_since_last_login(session, user_id, date)
        session_count = get_session_count(session, user_id, date)
        time_spent = get_time_spent(session, user_id, date)

        response = {
            'country': country,
            'number_of_logins': number_of_logins,
            'days_since_last_login': days_since_last_login,
            'number_of_sessions': session_count,
            'time_spent_in_game': time_spent
        }

        session.close()
        return response
    except Exception as ex:
        return ex


def get_number_of_active_users(session, country, date):
    query = session.query(User.user_id).join(BaseSession, User.user_id == BaseSession.user_id).distinct(User.user_id)

    if date is not None:
        query = query.filter(func.date(BaseSession.login_timestamp) == date)

    if country is not None:
        query = query.filter(User.country == country)

    return query.count()


def get_number_of_logins(session, country, date):
    query = session.query(func.count(BaseSession.session_id)).join(User, User.user_id == BaseSession.user_id)

    if date is not None:
        query = query.filter(func.date(BaseSession.login_timestamp) == date)

    if country is not None:
        query = query.filter(User.country == country)

    return query.scalar()


def get_total_revenue(session, country, date):
    amount_calculation = case(
        (Transaction.currency == 'EUR', Transaction.amount * 1.3),
        else_=Transaction.amount
    )

    query = session.query(func.sum(amount_calculation))

    if date is not None:
        query = query.filter(func.date(Transaction.transaction_timestamp) == date)

    if country is not None:
        query = query.join(User, User.user_id == Transaction.user_id)
        query = query.filter(User.country == country)

    total_revenue = query.scalar()
    return total_revenue if total_revenue is not None else 0


def get_number_of_paid_users(session, country, date):
    query = session.query(func.count(User.user_id)).filter(User.marketing_campaign.isnot(None),
                                                           User.marketing_campaign != '')

    if date is not None:
        query = query.filter(func.date(User.registration_timestamp) == date)

    if country is not None:
        query = query.filter(User.country == country)

    return query.scalar()


def get_average_number_of_sessions(session, country, date):
    subquery = session.query(
        BaseSession.user_id,
        func.count(BaseSession.session_id).label('session_count')
    ).group_by(BaseSession.user_id).subquery()

    query = session.query(func.avg(subquery.c.session_count))

    query = query.join(User, User.user_id == subquery.c.user_id)

    if date is not None:
        date_filter = or_(
            func.date(BaseSession.login_timestamp) == date,
            func.date(BaseSession.logout_timestamp) == date
        )
        query = query.filter(date_filter)

    if country is not None:
        query = query.filter(User.country == country)

    average_sessions = query.scalar()
    return average_sessions if average_sessions is not None else 0


def get_average_total_time_spent(session, country, date):
    query_text = text("""
                    SELECT AVG(total_time_spent) AS average_time_spent
                    FROM (
                        SELECT sessions.user_id, SUM(
                            CASE
                                WHEN :date_start IS NOT NULL AND :date_end IS NOT NULL THEN
                                    CASE
                                        WHEN sessions.login_timestamp < :date_start AND sessions.logout_timestamp > :date_end THEN
                                            EXTRACT(EPOCH FROM INTERVAL '1 day')
                                        WHEN sessions.login_timestamp < :date_start THEN
                                            EXTRACT(EPOCH FROM sessions.logout_timestamp - :date_start)
                                        WHEN sessions.logout_timestamp > :date_end THEN
                                            EXTRACT(EPOCH FROM :date_end - sessions.login_timestamp)
                                        ELSE
                                            sessions.session_length_seconds
                                    END
                                ELSE
                                    sessions.session_length_seconds
                            END
                        ) AS total_time_spent
                        FROM sessions
                        JOIN users ON sessions.user_id = users.user_id
                        WHERE sessions.session_valid = TRUE
                          AND (:country IS NULL OR users.country = :country)
                          AND (:date_start IS NULL OR sessions.login_timestamp < :date_end)
                          AND (:date_end IS NULL OR sessions.logout_timestamp > :date_start)
                        GROUP BY sessions.user_id
                    ) AS per_user_totals
                        """)

    average_total_time_spent = session.execute(query_text, {'country': country, 'date_start': date,
                                              'date_end': date + timedelta(days=1) if date is not None else None}).scalar()

    return average_total_time_spent if average_total_time_spent is not None else 0


def query_game_data(date, country):
    try:
        session = create_session()
        number_of_active_users = get_number_of_active_users(session, country, date)
        number_of_logins = get_number_of_logins(session, country, date)
        total_revenue = get_total_revenue(session, country, date)
        number_of_paid_users = get_number_of_paid_users(session, country, date)
        average_number_of_sessions = get_average_number_of_sessions(session, country, date)
        average_total_time_spent = get_average_total_time_spent(session, country, date)

        response = {
            'number_of_active_users': number_of_active_users,
            'number_of_logins': number_of_logins,
            'total_revenue': total_revenue,
            'number_of_paid_users': number_of_paid_users,
            'average_number_of_sessions': average_number_of_sessions,
            'average_total_time_spent': average_total_time_spent
        }

        session.close()
        return response
    except Exception as ex:
        return str(ex)
