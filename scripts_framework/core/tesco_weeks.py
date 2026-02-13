from core import database

def get_current_tesco_week():
    return database.get_current_tesco_week()

def get_previous_tesco_week():
    return database.get_tesco_week()

def get_current_tesco_period():
    return database.get_current_tesco_period()