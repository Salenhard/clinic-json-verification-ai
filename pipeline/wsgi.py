"""WSGI entry point for gunicorn.

Usage:
    gunicorn --bind 0.0.0.0:5000 --workers 2 wsgi:application
"""
from app import app, init_db

init_db()
application = app
