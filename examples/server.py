#!/usr/bin/env python3

from flask import Flask, request
from flask_autoindex import AutoIndex
import json
from conbond import jisilu
import pathlib
from datetime import date
import os

app = Flask(__name__)
AutoIndex(app, browse_root=os.path.curdir)
AUTH=None

@app.route("/jisilu")
def jsl():
    global AUTH
    if not AUTH:
        auth_file = pathlib.Path('.auth.json')
        AUTH = json.load(auth_file.open('r'))
    username = AUTH['jisilu']['username']
    password = AUTH['jisilu']['password']
    df = jisilu.fetch(date.today(), 'cache', username, password)
    top = int(request.args.get('top', '20'))
    df = df.nsmallest(top, 'double_low')
    return df.to_json()

if __name__ == '__main__':
    app.run(host='localhost', port=18888)
