# encoding=utf-8
"""
@author huxujun
@date 2019/9/24
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import importlib
import json

import requests
import urllib3

from airflow import configuration

urllib3.disable_warnings()


def dingbot_msg_sender(msg):
    bot_url = configuration.get('dingding', 'BOT_URL')
    headers = {'Content-Type': 'application/json'}

    md_text = {
        "title": "AIRFLOW ERROR",
        "text": msg
    }

    post_data = {
        "msgtype": "markdown",
        "markdown": md_text
    }

    r = requests.post(bot_url, headers=headers, data=json.dumps(post_data))


def ding_bot_backend(msg):
    """
    Send ding message using backend specified in DING_BOT_BACKEND
    :param msg:
    :return:
    """
    path, attr = configuration.get('dingbot', 'DING_BOT_BACKEND').rsplit('.', 1)
    module = importlib.import_module(path)
    backend = getattr(module, attr)
    return backend(msg)


if __name__ == '__main__':
    pass
