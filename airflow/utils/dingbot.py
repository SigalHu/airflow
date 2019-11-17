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
import logging
from json import JSONDecodeError

import requests
import urllib3

from airflow import configuration

urllib3.disable_warnings()


def dingbot_msg_sender(msg):
    send_markdown("AIRFLOW ERROR", msg)


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


def send_markdown(title, markdown_text):
    bot_url = configuration.get('dingding', 'BOT_URL')
    headers = {'Content-Type': 'application/json'}

    md_text = {
        "title": title,
        "text": markdown_text
    }

    post_data = {
        "msgtype": "markdown",
        "markdown": md_text,
        "at": {
            "isAtAll": True,
        },
    }

    try:
        response = requests.post(bot_url, headers=headers, data=json.dumps(post_data))
    except requests.exceptions.HTTPError as exc:
        logging.error("消息发送失败， HTTP error: %d, reason: %s" % (exc.response.status_code, exc.response.reason))
    except requests.exceptions.ConnectionError:
        logging.error("消息发送失败，HTTP connection error!")
    except requests.exceptions.Timeout:
        logging.error("消息发送失败，Timeout error!")
    except requests.exceptions.RequestException:
        logging.error("消息发送失败, Request Exception!")
    else:
        try:
            result = response.json()
        except JSONDecodeError:
            logging.error("服务器响应异常，状态码：%s，响应内容：%s" % (response.status_code, response.text))
        else:
            logging.debug('发送结果：%s' % result)
            if result['errcode']:
                error_data = {"msgtype": "text", "text": {"content": "OPS钉钉机器人消息发送失败，原因：%s" % result['errmsg']},
                              "at": {"isAtAll": True}}
                logging.error("消息发送失败，自动通知：%s" % error_data)
                try:
                    requests.post(bot_url, headers=headers, data=json.dumps(error_data))
                except:
                    pass


if __name__ == '__main__':
    pass
