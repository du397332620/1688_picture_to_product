import requests
from collections import OrderedDict
import time
import random
import re
import json
import base64
from threading import Thread
from queue import Queue
import traceback

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def get_date():
    api = 'https://open-s.1688.com/openservice/.htm'
    callback = 'jsonp_%d_%d' % (int(time.time()*1000), random.randint(10000, 100000))
    params = OrderedDict([
        ('outfmt', 'jsonp'),
        ('serviceIds', 'cbu.searchweb.config.system.currenttime'),
        ('callback', callback)
    ])
    headers = OrderedDict([
        ('Accept', '*/*'),
        ('Accept-Encoding', 'gzip, deflate, br'),
        ('Accept-Language', 'zh-CN,zh;q=0.9,en;q=0.8'),
        ('Cache-Control', 'no-cache'),
        ('Connection', 'keep-alive'),
        ('Host', 'open-s.1688.com'),
        ('Pragma', 'no-cache'),
        ('Referer', 'https://s.1688.com/youyuan/index.htm'),
        ('Sec-Fetch-Dest', 'script'),
        ('Sec-Fetch-Mode', 'no-cors'),
        ('Sec-Fetch-Site', 'same-site'),
        ('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36')
    ])

    r = requests.get(api, params=params, headers=headers, proxies=proxies, verify=False)
    data = json.loads(re.findall(r'%s\(([^)]+)\)' % callback, r.text)[0])
    return data['cbu.searchweb.config.system.currenttime']['dataSet']


def get_params():
    api = 'https://open-s.1688.com/openservice/ossDataService'
    key = str(base64.b64decode('cGNfdHVzb3U='.encode('utf-8')), encoding='utf-8')
    appkey = '%s;%s' % (key, str(get_date()))
    callback = 'jsonp_%d_%d' % (int(time.time()*1000), random.randint(10000, 100000))

    params = {
        'appName': key,
        'appKey': base64.b64encode(appkey.encode('utf-8')),
        'callback': callback
    }
    headers = OrderedDict([
        ('Accept', '*/*'),
        ('Accept-Encoding', 'gzip, deflate, br'),
        ('Accept-Language', 'zh-CN,zh;q=0.9,en;q=0.8'),
        ('Cache-Control', 'no-cache'),
        ('Connection', 'keep-alive'),
        ('Host', 'open-s.1688.com'),
        ('Pragma', 'no-cache'),
        ('Referer', 'https://s.1688.com/youyuan/index.htm'),
        ('Sec-Fetch-Dest', 'script'),
        ('Sec-Fetch-Mode', 'no-cors'),
        ('Sec-Fetch-Site', 'same-site'),
        ('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36')
    ])

    r = requests.get(api, params=params, headers=headers, proxies=proxies, verify=False)
    data = json.loads(re.findall(r'%s\(([^)]+)\)' % callback, r.text)[0])
    return data['data']['host'], data['data']['accessid'], data['data']['policy'], data['data']['signature']


def get_random_str(length):
    s = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
    return ''.join([random.choice(s) for x in range(length)])


def upload_img(img_url, host, accessid, policy, signature):
    key = "cbuimgsearch/" + get_random_str(10) + str(int(time.time() * 1000)) + ".jpg"
    name = get_random_str(5) + ".jpg"

    r = requests.get(img_url, proxies=proxies, verify=False)
    files = {
        "name": (None, name),
        "key": (None, key),
        "policy": (None, policy),
        "OSSAccessKeyId": (None, accessid),
        "success_action_status": (None, 200),
        "callback": (None, ""),
        "signature": (None, signature),
        "file": (name, r.content)
    }
    headers = OrderedDict([
        ('Accept', '*/*'),
        ('Accept-Encoding', 'gzip, deflate, br'),
        ('Accept-Language', 'zh-CN,zh;q=0.9,en;q=0.8'),
        ('Cache-Control', 'no-cache'),
        ('Connection', 'keep-alive'),
        ('Content-Length', '20367'),
        ('Host', 'cbusearch.oss-cn-shanghai.aliyuncs.com'),
        ('Origin', 'https://s.1688.com'),
        ('Pragma', 'no-cache'),
        ('Referer', 'https://s.1688.com/youyuan/index.htm'),
        ('Sec-Fetch-Dest', 'empty'),
        ('Sec-Fetch-Mode', 'cors'),
        ('Sec-Fetch-Site', 'cross-site'),
        ('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36')
    ])

    r = requests.post(host, files=files, headers=headers, proxies=proxies, verify=False)
    return key


def get_data(key):
    api = 'https://search.1688.com/service/imageSearchOfferResultViewService'
    headers = OrderedDict([
        ('Accept', '*/*'),
        ('Accept-Encoding', 'gzip, deflate, br'),
        ('Accept-Language', 'zh-CN,zh;q=0.9,en;q=0.8'),
        ('Cache-Control', 'no-cache'),
        ('Connection', 'keep-alive'),
        ('Host', 'search.1688.com'),
        ('Origin', 'https://s.1688.com'),
        ('Pragma', 'no-cache'),
        ('Referer', 'https://s.1688.com/youyuan/index.htm'),
        ('Sec-Fetch-Dest', 'empty'),
        ('Sec-Fetch-Mode', 'cors'),
        ('Sec-Fetch-Site', 'same-site'),
        ('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36')
    ])
    params = OrderedDict([
        ('tab', 'imageSearch'),
        ('imageType', 'oss'),
        ('imageAddress', key),
        ('spm', 'a260k.dacugeneral.search.0'),
        ('beginPage', '1'),
        ('pageSize', '40'),
    ])

    r = requests.get(api, params=params, headers=headers, verify=False)
    return list(filter(lambda x: x['tradePrice']['offerPrice']['originalValue']['integer']<=max_price, r.json()['data']['data']['offerList']))[:5]


def get_delivery_rate(login_id):
    api = 'https://member.1688.com/member/ajax/member_bsr_indexs_json.do'
    callback = 'jsonp_%d_%d' % (int(time.time() * 1000), random.randint(10000, 100000))
    params = OrderedDict([('_input_charset', 'utf-8'), ('loginid', login_id), ('callback', callback)])
    headers = OrderedDict([
        ('accept', '*/*'),
        ('accept-encoding', 'gzip, deflate, br'),
        ('accept-language', 'zh-CN,zh;q=0.9,en;q=0.8'),
        ('cache-control', 'no-cache'),
        ('pragma', 'no-cache'),
        ('referer', 'https://s.1688.com/youyuan/index.htm'),
        ('sec-fetch-dest', 'script'),
        ('sec-fetch-mode', 'no-cors'),
        ('sec-fetch-site', 'same-site'),
        ('user-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36')
    ])

    while True:
        try:
            r = requests.get(api, params=params, headers=headers, proxies=proxies, verify=False)
            break
        except:
            output_queue.put(('log', traceback.format_exc()))
            continue
    data = json.loads(re.findall(r'%s\(([^)]+)\)' % callback, r.text)[0])
    for t in data['data']['bsrindexs']:
        if t['indexNameCN'] == '发货速度':
            return t['indexDiffRate']


if __name__ == '__main__':
    import os
    import threading

    # 使用代理
    proxies = {
        'xxxxxx',
        'xxxxxx'
    }
    # 不使用代理
    # proxies  = None
    spider_thread_num = 20#线程数
    max_price = 35 #最大价格

    finished_set = set()
    if os.path.isfile('finished.txt'):
        with open('finished.txt', 'r') as f:
            for t in f:
                finished_set.add(t.strip())

        with open('finished.txt', 'w') as f:
            for t in finished_set:
                f.write(t+'\n')

    total_count = 0
    temp_set = set()
    with open('图片.csv', 'r') as f:
        for t in f:
            if not t.strip() in finished_set:
                temp_set.add(t.strip())

    if os.path.isfile('untarget.csv'):
        with open('untarget.csv', 'r') as f:
            for t in f:
                if not t.strip() in finished_set:
                    temp_set.add(t.strip())

    with open('temp.csv', 'w') as f1:
        for t in temp_set:
            f1.write(t+'\n')
            total_count += 1

    pic_url_queue = Queue(maxsize=100)
    output_queue = Queue()

    def output_worker():
        log_file = open('log.txt', 'a', newline='', encoding='utf-8')
        output_file = open('data.csv', 'a', newline='', encoding='utf-8')
        finished_file = open('finished.txt', 'a')
        untarget = open('untarget.csv', 'w')
        count = 0
        start = time.time()
        while True:
            t, data = output_queue.get()
            if t == 'data':
                thread_name, goods_url, pic_url = data
                output_file.write(goods_url+'\n')
                output_file.flush()
                finished_file.write(pic_url+'\n')
                finished_file.flush()
                count += 1
                print('当前速度：%d/12h，已完成：%d / %d, %s' % (int(count / (time.time()-start) * 3600 * 12), count, total_count, thread_name))
            elif t == 'log':
                log_file.write(time.strftime('%Y-%m-%d %H:%M:%S ', time.localtime(time.time())))
                log_file.write(data+'\n')
                log_file.flush()
            elif t == 'print':
                print(data)
            elif t == 'untarget':
                untarget.write(data+'\n')
                untarget.flush()
                finished_file.write(data+'\n')
                finished_file.flush()
                count += 1
                print('当前速度：%d/12h，已完成：%d / %d' % (int(count / (time.time()-start) * 3600 * 12), count, total_count))
    output_thread = Thread(None, output_worker)
    output_thread.start()

    def url_generator():
        output_queue.put(('print', 'url generator thread start'))
        with open('temp.csv', 'r') as f:
            for t in f:
                pic_url_queue.put(t.strip())
    url_generator_thread = Thread(None, url_generator)
    url_generator_thread.start()

    def spider():
        thread_name = threading.currentThread().getName()
        while True:
            pic_url = pic_url_queue.get()
            while True:
                try:
                    host, accessid, policy, signature = get_params()
                    key = upload_img(pic_url, host, accessid, policy, signature)
                    goods_list = get_data(key)

                    max_delivery_rate = -1000
                    url = None
                    for t in goods_list:
                        delivery_rate = get_delivery_rate(t['aliTalk']['loginId'])
                        if delivery_rate > max_delivery_rate:
                            max_delivery_rate = delivery_rate
                            url = t['information']['detailUrl']

                    if not url is None:
                        # output_queue.put(('print', (url, max_delivery_rate)))
                        output_queue.put(('data', (thread_name, url, pic_url)))
                    else:
                        output_queue.put(('untarget', pic_url))
                    break
                except:
                    output_queue.put(('log', traceback.format_exc()))
                    continue

    spider_threads = []
    for i in range(spider_thread_num):
        spider_threads.append(Thread(None, spider))

    for t in spider_threads:
        t.start()

    for t in spider_threads:
        t.join()
