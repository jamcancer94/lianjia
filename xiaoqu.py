# coding:utf-8

import requests
import json
import time
import re
from lxml import html
from pymongo import MongoClient
from multiprocessing.dummy import Pool as ThreadPool

# mongodb
client = MongoClient()
dbName = 'sz'
dbTable = 'xiaoqu'
tab = client[dbName][dbTable]
dbTable2 = 'xiaoqu_detail'
tab2 = client[dbName][dbTable2]

# conn = MongoClient(host='127.0.0.1', port='27017')

# urls = []
# for i in range(99):
#     url = 'http://sz.lianjia.com/xiaoqu/pg' + str(i+1) + '/'
#     urls.append(url)
# url = ['http://sz.lianjia.com/xiaoqu/']

header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36',}

cookie = {'Cookie': 'all-lj=007e0800fb44885aa2065c6dfaaa4029; lianjia_uuid=03fa8b2a-3e74-44f8-90f1-7c1d170e91f3; _jzqy=1.1486989264.1487154044.1.jzqsr=baidu|jzqct=%E9%93%BE%E5%AE%B6.-; _jzqckmp=1; select_city=440300; _jzqx=1.1487164436.1487221856.2.jzqsr=sz%2Elianjia%2Ecom|jzqct=/xiaoqu/.jzqsr=sz%2Elianjia%2Ecom|jzqct=/xiaoqu/cro21/; _smt_uid=58a1a7cf.38dc469b; CNZZDATA1255849469=1669532160-1486985492-http%253A%252F%252Fbzclk.baidu.com%252F%7C1487224449; CNZZDATA1254525948=1024209759-1486984312-http%253A%252F%252Fbzclk.baidu.com%252F%7C1487221927; CNZZDATA1255633284=398859508-1486985724-http%253A%252F%252Fbzclk.baidu.com%252F%7C1487222774; CNZZDATA1255604082=2027433198-1486986231-http%253A%252F%252Fbzclk.baidu.com%252F%7C1487220488; _qzja=1.2087132298.1486989263881.1487213710568.1487221855922.1487223580959.1487224891365.0.0.0.58.5; _qzjb=1.1487221855920.7.0.0.0; _qzjc=1; _qzjto=19.2.0; _jzqa=1.1590252034770900000.1486989264.1487213711.1487221856.5; _jzqc=1; _jzqb=1.7.10.1487221856.1; _ga=GA1.2.1213470030.1486989266; lianjia_ssid=c31b2d96-680c-46b3-b5d3-0083a529784e'}
# 代理
proxies = {
    "http": "xxxxx"
}

def getSource(url):
    rep = requests.get(url)
    # print rep.content
    selector = html.fromstring(rep.content)
    pre_list = selector.xpath('//ul[@class="listContent"]/li')
    items = {}
    i = 0
    for sel in pre_list:
        name = sel.xpath('div[@class="info"]/div[@class="title"]/a/text()')[0]
        link = sel.xpath('div[@class="info"]/div[@class="title"]/a/@href')[0]
        price = sel.xpath('div[@class="xiaoquListItemRight"]/div/div[@class="totalPrice"]/span/text()')[0]
        num = re.findall('(\d+)', link)[0]
        # 这一步出现键错误，自定义_id的值
        items['_id'] = num
        items['name'] = name
        items['link'] = link
        items['price'] = price
        # items['num'] = num
        tab.insert(items)
        i += 1
        print name, link, i
        print '成功 '

def getDetail(url):

    items = {}
    time_start = time.time()
    # url = x['link']
    id = re.findall('(\d+)', url)[0]
    # print id
    rep = requests.get(url, headers=header, cookies=cookie)
    sel = html.fromstring(rep.content)
    qu = sel.xpath('//div[@class="fl l-txt"]/a[3]/text()')[0]
    di = sel.xpath('//div[@class="fl l-txt"]/a[4]/text()')[0]
    name = sel.xpath('//div[@class="fl l-txt"]/a[5]/text()')[0]
    price = sel.xpath('//div[@class="xiaoquPrice clear"]/div/span/text()')[0]
    info = sel.xpath('//div[@class="xiaoquInfo"]')
    for i in info:
        data = i.xpath('div[1]/span[2]/text()')[0]
        wuyefei = i.xpath('div[3]/span[2]/text()')[0]
        wuye = i.xpath('div[4]/span[2]/text()')[0]
        kaifa = i.xpath('div[5]/span[2]/text()')[0]
        loudong = i.xpath('div[6]/span[2]/text()')[0]
        hushu = i.xpath('div[7]/span[2]/text()')[0]

    try:
        priceTrendUrl = 'http://sz.lianjia.com/fangjia/priceTrend/c' + (str(id)) + '?nfbd=1'
        # print priceTrendUrl
        priceTrendPage = requests.get(priceTrendUrl, headers=header, cookies=cookie).content
        priceTrendJson = json.loads(priceTrendPage, 'utf-8')

        for each_price, each_month in \
                zip(priceTrendJson['currentLevel']['dealPrice']['total'],
                    priceTrendJson['currentLevel']['month']):
            items[each_month] = each_price
    except:
        pass

    items[u'小区名'] = name
    items[u'均价'] = price
    items['_id'] = id
    items[u'区'] = qu
    items[u'区域'] = di
    items[u'建成日期'] = data
    items[u'物业公司'] = wuye
    items[u'开发商'] = kaifa
    items[u'物业费'] = wuyefei
    items[u'楼栋总数'] = loudong
    items[u'户数'] = hushu
    tab2.insert(items)
    print u'小区完成:%s ,%s' % (name, (time.time() - time_start))
    time.sleep(5)
    # print qu, di, data, wuyefei, wuye, kaifa, loudong, hushu

def find_unfinished():
    i = 1
    finished_id = []
    urls_unfinished = []
    # 去重
    for key in detail:
        id_1 = key['_id']
        finished_id.append(id_1)

    for url in urls:
        # print url
        xiaoqu_id = re.findall('(\d+)', url)[0]
        # print xiaoqu_id
        if not xiaoqu_id in finished_id:
            urls_unfinished.append(url)
            print url

    for k in urls_unfinished:
        print k
        i += 1
        # getDetail(url)
    print 'unfinished:%s' % i
    return urls_unfinished

# 过滤
def del_doc():

    # 过滤
    for key_1 in detail:
        if len(key_1) == 11:
            tab2.delete_one(key_1)


urls = []
xiaoqus = tab.find()
detail = tab2.find()

for x in xiaoqus:
    url = x['link']
    urls.append(url)

if __name__ == '__main__':

    pool = ThreadPool(10)
    time_1 = time.time()
    try:
        # 查重
        urls_unfinished = find_unfinished()
        # 主程序
        result = pool.map(getDetail, urls_unfinished)
    except:
        #过滤
        del_doc()
        time.sleep(10)

    pool.close()
    pool.join()

    print ('finished: %s') % (time.time() - time_1)