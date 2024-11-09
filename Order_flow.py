
import requests
import time
import smtplib
import logging
import sys
import json
import threading
import concurrent.futures
from typing import List
from typing import Optional
from pydantic import BaseModel
from email.mime.text import MIMEText
from binance.um_futures import UMFutures
from datetime import datetime, timedelta
from concurrent.futures import as_completed
from email.mime.multipart import MIMEMultipart
from requests.exceptions import RequestException
from tenacity import retry, retry_if_exception_type, wait_fixed, stop_after_attempt, before, before_log

# 初始化 Binance API 客户端
client = UMFutures()
# 配置日志记录器
logging.basicConfig(
    filename='/Users/sun/Desktop/Order_flow',      # 指定日志文件名
    # 设置日志级别，DEBUG 为最低级别，记录所有级别的日志
    format='%(asctime)s - %(levelname)s - %(message)s' , # 设置日志格式
    datefmt='%Y-%m-%d %H:%M:%S',  # 时间格式
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# 配置重试机制
@retry(
    retry=retry_if_exception_type(RequestException),
    before=before_log(logger, logging.INFO),
    wait=wait_fixed(1),  # 每次重试之间等待60秒
    stop=stop_after_attempt(3)  # 最多重试20次
     # 在每次重试前记录日志
)


    
# 定义输入参数数据类
class RequestParams(BaseModel):
    exchange: str
    symbol: str
    interval: str
    endTime: int
    size: int
    productType: str

  #订单流返回结构
class DataItem(BaseModel):
    symbol: str
    exchangeName: str
    step: Optional[float] = None
    prices: List[float]  # 假设价格是浮点数，可以根据实际情况调整
    asks: List[float]
    bids: List[float]
    ts: int  # 时间戳

class KlineData(BaseModel):
    start_time: int   # 开始时间
    end_time: Optional[int]    # 结束时间
    open_price: float # 开盘价
    close_price: float # 最高价
    high_price: float  # 最低价
    low_price: float # 收盘价
    volume: float     # 成交量
    turnover: float   # 成交额
    trade_count: Optional[int] # 成交笔数
    


# 定义输出结构数据类
class OrderFlowData(BaseModel):
    # 根据 API 返回的 JSON 定义字段
    success: bool
    code: int
    data: Optional[List[DataItem]] = None
    # 根据实际返回数据继续添加字段...

# 定义输出结构数据类
class KlineFlowData(BaseModel):
    # 根据 API 返回的 JSON 定义字段
    success: bool
    code: int
    data: List[KlineData]  # 使用 List[List[Any]] 来定义嵌套列表
    # 根据实际返回数据继续添加字段...
    @classmethod
    def from_api_response(cls, response: dict):
        # 处理响应，将数据转换为 KlineData 实例
        data = [KlineData(
            start_time=item[0],
            end_time=item[1],
            open_price=item[2],
            close_price=item[3],
            high_price=item[4],
            low_price=item[5],
            volume=item[6],
            turnover=item[7],
            trade_count=item[8]
        ) for item in response['data']]
        
        return cls(success=response['success'], code=response['code'], data=data)


# 获取当前时间并向下舍入到最近的 3 分钟整点
def adjust_timestamp_to_last_interval():
    now = datetime.now()
    # 计算时间差的秒数
    minutes = (now.minute // 3) * 3
    adjusted_time = now.replace(minute=minutes, second=59, microsecond=0)
    return adjusted_time

def just_passed_the_hour(threshold):
    # 获取当前时间
    now = datetime.now()
    # 判断是否刚过整点（分钟为0，并且秒数在0到threshold之间）
    if now.minute == 0  and 0 <= now.second <= threshold:
        return True
    else:
        return False
    
# 确保请求的是有效时间段的数据
def fetch_order_flow_with_adjusted_timestamp(symbol):
    # 调整时间戳
    adjusted_time = adjust_timestamp_to_last_interval()
    
    # 等待数据更新：如果当前时间太接近下一个时间段，延迟请求
    now = datetime.now()
    if now < adjusted_time + timedelta(minutes=3):
        # 请求的是之前 3 分钟的数据，等待数据更新
        time=adjusted_time-timedelta(seconds=1)
        return True,time
    else:
        # 数据还没更新，稍后再试
        logger.info("数据还未更新，稍后再请求。")
        return None

# 假设这个函数是你调用API获取订单流数据的地方
def fetch_order_flow(params: RequestParams):
    url = "https://open-api.coinank.com/api/orderFlow/lists"
    headers = {
        "accept": "application/json",
        "apikey": "38efe53f82ee40e7956a0e2565aae93f"  # 使用你的 API 密钥
    }
    response = requests.get(url, params=params.dict(), headers=headers)
    if response.status_code == 200 :
        response_json = response.json()
        if  response_json.get("success", True):
            response_data = OrderFlowData(**response.json())
            return response_data
    else:
        logger.info(f"Error: {response.status_code}, {response.text}")
        return None

# 假设这个函数是你调用历史K线获取数据的地方
def fetch_kline(params: RequestParams):
    url = "https://open-api.coinank.com/api/kline/lists"

    headers = {
        "accept": "application/json",
        "apikey": "38efe53f82ee40e7956a0e2565aae93f"  # 使用你的 API 密钥
    }

    response = requests.get(url, params=params.dict(), headers=headers)

    if response.status_code == 200 :
        response_json = response.json()
        if  response_json.get("success", True):
            response_data = KlineFlowData.from_api_response(response_json)
            return response_data
    else:
        logger.info(f"Error: {response.status_code}, {response.text}")
        return None

# 调用获取并调整时间戳的方法



def send_email(subject, body):
    """发送邮件"""
    sender_email = "1091282902@qq.com"
    receiver_email = "2199483735@qq.com"
    password = "noelhxuvgwlphcjc"

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    retries = 3  # 最多重试3次
    for attempt in range(retries):
        try:
            server = smtplib.SMTP_SSL('smtp.qq.com', 465)
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            server.quit()
            logger.info(f"Email sent to {receiver_email}")
            break
        except smtplib.SMTPAuthenticationError:
            logger.info("Authentication failed. Please check your username and password.")
        except smtplib.SMTPConnectError:
            logger.info("Failed to connect to the SMTP server. Check your network and server settings.")
        except Exception as e:
            logger.info(f"Failed to send email: {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying ({attempt + 1}/{retries})...")
                time.sleep(1)  # 等待5秒后重试
            else:
                logger.info("Failed to send email after multiple attempts.")

def send_email_non_blocking_1h(data, symbol,open,close):
    threading.Thread(target=if_send_email_1h, args=(data, symbol,open,close)).start()

def send_email_non_blocking_4h(data, symbol,open,close):
    threading.Thread(target=if_send_email_4h, args=(data, symbol,open,close)).start()


def if_send_email_1h(symbol_dict,symbol,open,close): 
        minvalue= min(abs(symbol_dict["max_price"]-symbol_dict["POC_price"]),abs(symbol_dict["min_price"]-symbol_dict["POC_price"]))
        if minvalue == 0:
            return
        Min=min(open,close);
        Max=max(open,close)
        Isred = False;
        if(open>=close):
            Isred=True

        logger.info(f"open:{open},POC:{symbol_dict['POC_price']},close:{close},symbol{symbol}")
        if(symbol_dict["POC_price"]<=Min or symbol_dict["POC_price"]>=Max):
            rate = (int)(minvalue/(symbol_dict["max_price"]-symbol_dict['min_price'])*100)
            if Isred==True: send_email(f"1小时鲜红K线呀呀呀 {symbol} Order_flow Alert", f"\n 侦测到 POC {symbol_dict['POC_price']} \n 跳出两端  \n rate: {rate} ")
            else :send_email(f"1小时绿绿绿哦哦哦哦 {symbol} Order_flow Alert", f"\n 侦测到 POC {symbol_dict['POC_price']} \n 跳出两端  \n rate: {rate} ")

def if_send_email_4h(symbol_dict,symbol,open,close): 
        minvalue= min(abs(symbol_dict["max_price"]-symbol_dict["POC_price"]),abs(symbol_dict["min_price"]-symbol_dict["POC_price"]))
        if minvalue == 0:
            return
        Min=min(open,close);
        Max=max(open,close)
        Isred = False;
        if(open>=close):
            Isred=True

        logger.info(f"open:{open},POC:{symbol_dict['POC_price']},close:{close},symbol{symbol}")
        if(symbol_dict["POC_price"]<=Min or symbol_dict["POC_price"]>=Max):
            rate = (int)(minvalue/(symbol_dict["max_price"]-symbol_dict['min_price'])*100)
            if Isred==True: send_email(f"4小时鲜红K线呀呀呀 {symbol} Order_flow Alert", f"\n 侦测到 POC {symbol_dict['POC_price']} \n 跳出两端  \n rate: {rate} ")
            else :send_email(f"4小时绿绿绿哦哦哦哦 {symbol} Order_flow Alert", f"\n 侦测到 POC {symbol_dict['POC_price']} \n 跳出两端  \n rate: {rate} ")

def process_symbol_1h(symbol, all_coins, all_coins_minmaxvalue):

    params_1h = RequestParams(
    exchange="Binance",
    symbol="Begin",
    interval="1h",
    endTime= 1727157359661,
    size=1, 
    productType="SWAP"
    )
    
    if symbol not in all_coins:
        all_coins[symbol] = {}

    if symbol not in all_coins_minmaxvalue:
        all_coins_minmaxvalue[symbol] = {
            "max_price": -sys.maxsize - 1,
            "min_price": sys.maxsize,
            "POC_value": -sys.maxsize - 1,
            "POC_price": -sys.maxsize - 1
        }
    else:
        all_coins_minmaxvalue[symbol]["max_price"] = -sys.maxsize - 1
        all_coins_minmaxvalue[symbol]["min_price"] = sys.maxsize
        all_coins_minmaxvalue[symbol]["POC_value"] = -sys.maxsize - 1
        all_coins_minmaxvalue[symbol]["POC_price"] = -sys.maxsize - 1


    params_1h.symbol = symbol
    Endtime = datetime.now()
    params_1h.endTime = int(Endtime.timestamp() * 1000)
    if(symbol=='GRASSUSDT' or symbol=='DRIFTUSDT'):
        params_1h.exchange='Bybit'
    
    params_1h.size=2
    logger.error(f"订单流请求参数params:{params_1h}")
    Orderflow_response_data = fetch_order_flow(params_1h)

    unix_time = int((datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)).timestamp())
    # 使用 json.dumps() 打印 JSON 格式 
    
    if( len(Orderflow_response_data.data)>1):
        if(Orderflow_response_data.data[1].ts==unix_time*1000):
            Orderflow_response_data.data[0] = Orderflow_response_data.data[1]


    Endtime = datetime.now()
    params_1h.endTime = int(Endtime.timestamp() * 1000)
    params_1h.size=2
    Kline_response_data=fetch_kline(params_1h)

    

    if(Kline_response_data.data[1].start_time==unix_time*1000):
        Kline_response_data.data[0] = Kline_response_data.data[1]


    # 将 Kline_response_data 转换为字典
    # Klinedata_dict = Kline_response_data.model_dump()
    # logger.error(f"{symbol},{json.dumps(Klinedata_dict, indent=4, ensure_ascii=False)}")
    # 转换为上一个整点的 Unix 时间戳


    if(Orderflow_response_data is not None):
        for index, value in enumerate(Orderflow_response_data.data[0].prices):
            if value in all_coins[symbol]:
                all_coins[symbol][value] += Orderflow_response_data.data[0].asks[index] + Orderflow_response_data.data[0].bids[index]
            else:
                all_coins[symbol][value] = Orderflow_response_data.data[0].asks[index] + Orderflow_response_data.data[0].bids[index]
            
            # 调整最大值
            if value >= all_coins_minmaxvalue[symbol]["max_price"]:
                all_coins_minmaxvalue[symbol]["max_price"] = value
            
            # 调整最小值
            if value <= all_coins_minmaxvalue[symbol]["min_price"]:
                all_coins_minmaxvalue[symbol]["min_price"] = value
            
            # 调整当前时间级别的POC
            if all_coins[symbol][value] > all_coins_minmaxvalue[symbol]["POC_value"]:
                all_coins_minmaxvalue[symbol]["POC_value"] = all_coins[symbol][value]
                all_coins_minmaxvalue[symbol]["POC_price"] = value
        
            # 非阻塞地发送邮件
        send_email_non_blocking_1h(all_coins_minmaxvalue[symbol], symbol,Kline_response_data.data[0].open_price,Kline_response_data.data[0].close_price)
        all_coins[symbol] = {}
        #formatted_time = datetime.fromtimestamp(Orderflow_response_data.data[0].ts / 1000).strftime('%H:%M:%S')
        #logger.info(f"返回时间戳: {formatted_time},{symbol}的最大值为{all_coins_minmaxvalue[symbol]['max_price']},最小值为{all_coins_minmaxvalue[symbol]['min_price']},POC值为{all_coins_minmaxvalue[symbol]['POC_value']},POC价格为{all_coins_minmaxvalue[symbol]['POC_price']}")

def process_symbol_4h(symbol, all_coins, all_coins_minmaxvalue):

    params_1h = RequestParams(
    exchange="Binance",
    symbol="Begin",
    interval="4h",
    endTime= 1727157359661,
    size=1, 
    productType="SWAP"
    )
    
    if symbol not in all_coins:
        all_coins[symbol] = {}

    if symbol not in all_coins_minmaxvalue:
        all_coins_minmaxvalue[symbol] = {
            "max_price": -sys.maxsize - 1,
            "min_price": sys.maxsize,
            "POC_value": -sys.maxsize - 1,
            "POC_price": -sys.maxsize - 1
        }
    else:
        all_coins_minmaxvalue[symbol]["max_price"] = -sys.maxsize - 1
        all_coins_minmaxvalue[symbol]["min_price"] = sys.maxsize
        all_coins_minmaxvalue[symbol]["POC_value"] = -sys.maxsize - 1
        all_coins_minmaxvalue[symbol]["POC_price"] = -sys.maxsize - 1


    params_1h.symbol = symbol
    Endtime = datetime.now()
    params_1h.endTime = int(Endtime.timestamp() * 1000)
    if(symbol=='GRASSUSDT' or symbol=='DRIFTUSDT'):
        params_1h.exchange='Bybit'
    
    params_1h.size=2
    logger.error(f"订单流请求参数params:{params_1h}")
    Orderflow_response_data = fetch_order_flow(params_1h)

    unix_time = int((datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)).timestamp())
    # 使用 json.dumps() 打印 JSON 格式 
    
    if( len(Orderflow_response_data.data)>1):
        if(Orderflow_response_data.data[1].ts==unix_time*1000):
            Orderflow_response_data.data[0] = Orderflow_response_data.data[1]


    Endtime = datetime.now()
    params_1h.endTime = int(Endtime.timestamp() * 1000)
    params_1h.size=2
    Kline_response_data=fetch_kline(params_1h)

    

    if(Kline_response_data.data[1].start_time==unix_time*1000):
        Kline_response_data.data[0] = Kline_response_data.data[1]


    # 将 Kline_response_data 转换为字典
    # Klinedata_dict = Kline_response_data.model_dump()
    # logger.error(f"{symbol},{json.dumps(Klinedata_dict, indent=4, ensure_ascii=False)}")
    # 转换为上一个整点的 Unix 时间戳


    if(Orderflow_response_data is not None):
        for index, value in enumerate(Orderflow_response_data.data[0].prices):
            if value in all_coins[symbol]:
                all_coins[symbol][value] += Orderflow_response_data.data[0].asks[index] + Orderflow_response_data.data[0].bids[index]
            else:
                all_coins[symbol][value] = Orderflow_response_data.data[0].asks[index] + Orderflow_response_data.data[0].bids[index]
            
            # 调整最大值
            if value >= all_coins_minmaxvalue[symbol]["max_price"]:
                all_coins_minmaxvalue[symbol]["max_price"] = value
            
            # 调整最小值
            if value <= all_coins_minmaxvalue[symbol]["min_price"]:
                all_coins_minmaxvalue[symbol]["min_price"] = value
            
            # 调整当前时间级别的POC
            if all_coins[symbol][value] > all_coins_minmaxvalue[symbol]["POC_value"]:
                all_coins_minmaxvalue[symbol]["POC_value"] = all_coins[symbol][value]
                all_coins_minmaxvalue[symbol]["POC_price"] = value
        
            # 非阻塞地发送邮件
        send_email_non_blocking_4h(all_coins_minmaxvalue[symbol], symbol,Kline_response_data.data[0].open_price,Kline_response_data.data[0].close_price)
        all_coins[symbol] = {}
        #formatted_time = datetime.fromtimestamp(Orderflow_response_data.data[0].ts / 1000).strftime('%H:%M:%S')
        #logger.info(f"返回时间戳: {formatted_time},{symbol}的最大值为{all_coins_minmaxvalue[symbol]['max_price']},最小值为{all_coins_minmaxvalue[symbol]['min_price']},POC值为{all_coins_minmaxvalue[symbol]['POC_value']},POC价格为{all_coins_minmaxvalue[symbol]['POC_price']}")

def real_data_update():
    Symbols = ['NEIROETHUSDT', 'NEIROUSDT', 'GOATUSDT', 'MOODENGUSDT', 'MEWUSDT', 'GRASSUSDT', 'TROYUSDT','DRIFTUSDT']
    all_coins_1h = {}
    all_coins_minmaxvalue_1h = {}

    all_coins_4h = {}
    all_coins_minmaxvalue_4h = {}

    while True:
        # 每次循环都记录当前时间
        now = datetime.now()
        logger.info(f"Main loop running at     : {now}")
        
        # 确保在整点的前 59 秒内执行 process_symbol
        if now.minute == 0 and 0 <= now.second < 59:
            logger.info("Condition met for processing symbols")
            for symbol in Symbols:
                try:
                    process_symbol_1h(symbol=symbol, all_coins=all_coins_1h, all_coins_minmaxvalue=all_coins_minmaxvalue_1h)
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")

            time.sleep(40)

            if(now.hour%4==0):
                for symbol in Symbols:
                    try:
                        process_symbol_4h(symbol=symbol, all_coins=all_coins_4h, all_coins_minmaxvalue=all_coins_minmaxvalue_4h)
                    except Exception as e:
                        logger.error(f"Error processing {symbol}: {e}")
            # 避免在同一分钟内重复触发，将进入等待下一分钟
            while datetime.now().minute == 0 and datetime.now().second < 59:
                time.sleep(20)
                logger.info(f"执行过一次，现在等待跳过一分钟整: {datetime.now()}")
        
        # 检查每隔 29 秒是否进入下一个循环
        time.sleep(29)
        logger.info(f"Loop iteration complete at: {datetime.now()}")

real_data_update()