import pandas as pd
from binance.spot import Spot
from datetime import datetime
from binance.um_futures import UMFutures
import time
import smtplib
import logging
import threading
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from tenacity import retry, retry_if_exception_type, wait_fixed, stop_after_attempt, before, before_log
from requests.exceptions import RequestException

# 初始化 Binance API 客户端
client = UMFutures()

# 配置日志记录器
logging.basicConfig(
    filename='/Users/sun/Desktop/Binance',      # 指定日志文件名
    level=logging.INFO,         # 设置日志级别，DEBUG 为最低级别，记录所有级别的日志
    format='%(asctime)s - %(levelname)s - %(message)s'  # 设置日志格式
)
logger = logging.getLogger(__name__)


# 配置重试机制
@retry(
    retry=retry_if_exception_type(RequestException),
    before=before_log(logger, logging.INFO),
    wait=wait_fixed(60),  # 每次重试之间等待60秒
    stop=stop_after_attempt(20)  # 最多重试20次
     # 在每次重试前记录日志
)

def fetch_klines(symbol, interval, limit):
    """从 Binance 获取 K 线数据，并在失败时重试"""
    logger.info(f"Attempting to fetch klines for {symbol} with interval {interval}...")
    return client.klines(symbol, interval, limit=limit)


def get_historical_klines(symbol, interval,j,k):
    """获取历史K线数据"""
    try:
     klines = fetch_klines(symbol, interval, limit=1500)
    except requests.RequestException as e:
     logger.error(f"Failed to fetch klines after multiple attempts: {e}")
    data = []
    for kline in klines:
        data.append([datetime.fromtimestamp(kline[0] / 1000), float(kline[4])])
     # 确保 all_history_data 的长度足够大
    if len(all_history_data) <= j:
        all_history_data.extend([[] for _ in range(j - len(all_history_data) + 1)])

    if len(all_history_data[j]) <= k:
        all_history_data[j].extend([[] for _ in range(k - len(all_history_data[j]) + 1)])

    # 确保 all_history_data[j] 被初始化为列表
    if not all_history_data[j]:
        all_history_data[j] = []
        all_history_data[j]=(pd.DataFrame(data, columns=['Time', 'Close']))

    if not all_history_data[j][k]:
        all_history_data[j][k] = []
        all_history_data[j][k]=(pd.DataFrame(data, columns=['Time', 'Close']))

    return pd.DataFrame(data, columns=['Time', 'Close'])

def get_realtime_klines(symbol, interval):
    """获取实时K线数据"""
    try:
     kline = fetch_klines(symbol=symbol,interval=interval,limit=1)[0]
    except requests.RequestException as e:
     logger.error(f"Failed to fetch klines after multiple attempts: {e}")
    data = []
    data.append([datetime.fromtimestamp(kline[0] / 1000), float(kline[4])])
    return pd.DataFrame(data, columns=['Time', 'Close'])

def periodic_email():
    """每10分钟发送一次提醒邮件"""
    send_email("定时提醒", "这是一封每10分钟发送的提醒邮件")
    # 重新设置定时器，每10分钟调用一次 periodic_email
    threading.Timer(600, periodic_email).start()

def calculate_ema(data, period):
    """计算 EMA """
    data['EMA'] = data['Close'].ewm(span=period, adjust=False).mean()
    return data

def send_email(subject, body):
    """发送邮件"""
    sender_email = "1091282902@qq.com"
    receiver_email = "2492088534@qq.com"
    password = "qovxazwjbqonbada"

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
            logging.info(f"Email sent to {receiver_email}")
            break
        except smtplib.SMTPAuthenticationError:
            print("Authentication failed. Please check your username and password.")
        except smtplib.SMTPConnectError:
            print("Failed to connect to the SMTP server. Check your network and server settings.")
        except Exception as e:
            print(f"Failed to send email: {e}")
            if attempt < retries - 1:
                print(f"Retrying ({attempt + 1}/{retries})...")
                time.sleep(5)  # 等待5秒后重试
            else:
                print("Failed to send email after multiple attempts.")
def check_for_alert(latest_price, latest_ema, threshold,symbol,period,timespan):
    """检查是否需要发送提醒"""
    if abs(abs(latest_price - latest_ema)/latest_ema)*100 < threshold:
        send_email(f"{symbol}Alert", f"\n Now Price {latest_price} \n 接近于 {timespan}  \nEMA {period}  \n{latest_ema}")

# 获取历史数据
symbols=['BTCUSDT','1000PEPEUSDT','1000SATSUSDT','PEOPLEUSDT','SOLUSDT','ORDIUSDT','ETHUSDT','1000RATSUSDT','SAGAUSDT','BIGTIMEUSDT']
Symbols=['BTCUSDT','1000PEPEUSDT']

all_history_data=[];
all_history_data.append([]);
all_history_ema=[];
# 实时更新
def realtime_update():
    """实时更新 EMA 并检查提醒"""
    threshold = 0.5
    timespans =["4h","8h","12h","1d","3d","1w","1M"]
    periods = [12,144,169]
    periodic_email()

    i=0;
    num=0;
    while True:
        j=0;
        if i==0:
            for symbol in Symbols:
                new_kline = client.klines(symbol=symbol, interval='1m', limit=1)[0]
                new_price = float(new_kline[4])  
                k=0;              
                for timespan in timespans:
                    historical_data = get_historical_klines(symbol, timespan,j,k)                   
                    for period in periods:
                        historical_data = calculate_ema(historical_data, period) 
                        historical_data.loc[len(historical_data)] = [datetime.fromtimestamp(new_kline[0] / 1000),new_price,'EMA']
                        historical_data['EMA'] = historical_data['Close'].ewm(span=period, adjust=False).mean()
                        latest_ema = historical_data['EMA'].iloc[-1]
                        logging.info(f"调用历史数据函数，正在计算代币：{symbol},周期：{period},间隔：{timespan}")
                        check_for_alert(new_price, latest_ema, threshold,symbol,period,timespan)
                        #time.sleep(1.5)
                    k=k+1
                j=j+1
            i=1
        else :
            j=0;
            logging.info(f"调用实时循环体")
            for symbol in Symbols:
                new_kline = client.klines(symbol=symbol, interval='1m', limit=1)[0]
                new_price = float(new_kline[4]) 
                logging.info(f"调用实时循环体,进入{symbol}")
                k=0;
                for timespan in timespans:
                    historical_data = get_realtime_klines(symbol,timespan);
                    if (len(all_history_data[j])>0 and len(all_history_data[j][k]) > 0):
                        last_historical_data = all_history_data[j][k].tail(1)
                        logging.info(f"调用实时循环体,进入{symbol},计算{timespan}")
                        comparison_result = last_historical_data['Time'].reset_index(drop=True) == historical_data['Time'].reset_index(drop=True)
                        if (comparison_result == False).any():# 至少有一个值是 False 的处理逻辑
                            all_history_data[j][k]=pd.concat([all_history_data[j][k],historical_data],ignore_index=True)
                        else:
                            all_history_data[j][k].at[all_history_data[j][k].index[-1], 'Close']=historical_data['Close']
                    for period in periods:
                        historical_data = calculate_ema(all_history_data[j][k], period) 
                        historical_data.loc[len(historical_data)] = [datetime.fromtimestamp(new_kline[0] / 1000),new_price,'EMA']
                        historical_data['EMA'] = historical_data['Close'].ewm(span=period, adjust=False).mean()
                        latest_ema = historical_data['EMA'].iloc[-1]
                        logging.info(f"调用实时数据函数，正在计算：{symbol},周期：{period},间隔：{timespan}")
                        check_for_alert(new_price, latest_ema, threshold,symbol,period,timespan)
                        #time.sleep(1.5)
                        #all_history_data[j][k] = all_history_data[j][k].tail(20)
                    k=k+1;
                logger.info(f"{i}次执行实时循环")
                j=j+1;
        time.sleep(300)  # 每隔 300 秒更新一次

# 开始实时更新
realtime_update()
