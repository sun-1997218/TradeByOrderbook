import requests
from pydantic import BaseModel
from typing import List

# 定义输入参数数据类
class RequestParams(BaseModel):
    exchange: str
    symbol: str
    interval: str
    endTime: int
    size: int
    productType: str

class DataItem(BaseModel):
    symbol: str
    exchangeName: str
    step: float
    prices: List[float]  # 假设价格是浮点数，可以根据实际情况调整
    asks: List[float]
    bids: List[float]
    ts: int  # 时间戳

# 定义输出结构数据类
class OrderFlowData(BaseModel):
    # 根据 API 返回的 JSON 定义字段
    success: bool
    code: int
    data: List[DataItem]
    # 根据实际返回数据继续添加字段...

class ApiResponse(BaseModel):
    data: List[OrderFlowData]
    success: bool
    message: str

def fetch_order_flow(params: RequestParams):
    url = "https://open-api.coinank.com/api/orderFlow/lists"
    headers = {
        "accept": "application/json",
        "apikey": "38efe53f82ee40e7956a0e2565aae93f"  # 使用你的 API 密钥
    }

    response = requests.get(url, params=params.dict(), headers=headers)

    if response.status_code == 200:
        response_data = OrderFlowData(**response.json())

        return response_data
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

# 创建请求参数
params = RequestParams(
    exchange="Binance",
    symbol="NEIROUSDT",
    interval="3m",
    endTime=1727157359661,
    size=1, 
    productType="SWAP"
)

# 调用函数并获取结果
result = fetch_order_flow(params)

if result:
    print(result)